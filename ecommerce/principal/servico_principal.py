import os
import sys
import threading

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import shared.auxi as auxi
import shared.rabbitmq as rmq

FILA_PRINCIPAL = "fila_principal"
FILA_PROMOCOES = "fila_promocoes"

CHAVE_PRIVADA = os.path.join(BASE_DIR, "chaves", "privada.pem")

CHAVE_PUBLICA_ESTOQUE = os.path.join(BASE_DIR, "chaves", "publicas", "estoque.pem")
CHAVE_PUBLICA_PAGAMENTO = os.path.join(BASE_DIR, "chaves", "publicas", "pagamento.pem")
CHAVE_PUBLICA_ENTREGA = os.path.join(BASE_DIR, "chaves", "publicas", "entrega.pem")
CHAVE_PUBLICA_PROMOCAO = os.path.join(BASE_DIR, "chaves", "publicas", "promocao.pem")

pedidos = {}
produtos = []
promocoes = []
proximo_pedido_id = 1
lock = threading.Lock()
evento_produtos = threading.Event()
evento_consumidor_pronto = threading.Event()

def solicitar_produtos(canal, chave_privada, exibir=True):
    evento = auxi.criar_evento("produto.consulta", {})

    evento_produtos.clear()
    rmq.publicar_evento(canal, rmq.EXCHANGE_ECOMMERCE, evento, chave_privada)

    if evento_produtos.wait(timeout=3.0):
        if exibir:
            mostrar_produtos()
    else:
        print("\nErro: O serviço de Estoque não respondeu a tempo.")

def mostrar_produtos():
    with lock:
        if not produtos:
            print("\nNenhum produto carregado. Escolha a opção 1 para consultar o Estoque.")
            return
        
        print("\n ======== PRODUTOS ========")

        for produto in produtos:
            print(f"ID: {produto['produto_id']} | "f"{produto['nome']} | "f"R$ {produto['valor']:.2f} | "f"Disponível: {produto['quantidade']}")
            
            promocao = next((
                p for p in promocoes
                    if p["produto_id"] == produto["produto_id"]
            ), None)

            if promocao:
                print(f"Promoção: R$ {promocao['valor_promocional']:.2f} | Categoria: {promocao['categoria']}")

        print("=============================")

def criar_pedido(canal, chave_privada):
    global proximo_pedido_id
    mostrar_produtos()

    if not produtos:
        return
    try:
        produto_id = int(input("\nDigite o ID do produto: "))
        quantidade = int(input("Digite a quantidade: "))
    except ValueError:
        print("\nDigite valores numéricos válidos.")
        return

    if quantidade <= 0:
        print("\nQuantidade inválida.")
        return

    produto_encontrado = None

    with lock:
        for produto in produtos:
            if produto["produto_id"] == produto_id:
                produto_encontrado = produto
                break

    if produto_encontrado is None:
        print("\nProduto inexistente.")
        return

    with lock:
        promocao = next(
            (
                p for p in promocoes
                    if p["produto_id"] == produto_id
            ), None)

    if promocao:
        valor_unitario = promocao["valor_promocional"]
    else:
        valor_unitario = produto_encontrado["valor"]

    pedido_id = proximo_pedido_id
    proximo_pedido_id += 1

    pedido = {
        "pedido_id": pedido_id,
        "produtos": [
            {"produto_id": produto_id, "quantidade": quantidade, "valor_unitario": valor_unitario}
        ],
        "status": "criado"
    }

    with lock:
        pedidos[pedido_id] = pedido

    evento = auxi.criar_evento("pedido.criado", pedido)

    rmq.publicar_evento(canal, rmq.EXCHANGE_ECOMMERCE, evento, chave_privada)

    print(f"\nPedido {pedido_id} criado.")
    print(f"Status: {pedido['status']}")

def consultar_pedido():
    try:
        pedido_id = int(input("\nDigite o ID do pedido: "))
    except ValueError:
        print("\nID inválido.")
        return

    with lock:
        pedido = pedidos.get(pedido_id)

    if pedido is None:
        print("\nPedido não encontrado.")
        return

    print("\n ======== PEDIDO ========")
    print(f"ID: {pedido['pedido_id']} | " f"Status: {pedido['status']}")
    print("Produtos:")

    for item in pedido["produtos"]:
        nome_produto = "Desconhecido"
        with lock:
            for p in produtos:
                if p["produto_id"] == item["produto_id"]:
                    nome_produto = p["nome"]
                    break
                    
        print(f"Produto ID: {item['produto_id']} | Nome: {nome_produto} | Quantidade: {item['quantidade']} | Valor: R$ {item['valor_unitario']:.2f}")

    print("\n =======================")

def consultar_status():
    try:
        pedido_id = int(input("\nDigite o ID do pedido: "))
    except ValueError:
        print("\nID inválido.")
        return

    with lock:
        pedido = pedidos.get(pedido_id)

    if pedido is None:
        print("\nPedido não encontrado.")
        return

    print("\n ======== PEDIDO ========")
    print(f"ID: {pedido['pedido_id']} | " f"Status: {pedido['status']}")

def excluir_pedido(canal, chave_privada):
    try:
        pedido_id = int(input("\nDigite o ID do pedido: "))
    except ValueError:
        print("\nID inválido.")
        return

    with lock:
       pedido = pedidos.get(pedido_id)
       
    if pedido is None:
        print("\nPedido não encontrado.")
        return        

    if pedido["status"] in ["excluido", "enviado"]:
        print("\nEsse pedido não pode ser excluído.")
        return

    evento = auxi.criar_evento("pedido.excluido", {"pedido_id": pedido_id, "produtos": pedido["produtos"]})
    rmq.publicar_evento(canal, rmq.EXCHANGE_ECOMMERCE, evento, chave_privada)

    with lock:
        pedidos[pedido_id]["status"] = "excluido"

def processar_evento(canal, evento, chave_privada):
    global produtos

    tipo = evento["tipo"]
    dados = evento["dados"]

    match tipo:

        case "produto.lista":
            with lock:
                produtos = dados["produtos"]

            evento_produtos.set()

        case "promocao.criada":
            with lock:
                promocoes.append(dados)

        case "pedido.estoque_ok":
            pedido_id = dados["pedido_id"]

            with lock:
                if pedido_id not in pedidos:
                    print(f"\nPedido {pedido_id} não encontrado.")
                    return

                pedidos[pedido_id]["status"] = "estoque_ok"

        case "estoque.indisponivel":
            pedido_id = dados["pedido_id"]

            with lock:
                if pedido_id not in pedidos:
                    print(f"\nPedido {pedido_id} não encontrado.")
                    return

                pedido = pedidos[pedido_id]

            print("\nEstoque indisponível. Pedido será excluído.")

            evento_exclusao = auxi.criar_evento(
                "pedido.excluido",
                {
                    "pedido_id": pedido_id,
                    "produtos": pedido["produtos"]
                }
            )

            rmq.publicar_evento(
                canal,
                rmq.EXCHANGE_ECOMMERCE,
                evento_exclusao,
                chave_privada
            )

            with lock:
                pedidos[pedido_id]["status"] = "excluido"

        case "pagamento.aprovado":
            pedido_id = dados["pedido_id"]

            with lock:
                if pedido_id not in pedidos:
                    print(f"\nPedido {pedido_id} não encontrado.")
                    return

                pedidos[pedido_id]["status"] = "pagamento_aprovado"

        case "pagamento.recusado":
            pedido_id = dados["pedido_id"]

            with lock:
                if pedido_id not in pedidos:
                    print(f"\nPedido {pedido_id} não encontrado.")
                    return

                pedido = pedidos[pedido_id]

            print("\nPagamento recusado. Pedido excluído.")

            evento_exclusao = auxi.criar_evento(
                "pedido.excluido",
                {
                    "pedido_id": pedido_id,
                    "produtos": pedido["produtos"]
                }
            )

            rmq.publicar_evento(
                canal,
                rmq.EXCHANGE_ECOMMERCE,
                evento_exclusao,
                chave_privada
            )

            with lock:
                pedidos[pedido_id]["status"] = "excluido"

        case "pedido.enviado":
            pedido_id = dados["pedido_id"]

            with lock:
                if pedido_id not in pedidos:
                    print(f"\nPedido {pedido_id} não encontrado.")
                    return

                pedidos[pedido_id]["status"] = "enviado"

        case _:
            print(f"\nEvento não esperado pelo Principal: {tipo}")

def receber_evento(canal, metodo, propriedades, corpo, chave_privada, chaves_publicas):
    evento = auxi.json_para_evento(corpo)

    tipo = evento["tipo"]

    #Descobre qual chave publica usar
    match tipo:
        case "produto.lista":
            chave_publica = chaves_publicas["estoque"]

        case "pedido.estoque_ok":
            chave_publica = chaves_publicas["estoque"]

        case "estoque.indisponivel":
            chave_publica = chaves_publicas["estoque"]

        case "pagamento.aprovado":
            chave_publica = chaves_publicas["pagamento"]

        case "pagamento.recusado":
            chave_publica = chaves_publicas["pagamento"]

        case "pedido.enviado":
            chave_publica = chaves_publicas["entrega"]

        case "promocao.criada":
            chave_publica = chaves_publicas["promocao"]

        case _:
            print("\nEvento não esperado pelo Principal.")
            canal.basic_ack(delivery_tag=metodo.delivery_tag)
            return

    assinatura_valida = auxi.verificar_assinatura(evento, chave_publica)

    if not assinatura_valida:
        canal.basic_ack(delivery_tag=metodo.delivery_tag)

        return

    processar_evento(canal, evento, chave_privada)

    canal.basic_ack(delivery_tag=metodo.delivery_tag)

def iniciar_consumidor(chave_privada, chaves_publicas):
    conexao, canal = rmq.criar_canal()

    #Filas do ECOMMERCE
    canal.queue_declare(queue=FILA_PRINCIPAL, durable=True)

    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="produto.lista")
    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pedido.estoque_ok")
    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="estoque.indisponivel")

    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pagamento.aprovado")
    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pagamento.recusado")

    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pedido.enviado")

    #Filas de PROMOÇÃO
    canal.queue_declare(queue=FILA_PROMOCOES, durable=True)
    canal.queue_bind(exchange=rmq.EXCHANGE_PROMOCAO, queue=FILA_PROMOCOES, routing_key="promocao.categoria.*")

    canal.basic_qos(prefetch_count=1)

    canal.basic_consume(queue=FILA_PRINCIPAL, on_message_callback=lambda ch, method, properties, body:
                        receber_evento(ch, method, properties, body, chave_privada, chaves_publicas),
                        auto_ack=False)
    
    canal.basic_consume(queue=FILA_PROMOCOES, on_message_callback=lambda ch, method, properties, body:
                        receber_evento(ch, method, properties, body, chave_privada, chaves_publicas),
                        auto_ack=False)

    evento_consumidor_pronto.set()

    canal.start_consuming()

def menu():
    print("\n=============================================")
    print("          E-COMMERCE MONSTER ENERGY            ")
    print("\n=============================================")
    print("1 - Ver produto")
    print("2 - Criar pedido")
    print("3 - Consultar pedido")
    print("4 - Consultar status")
    print("5 - Excluir pedido")
    print("0 - Sair")
    print("\n=============================================")

def interface(canal, chave_privada):
    while True:
        menu()
        opcao = input("Escolha uma opção: ")

        match opcao:
            case "1": 
                solicitar_produtos( canal, chave_privada )
            case "2":
                criar_pedido( canal, chave_privada ) 
            case "3": 
                consultar_pedido() 
            case "4": 
                consultar_status() 
            case "5": 
                excluir_pedido( canal, chave_privada ) 
            case "0": 
                print( "\nEncerrando Principal..." ) 
                break
            case _: 
                print("\nOpção inválida.")

def main():
    conexao, canal = rmq.criar_canal()
    print("Conexão com RabbitMQ estabelecida.")

    chave_privada = auxi.carregar_chave_privada(CHAVE_PRIVADA)

    chave_publica_estoque = auxi.carregar_chave_publica(CHAVE_PUBLICA_ESTOQUE)
    chave_publica_pagamento = auxi.carregar_chave_publica(CHAVE_PUBLICA_PAGAMENTO)
    chave_publica_entrega = auxi.carregar_chave_publica(CHAVE_PUBLICA_ENTREGA)
    chave_publica_promocao = auxi.carregar_chave_publica(CHAVE_PUBLICA_PROMOCAO)

    chaves_publicas = {
        "estoque": chave_publica_estoque,
        "pagamento": chave_publica_pagamento,
        "entrega": chave_publica_entrega,
        "promocao": chave_publica_promocao
    }

    thread_consumidor = threading.Thread(target= iniciar_consumidor, args=(chave_privada, chaves_publicas), daemon=True)
    thread_consumidor.start()

    evento_consumidor_pronto.wait()

    solicitar_produtos(canal, chave_privada, False)

    interface(canal, chave_privada)

    conexao.close()

if __name__ == "__main__":
    main()