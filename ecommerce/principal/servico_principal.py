import os
import sys
import threading
import random

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import shared.auxi as auxi
from shared.rabbitmq import criar_canal, EXCHANGE_ECOMMERCE

FILA_PRINCIPAL = "fila_principal"

CHAVE_PRIVADA = os.path.join(BASE_DIR, "chaves", "privada.pem")

CHAVE_PUBLICA_ESTOQUE = os.path.join(BASE_DIR, "chaves", "publicas", "estoque.pem")
CHAVE_PUBLICA_PAGAMENTO = os.path.join(BASE_DIR, "chaves", "publicas", "pagamento.pem")
CHAVE_PUBLICA_ENTREGA = os.path.join(BASE_DIR, "chaves", "publicas", "entrega.pem")

pedidos = {}
produtos = []
proximo_pedido_id = 1
lock = threading.Lock()

def publicar_evento(canal, evento, chave_privada):
    auxi.assinar_evento(evento, chave_privada)

    canal.basic_publish(
        exchange=EXCHANGE_ECOMMERCE,
        routing_key=evento["tipo"],
        body=auxi.evento_para_json(evento),
    )

    print(f"Evento publicado: {evento['tipo']}")

def solicitar_produtos(canal, chave_privada):
    evento = auxi.criar_evento("produto.consulta", {})

    publicar_evento(canal, evento, chave_privada)

    print("Aguardando lista de produtos...")

def mostrar_produtos():
    with lock:
        if not produtos:
            print("\nNenhum produto carregado. Escolha a opção 1 para consultar o Estoque.")
            return
        
        print("\n ======== PRODUTOS ========")

        for produto in produtos:
            print(f"ID: {produto['produto_id']} | "f"{produto['nome']} | "f"R$ {produto['valor']:.2f} | "f"Disponível: {produto['quantidade']}")

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
        print("Digite valores numéricos válidos.")
        return

    if quantidade <= 0:
        print("Quantidade inválida.")
        return

    produto_encontrado = None

    with lock:
        for produto in produtos:
            if produto["produto_id"] == produto_id:
                produto_encontrado = produto
                break

    if produto_encontrado is None:
        print("Produto inexistente.")
        return

    pedido_id = proximo_pedido_id
    proximo_pedido_id += 1

    pedido = {
        "pedido_id": pedido_id,
        "produtos": [
            {"produto_id": produto_id, "quantidade": quantidade}
        ],
        "status": "criado"
    }

    with lock:
        pedidos[pedido_id] = pedido

    evento = auxi.criar_evento("pedido.criado", pedido)

    publicar_evento(canal, evento, chave_privada)

    print(f"\nPedido {pedido_id} criado.")
    print(f"Status: {pedido['status']}")

def consultar_pedido():
    try:
        pedido_id = int(input("\nDigite o ID do pedido: "))
    except ValueError:
        print("ID inválido.")
        return

    with lock:
        pedido = pedidos.get(pedido_id)

    if pedido is None:
        print("Pedido não encontrado.")
        return

    print("\n ======== PEDIDO ========")
    print(f"ID: {pedido['pedido_id']} |" f"Status: {pedido['status']}")
    print("Produtos:")
    for produto in pedido["produtos"]:
        print(f"Poduto ID: {produto['produto_id']} | " f"Quantidade: {produto['quantidade']}")
    print("\n =======================")

def consultar_status():
    try:
        pedido_id = int(input("\nDigite o ID do pedido: "))
    except ValueError:
        print("ID inválido.")
        return

    with lock:
        pedido = pedidos.get(pedido_id)

    if pedido is None:
        print("Pedido não encontrado.")
        return

    print("\n ======== PEDIDO ========")
    print(f"ID: {pedido['pedido_id']} |" f"Status: {pedido['status']}")

def excluir_pedido(canal, chave_privada):
    try:
        pedido_id = int(input("\nDigite o ID do pedido: "))
    except ValueError:
        print("ID inválido.")
        return

    with lock:
       pedido = pedidos.get(pedido_id)
       
    if pedido is None:
        print("Pedido não encontrado.")
        return        

    if pedido["status"] in ["excluido", "enviado"]:
        print("Esse pedido não pode ser excluído.")
        return

    evento = auxi.criar_evento("pedido.excluido", {"pedido_id": pedido_id, "produtos": pedido["produtos"]})
    publicar_evento(canal, evento, chave_privada)

    with lock:
        pedidos[pedido_id]["status"] = "excluido"
        print(f"Pedido {pedido_id} excluído.")

def processar_evento(canal, evento, chave_privada):
    global produtos
    tipo = evento["tipo"]
    dados = evento["dados"]

    if tipo == "produto.lista":
        with lock:
            produtos = dados["produtos"]
        return

    pedido_id = dados["pedido_id"]

    if pedido_id not in pedidos:
        print(f"Pedido {pedido_id} não encontrado.")
        return

    pedido = pedidos[pedido_id]

    with lock:
        if pedido_id not in pedidos:
            print(f"Pedido {pedido_id} não encontrado.")
            return

        pedido = pedidos[pedido_id]

    if tipo == "pedido.estoque_ok":
        with lock:
            pedidos[pedido_id]["status"] = ("estoque_ok")

        print("Estoque reservado com sucesso.")
        print("Status atualizado para: estoque_ok")

    elif tipo == "estoque.indisponivel":
        print("Estoque indisponível. Pedido será excluído.")

        evento_exclusao = auxi.criar_evento("pedido.excluido", {"pedido_id": pedido_id, "produtos": pedido["produtos"]})
        publicar_evento(canal, evento_exclusao, chave_privada)

        with lock:
            pedidos["status"] = "excluido"

    elif tipo == "pagamento.aprovado":
        with lock:
            pedidos[pedido_id]["status"] = ("pagamento_aprovado")

        print(f"Pagamento aprovado. Pedido: {pedido_id}")

    elif tipo == "pagamento.recusado":
        print("Pagamento recusado. Pedido será excluído.")
        
        evento_exclusao = auxi.criar_evento("pedido.excluido", {"pedido_id": pedido_id, "produtos": pedido["produtos"]})
        publicar_evento(canal, evento_exclusao, chave_privada)

        with lock:
            pedidos["status"] = "excluido"
        
    elif tipo == "pedido.enviado":
        with lock:
            pedidos["status"] = "enviado"

        print(f"Pedido {pedido_id} enviado!")

def receber_evento(canal, metodo, propriedades, corpo, chave_privada, chaves_publicas):
    evento = auxi.json_para_evento(corpo)

    tipo = evento["tipo"]

    #Descobre qual chave publica usar
    if tipo == "produto.lista":
        chave_publica = chaves_publicas["estoque"]

    if tipo == ["pedido.estoque_ok", "estoque.indisponivel"]:
        chave_publica = chaves_publicas["estoque"]

    elif tipo == ["pagamento.aprovado", "pagamento.recusado"]:
        chave_publica = chaves_publicas["pagamento"]

    elif tipo == "pedido.enviado":
        chave_publica = chaves_publicas["entrega"]

    else:
        print("Evento não esperado pelo Principal.")
        canal.basic_ack(delivery_tag=metodo.delivery_tag)
        return

    assinatura_valida = auxi.verificar_assinatura(evento, chave_publica)

    if not assinatura_valida:
        print("ERRO: assinatura inválida. Evento descartado.")

        canal.basic_ack(delivery_tag=metodo.delivery_tag)

        return

    print("Assinatura válida")

    processar_evento(canal, evento, chave_privada)

    canal.basic_ack(delivery_tag=metodo.delivery_tag)

def iniciar_consumidor(chave_privada, chaves_publicas):
    conexao, canal = criar_canal()

    canal.queue_declare(queue=FILA_PRINCIPAL, durable=True)

    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="produto.lista")
    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pedido.estoque_ok")
    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="estoque.indisponivel")

    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pagamento.aprovado")
    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pagamento.recusado")

    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pedido.enviado")

    canal.basic_qos(prefetch_count=1)

    canal.basic_consume(queue=FILA_PRINCIPAL, on_message_callback=lambda ch, method, properties, body:
                        receber_evento(ch, method, properties, body, chave_privada, chaves_publicas),
                        auto_ack=False)

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

        if opcao == "1": 
            solicitar_produtos( canal, chave_privada ) 
        elif opcao == "2":
            criar_pedido( canal, chave_privada ) 
        elif opcao == "3": 
            consultar_pedido() 
        elif opcao == "4": 
            consultar_status() 
        elif opcao == "5": 
            excluir_pedido( canal, chave_privada ) 
        elif opcao == "0": 
            print( "\nEncerrando Principal..." ) 
            break
        else:
            print("Opção inválida.")

def main():
    conexao, canal = criar_canal()
    print("Conexão com RabbitMQ estabelecida.")

    chave_privada = auxi.carregar_chave_privada(CHAVE_PRIVADA)

    chave_publica_estoque = auxi.carregar_chave_publica(CHAVE_PUBLICA_ESTOQUE)
    chave_publica_pagamento = auxi.carregar_chave_publica(CHAVE_PUBLICA_PAGAMENTO)
    chave_publica_entrega = auxi.carregar_chave_publica(CHAVE_PUBLICA_ENTREGA)

    chaves_publicas = {
        "estoque": chave_publica_estoque,
        "pagamento": chave_publica_pagamento,
        "entrega": chave_publica_entrega
    }

    thread_consumidor = threading.Thread(target= iniciar_consumidor, arg=(chave_privada, chaves_publicas), daemon=True)
    thread_consumidor.start()

    interface(canal, chave_privada)

    conexao.close()

if __name__ == "__main__":
    main()