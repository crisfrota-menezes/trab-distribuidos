import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import shared.auxi as auxi
import shared.rabbitmq as rmq

FILA_ESTOQUE = "fila_estoque"
CHAVE_PRIVADA = os.path.join(BASE_DIR, "chaves", "privada.pem")
CHAVE_PUBLICA_PRINCIPAL = os.path.join(BASE_DIR, "chaves", "publicas", "principal.pem")

#Catálogo de Produtos
produtos = {
    1: {
        "produto_id": 1,
        "nome": "Monster Tradicional",
        "valor": 12.00,
        "quantidade": 30
    },

    2: {
        "produto_id": 2,
        "nome": "Monster Mango Loco",
        "valor": 12.00,
        "quantidade": 40
    },

    3: {
        "produto_id": 3,
        "nome": "Monster Ultra White",
        "valor": 12.00,
        "quantidade": 50
    },

    4: {
        "produto_id": 4,
        "nome": "Monster Pacific Punch",
        "valor": 12.00,
        "quantidade": 20
    },

    5: {
        "produto_id": 5,
        "nome": "Monster Pipeline Punch",
        "valor": 12.00,
        "quantidade": 15
    },

    6: {
        "produto_id": 6,
        "nome": "Monster Ultra Watermelon",
        "valor": 12.00,
        "quantidade": 10
    },

    7: {
        "produto_id": 7,
        "nome": "Monster Rio Punch",
        "valor": 12.00,
        "quantidade": 0
    }
}

#Enviar lista dos produtos
def enviar_produtos(canal, chave_privada):
    lista_produtos = []

    for produto in produtos.values():
        lista_produtos.append({
            "produto_id": produto["produto_id"],
            "nome": produto["nome"],
            "valor": produto["valor"],
            "quantidade": produto["quantidade"]
        })

    evento = auxi.criar_evento("produto.lista", {"produtos": lista_produtos})

    rmq.publicar_evento(canal, rmq.EXCHANGE_ECOMMERCE, evento, chave_privada)
    print(f"Lista de produtos enviada.")

def processar_pedido(canal, evento, chave_privada):
    dados = evento['dados']
    pedido_id = dados['pedido_id']
    produtos_pedido = dados['produtos']

    print(f"Processando pedido {pedido_id}...")

    estoque_disponivel = True

    for item in produtos_pedido:
        produto_id = item['produto_id']
        quantidade = item['quantidade']

        if produto_id not in produtos:
            print(f"Produto {produto_id} não existe.")
            estoque_disponivel = False
            break

        #Verificar produtos antes de alterar estoque
        if produtos[produto_id]["quantidade"] < quantidade:
            print(f"Estoque insuficiente para o produto {produto_id}")
            estoque_disponivel = False
            break

    #Estoque indisponivel
    if not estoque_disponivel:
        evento_resposta = auxi.criar_evento("estoque.indisponivel", {"pedido_id": pedido_id, "produtos": produtos_pedido})
        rmq.publicar_evento(canal, rmq.EXCHANGE_ECOMMERCE, evento_resposta, chave_privada)
        print(f"Estoque indisponivel para o pedido {pedido_id}.")
        return

    #Reservar estoque
    for item in produtos_pedido:
        produto_id = item["produto_id"]
        quantidade_solicitada = item["quantidade"]

        produtos[produto_id]["quantidade"] -= quantidade_solicitada

        print(f"Produto {produto_id}: -{quantidade_solicitada} unidade(s).")

    print("Estoque reservado com sucesso.")

    evento_resposta = auxi.criar_evento("pedido.estoque_ok", {"pedido_id": pedido_id, "produtos": produtos_pedido})

    rmq.publicar_evento(canal, rmq.EXCHANGE_ECOMMERCE, evento_resposta, chave_privada)
    print(f"Estoque reservado para o pedido {pedido_id}.")

def restaurar_estoque(evento):
    dados = evento["dados"]
    pedido_id = dados["pedido_id"]
    produtos_pedido = dados["produtos"]

    print(f"\nRestaurando estoque do pedido {pedido_id}...")

    for item in produtos_pedido:
        produto_id = item["produto_id"]
        quantidade = item["quantidade"]

        if produto_id in produtos:
            produtos[produto_id]["quantidade"] += quantidade

            print(f"Produto {produto_id}: +{quantidade} unidade(s)")

    print("Estoque restaurado.")


def receber_evento(canal, metodo, propriedades, corpo, chave_privada, chave_publica_principal):
    evento = auxi.json_para_evento(corpo)

    tipo = evento["tipo"]

    print(f"\nEvento recebido: {tipo}")

    assinatura_valida = auxi.verificar_assinatura(evento, chave_publica_principal)

    if not assinatura_valida:
        print("Assinatura inválida. Evento descartado.")
        canal.basic_ack(delivery_tag=metodo.delivery_tag)
        return  

    print("Assinatura válida.")

    match tipo:
        case "pedido.criado":
            processar_pedido(canal, evento, chave_privada)
        case "produto.consulta":
            print("Consulta de produtos recebida.")
            enviar_produtos(canal, chave_privada)
        case "pedido.excluido":
            restaurar_estoque(evento)
        case _:
            print(f"Evento desconhecido: {tipo}")

    canal.basic_ack(delivery_tag=metodo.delivery_tag)

def main():
    conexao, canal = rmq.criar_canal()
    print("Conexão com RabbitMQ estabelecida.")

    canal.queue_declare(queue=FILA_ESTOQUE, durable=True)

    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_ESTOQUE, routing_key="pedido.criado")

    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_ESTOQUE, routing_key="pedido.excluido")

    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_ESTOQUE, routing_key="produto.consulta")

    print(f"Fila '{FILA_ESTOQUE}' vinculada à exchange '{rmq.EXCHANGE_ECOMMERCE}'.")

    print("Carregando chave privada...")
    chave_privada = auxi.carregar_chave_privada(CHAVE_PRIVADA)
    print("Chave privada carregada.")

    print("Carregando chave pública do serviço Principal...")
    chave_publica_principal = auxi.carregar_chave_publica(CHAVE_PUBLICA_PRINCIPAL)
    print("Chave pública do serviço Principal carregada.")

    canal.basic_qos(prefetch_count=1)

    canal.basic_consume(queue=FILA_ESTOQUE, on_message_callback=lambda ch, method, properties, body: 
                        receber_evento(ch, method, properties, body, chave_privada, chave_publica_principal), 
                        auto_ack=False)

    print("Aguardando eventos...")

    canal.start_consuming()

if __name__ == "__main__":
    main()