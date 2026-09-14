import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import shared.auxi as auxi
from shared.rabbitmq import criar_canal, EXCHANGE_ECOMMERCE

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

def publicar_evento(canal, evento, chave_privada):
    auxi.assinar_evento(evento, chave_privada)

    canal.basic_publish(
        exchange=EXCHANGE_ECOMMERCE,
        routing_key=evento['tipo'],
        body=auxi.evento_para_json(evento),
    )

    print(f"Evento publicado: {evento['tipo']}")
    print(f"Routing Key: {evento['tipo']}")

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

    publicar_evento(canal, evento, chave_privada)

def processar_pedido(canal, evento, chave_privada):
    dados = evento['dados']
    pedido_id = dados['pedido_id']
    produtos = dados['produtos']

    print(f"Processando pedido {pedido_id}...")

    estoque_disponivel = True

    for produto in produtos:
        produto_id = produto['produto_id']
        quantidade = produto['quantidade']

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
            evento_resposta = auxi.criar_evento("estoque.indisponivel", {"pedido_id": pedido_id, "produtos": produtos})
            publicar_evento(canal, evento, chave_privada)
            return

        #Reservar estoque
        for produto in produtos:
            produto_id = produto["produto_id"]
            quantidade = produto["quantidade"]

            produto["produto_id"]["quantidade"] -= quantidade

            print(f"Produto {produto_id}: -{quantidade} unidade(s).")

        print("Estoque reservado com sucesso.")

        evento_resposta = auxi.criar_evento("pedido.estoque_ok", {"pedido_id": pedido_id, "produtos": produtos})

        publicar_evento(canal, evento_resposta, chave_privada)

def restaurar_estoque(evento):
    dados = evento["dados"]
    pedido_id = dados["pedido_id"]
    produtos = dados["produtos"]

    print(f"\nRestaurando estoque do pedido {pedido_id}...")

    for produto in produtos:
        produto_id = produto["produto_id"]
        quantidade = produto["quantidade"]

        if produto-id in produtos:
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

    if tipo == "pedido.criado":
        processar_pedido(canal, evento, chave_privada)

    elif tipo == "produto.consulta":
        print("Consulta de produtos recebida.")
        enviar_produtos(canal, chave_privada)

    elif tipo == "pedido.excluido":
        restaurar_estoque(evento)
    
    else:
        print(f"Evento desconhecido: {tipo}")

    canal.basic_ack(delivery_tag=metodo.delivery_tag)

def main():
    conexao, canal = criar_canal()
    print("Conexão com RabbitMQ estabelecida.")

    canal.queue_declare(queue=FILA_ESTOQUE, durable=True)

    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_ESTOQUE, routing_key="pedido.criado")

    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_ESTOQUE, routing_key="pedido.excluido")

    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_ESTOQUE, routing_key="pedido.consulta")

    print(f"Fila '{FILA_ESTOQUE}' vinculada à exchange '{EXCHANGE_ECOMMERCE}'.")

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