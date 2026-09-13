import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import shared.crypto as crypto
from shared.rabbitmq import criar_canal, EXCHANGE_ECOMMERCE

FILA_ESTOQUE = "fila_estoque"
CHAVE_PRIVADA = os.path.join(BASE_DIR, "chaves", "privada.pem")
CHAVE_PUBLICA_PRINCIPAL = os.path.join(BASE_DIR, "chaves", "publicas", "principal.pem")

#simulação de estoque
estoque = {
    10:10,
    20:5,
    30:0
}

def publicar_evento(canal, evento, chave_privada):
    crypto.assinar_evento(evento, chave_privada)

    canal.basic_publish(
        exchange=EXCHANGE_ECOMMERCE,
        routing_key=evento['tipo'],
        body=crypto.evento_para_json(evento),
    )

    print(f"Evento publicado: {evento['tipo']}")
    print(f"Routing Key: {evento['tipo']}")

def processar_evento(canal, evento, chave_privada):
    dados = evento['dados']
    pedido_id = dados['pedido_id']
    produtos = dados['produtos']

    print(f"Processando pedido {pedido_id}...")

    disponibilidade = True

    for produto in produtos:
        produto_id = produto['produto_id']
        quantidade = produto['quantidade']

        quantidade_estoque = estoque.get(produto_id, 0)

        print(f"Produto {produto_id}: quantidade solicitada = {quantidade}, quantidade em estoque = {quantidade_estoque}")

        if quantidade_estoque < quantidade:
            disponibilidade = False

    if disponibilidade:

        for produto in produtos:
            produto_id = produto['produto_id']
            quantidade = produto['quantidade']

            estoque[produto_id] -= quantidade

        print(f"Pedido {pedido_id} processado com sucesso. Estoque atualizado.")

        evento_confirmacao = crypto.criar_evento("pedido.confirmado", {"pedido_id": pedido_id, "produtos": produtos})

        publicar_evento(canal, evento_confirmacao, chave_privada)

    else:
        print(f"\nPedido {pedido_id} não pode ser processado devido à falta de estoque.")

        evento_resposta = crypto.criar_evento("estoque.indisponivel", {"pedido_id": pedido_id, "produtos": produtos})

        publicar_evento(canal, evento_resposta, chave_privada)

def receber_evento(canal, metodo, propriedades, corpo, chave_privada, chave_publica_principal):
    evento = crypto.json_para_evento(corpo)

    print(f"\nEvento recebido: {evento['tipo']}")
    print(f"Pedido: {evento['dados']['pedido_id']}")

    assinatura_valida = crypto.verificar_assinatura(evento, chave_publica_principal)

    if not assinatura_valida:
        print("Assinatura inválida. Evento descartado.")
        canal.basic_ack(delivery_tag=metodo.delivery_tag)
        return  

    if evento['tipo'] == "pedido.criado":
        processar_evento(canal, evento, chave_privada)
    else:
        print(f"Evento desconhecido: {evento['tipo']}")

    canal.basic_ack(delivery_tag=metodo.delivery_tag)

def main():
    conexao, canal = criar_canal()
    print("Conexão com RabbitMQ estabelecida.")

    canal.queue_declare(queue=FILA_ESTOQUE, durable=True)

    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_ESTOQUE, routing_key="pedido.criado")

    print(f"Fila '{FILA_ESTOQUE}' vinculada à exchange '{EXCHANGE_ECOMMERCE}' com a routing key 'pedido.criado'.")

    print("Carregando chave privada...")
    chave_privada = crypto.carregar_chave_privada(CHAVE_PRIVADA)
    print("Chave privada carregada.")

    print("Carregando chave pública do serviço principal...")
    chave_publica_principal = crypto.carregar_chave_publica(CHAVE_PUBLICA_PRINCIPAL)
    print("Chave pública do serviço principal carregada.")

    canal.basic_qos(prefetch_count=1)

    canal.basic_consume(queue=FILA_ESTOQUE, on_message_callback=lambda ch, method, properties, body: 
                        receber_evento(ch, method, properties, body, chave_privada, chave_publica_principal), auto_ack=False)

    print("Aguardando eventos...")

    canal.start_consuming()

if __name__ == "__main__":
    main()