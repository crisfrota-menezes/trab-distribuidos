import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import random
from shared.rabbitmq import criar_canal, EXCHANGE_ECOMMERCE
import shared.crypto as crypto

FILA_PAGAMENTO = "fila_pagamento"
CHAVE_PRIVADA = os.path.join(BASE_DIR, "chaves", "privada.pem")
CHAVE_PUBLICA_ESTOQUE = os.path.join(BASE_DIR, "chaves", "publicas", "estoque.pem")

def publicar_evento(canal, evento, chave_privada):
    crypto.assinar_evento(evento, chave_privada)

    canal.basic_publish(
        exchange=EXCHANGE_ECOMMERCE,
        routing_key=evento['tipo'],
        body=crypto.evento_para_json(evento),
    )

    print(f"Evento publicado: {evento['tipo']}")
    print(f"Routing Key: {evento['tipo']}")
    print(f"Pedido: {evento['dados']['pedido_id']}")

def processar_pagamento(canal, evento, chave_privada):
    dados = evento['dados']
    pedido_id = dados['pedido_id']
    produtos = dados['produtos']

    print(f"Processando pagamento do pedido {pedido_id}...")

    # Simulação de processamento de pagamento
    aprovado = random.choice([True, False])

    if aprovado:
        print("Pagamento do pedido aprovado.")

        evento_confirmacao = crypto.criar_evento("pagamento.aprovado", {"pedido_id": pedido_id, "produtos": produtos})

        publicar_evento(canal, evento_confirmacao, chave_privada)

    else:
        print("Pagamento do pedido recusado.")

        evento_resposta = crypto.criar_evento("pagamento.recusado", {"pedido_id": pedido_id, "produtos": produtos})

        publicar_evento(canal, evento_resposta, chave_privada)

def receber_evento(canal, metodo, propriedades, corpo, chave_privada, chave_publica_estoque):
    evento = crypto.json_para_evento(corpo)

    print(f"\nEvento recebido: {evento['tipo']}")
    print(f"Pedido: {evento['dados']['pedido_id']}")

    assinatura_valida = crypto.verificar_assinatura(evento, chave_publica_estoque)

    if not assinatura_valida:
        print("Assinatura inválida. Evento descartado.")
        canal.basic_ack(delivery_tag=metodo.delivery_tag)
        return

    print("Assinatura válida. Processando evento...")

    if evento['tipo'] == "pedido.estoque_ok":
        processar_pagamento(canal, evento, chave_privada)
    else:
        print(f"Evento {evento['tipo']} não é relevante para o serviço de pagamento.")

    canal.basic_ack(delivery_tag=metodo.delivery_tag)

def main():
    conexao, canal = criar_canal()

    print("Conexão com RabbitMQ estabelecida.")

    canal.queue_declare(queue=FILA_PAGAMENTO, durable=True)

    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_PAGAMENTO, routing_key="pedido.estoque_ok")

    print("Fila criada:", FILA_PAGAMENTO)

    print("carregando chave privada.")
    chave_privada = crypto.carregar_chave_privada(CHAVE_PRIVADA)
    print("Chave privada carregada.")

    print("carregando chave privada.")
    chave_publica_estoque = crypto.carregar_chave_publica(CHAVE_PUBLICA_ESTOQUE)
    print("Chave publica do Estoque carregada.")

    canal.basic_consume(queue=FILA_PAGAMENTO, on_message_callback=lambda ch, method, properties, body:
                        receber_evento(ch, method, properties, body, chave_privada, chave_publica_estoque),
                    auto_ack=False
                    )                    

    canal.start_consuming()

if __name__ == "__main__":
    main()
    
