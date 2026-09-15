import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import random
import shared.auxi as auxi
import shared.rabbitmq as rmq

FILA_PAGAMENTO = "fila_pagamento"
CHAVE_PRIVADA = os.path.join(BASE_DIR, "chaves", "privada.pem")
CHAVE_PUBLICA_ESTOQUE = os.path.join(BASE_DIR, "chaves", "publicas", "estoque.pem")

def processar_pagamento(canal, evento, chave_privada):
    dados = evento['dados']
    pedido_id = dados['pedido_id']
    produtos = dados['produtos']

    print(f"Processando pagamento do pedido {pedido_id}...")

    # Simulação de processamento de pagamento
    aprovado = random.choice([True, False])

    if aprovado:
        print("Pagamento do pedido aprovado.")

        evento_confirmacao = auxi.criar_evento("pagamento.aprovado", {"pedido_id": pedido_id, "produtos": produtos})

        rmq.publicar_evento(canal, rmq.EXCHANGE_ECOMMERCE, evento_confirmacao, chave_privada)
        print(f"Pagamento aprovado: {evento_confirmacao['tipo']} | Routing Key: {evento_confirmacao['tipo']}")

    else:
        print("Pagamento do pedido recusado.")

        evento_resposta = auxi.criar_evento("pagamento.recusado", {"pedido_id": pedido_id, "produtos": produtos})

        rmq.publicar_evento(canal, rmq.EXCHANGE_ECOMMERCE, evento_resposta, chave_privada)
        print(f"Pagamento recusado: {evento_resposta['tipo']} | Routing Key: {evento_resposta['tipo']}")

def receber_evento(canal, metodo, propriedades, corpo, chave_privada, chave_publica_estoque):
    evento = auxi.json_para_evento(corpo)

    print(f"\nEvento recebido: {evento['tipo']}")
    print(f"Pedido: {evento['dados']['pedido_id']}")

    assinatura_valida = auxi.verificar_assinatura(evento, chave_publica_estoque)

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
    conexao, canal = rmq.criar_canal()
    print("Conexão com RabbitMQ estabelecida.")

    canal.queue_declare(queue=FILA_PAGAMENTO, durable=True)

    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_PAGAMENTO, routing_key="pedido.estoque_ok")

    print("Fila criada:", FILA_PAGAMENTO)

    print("Carregando chave privada...")
    chave_privada = auxi.carregar_chave_privada(CHAVE_PRIVADA)
    print("Chave privada carregada.")

    print("Carregando chave pública do serviço do Estoque...")
    chave_publica_estoque = auxi.carregar_chave_publica(CHAVE_PUBLICA_ESTOQUE)
    print("Chave pública do serviço do Estoque carregada.")

    canal.basic_qos(prefetch_count=1)

    canal.basic_consume(queue=FILA_PAGAMENTO, on_message_callback=lambda ch, method, properties, body:
                        receber_evento(ch, method, properties, body, chave_privada, chave_publica_estoque),
                        auto_ack=False)  

    print("Aguardando eventos...")                  

    canal.start_consuming()

if __name__ == "__main__":
    main()