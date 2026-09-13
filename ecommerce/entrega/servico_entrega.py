import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

from shared.rabbitmq import criar_canal, EXCHANGE_ECOMMERCE
import shared.crypto as crypto

FILA_ENTREGA = "fila_entrega"
CHAVE_PRIVADA = os.path.join(BASE_DIR, "chaves", "privada.pem")
CHAVE_PUBLICA_PAGAMENTO = os.path.join(BASE_DIR, "chaves", "publicas", "pagamento.pem")

def publicar_evento(canal, evento, chave_privada):
    crypto.assinar_evento(evento, chave_privada)

    canal.basic_publish(exchange=EXCHANGE_ECOMMERCE, routing_key=evento["tipo"], body=crypto.evento_para_json(evento))

    print(f"Evento publicado: {evento['tipo']}")
    print(f"Routing Key: {evento['tipo']}")
    print(f"Pedido: {evento['dados']['pedido_id']}")

def processar_entrega(canal, evento, chave_privada):
    dados = evento["dados"]
    pedido_id = dados["pedido_id"]
    produtos = dados["produtos"]

    print(f"\nPreparando entrega dp pedido {pedido_id}...")

    print("Gerando nota fiscal...")
    print("Preparando pedido...")
    print("Pedido pronto para envio.")

    evento_resposta = crypto.criar_evento("pedido.enviado",{"pedido_id": pedido_id, "produtos": produtos})

    publicar_evento(canal, evento_resposta, chave_privada)

def receber_evento(canal, metodo, propriedades, corpo, chave_privada, chave_publica_pagamento):
    evento = crypto.json_para_evento(corpo)

    print(f"\nEvento recebido: {evento['tipo']}")
    print(f"Pedido: {evento['dados']['pedido_id']}")

    assinatura_valida = crypto.verificar_assinatura(evento, chave_publica_pagamento)

    if not assinatura_valida:
        print("Assinatura inválida. Evento descartado.")
        canal.basic_ack(delivery_tag=metodo.delivery_tag)
        return

    print("Assinatura válida.")

    if evento["tipo"] == "pagamento.aprovado":
        processar_entrega(canal, evento, chave_privada)
    else:
        print(f"Evento {evento['tipo']} não reconhecido.")

    canal.basic_ack(delivery_tag=metodo.delivery_tag)

def main():
    conexao, canal = criar_canal()

    print("Conexão com RabbitMQ estabelecida.")

    canal.queue_declare(queue=FILA_ENTREGA, durable=True)

    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_ENTREGA, routing_key="pagamento.aprovado")

    print("Fila criada:", FILA_ENTREGA)

    print("Carregando chave privada...")
    chave_privada = crypto.carregar_chave_privada(CHAVE_PRIVADA)
    print("Chave privada carregada.")

    print("Carregando chave pública do serviço principal...")
    chave_publica_pagamento = crypto.carregar_chave_publica(CHAVE_PUBLICA_PAGAMENTO)
    print("Chave pública do serviço de entrega carregada.")

    canal.basic_qos(prefetch_count=1)

    canal.basic_consume(queue=FILA_ENTREGA, on_message_callback=lambda ch, method, properties, body:
                        receber_evento(ch, method, properties, body, chave_privada, chave_publica_pagamento),
                    auto_ack=False
                    )             

    canal.start_consuming()

if __name__ == "__main__":
    main()