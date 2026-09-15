import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import shared.auxi as auxi
import shared.rabbitmq as rmq

FILA_ENTREGA = "fila_entrega"
CHAVE_PRIVADA = os.path.join(BASE_DIR, "chaves", "privada.pem")
CHAVE_PUBLICA_PAGAMENTO = os.path.join(BASE_DIR, "chaves", "publicas", "pagamento.pem")

def processar_entrega(canal, evento, chave_privada):
    dados = evento["dados"]
    pedido_id = dados["pedido_id"]
    produtos = dados["produtos"]

    print(f"\nPreparando entrega do pedido {pedido_id}...")

    print("Gerando nota fiscal...")
    print("Preparando pedido...")
    print("Pedido pronto para envio.")

    evento_resposta = auxi.criar_evento("pedido.enviado",{"pedido_id": pedido_id, "produtos": produtos})

    rmq.publicar_evento(canal, rmq.EXCHANGE_ECOMMERCE, evento_resposta, chave_privada)
    print(f"Evento pedido.enviado publicado para o pedido {pedido_id}.")

def receber_evento(canal, metodo, propriedades, corpo, chave_privada, chave_publica_pagamento):
    evento = auxi.json_para_evento(corpo)

    print(f"\nEvento recebido: {evento['tipo']}")
    print(f"Pedido: {evento['dados']['pedido_id']}")

    assinatura_valida = auxi.verificar_assinatura(evento, chave_publica_pagamento)

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
    conexao, canal = rmq.criar_canal()

    print("Conexão com RabbitMQ estabelecida.")

    canal.queue_declare(queue=FILA_ENTREGA, durable=True)

    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_ENTREGA, routing_key="pagamento.aprovado")

    print("Fila criada:", FILA_ENTREGA)

    print("Carregando chave privada...")
    chave_privada = auxi.carregar_chave_privada(CHAVE_PRIVADA)
    print("Chave privada carregada.")

    print("Carregando chave pública do serviço de Pagamento...")
    chave_publica_pagamento = auxi.carregar_chave_publica(CHAVE_PUBLICA_PAGAMENTO)
    print("Chave pública do serviço de Pagamento carregada.")

    canal.basic_qos(prefetch_count=1)

    canal.basic_consume(queue=FILA_ENTREGA, on_message_callback=lambda ch, method, properties, body:
                        receber_evento(ch, method, properties, body, chave_privada, chave_publica_pagamento),
                        auto_ack=False)        

    print("Aguardando eventos...")     

    canal.start_consuming()

if __name__ == "__main__":
    main()