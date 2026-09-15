import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)
    
import shared.auxi as auxi
import shared.rabbitmq as rmq

FILA_C1 = "fila_consumidor_c1"

def receber_promocao(canal, metodo, propriedades, corpo):
    evento = auxi.json_para_evento(corpo)

    print("\n========== C1 ==========")
    print("Promoção recebida!")
    print(f"Produto: {evento['dados']['nome']}")
    print(f"Valor promocional: R$ {evento['dados']['valor_promocional']:.2f}")
    print(f"Categoria: {evento['dados']['categoria']}")
    print("========================")

    canal.basic_ack(delivery_tag=metodo.delivery_tag)

def main():
    conexao, canal = rmq.criar_canal()

    canal.queue_declare(queue=FILA_C1, durable=True)

    canal.queue_bind(exchange=rmq.EXCHANGE_PROMOCAO, queue=FILA_C1, routing_key="promocao.categoria.A")
    canal.queue_bind(exchange=rmq.EXCHANGE_PROMOCAO, queue=FILA_C1, routing_key="promocao.categoria.B")
    
    canal.basic_consume(queue=FILA_C1, on_message_callback=receber_promocao, auto_ack=False)

    print("Consumidor C1 iniciado.")
    print("Interessado nas categorias A e B")

    canal.start_consuming()

if __name__ == "__main__":
    main()