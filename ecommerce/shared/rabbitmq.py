import pika
import shared.auxi as auxi

#centralizando a conexao com o RabbitMQ

RABBITMQ_HOST = 'localhost'
RABBITMQ_PORT = 5672
RABBITMQ_USER = 'admin'
RABBITMQ_PASSWORD = 'admin'

EXCHANGE_ECOMMERCE = 'eCommerce'
EXCHANGE_PROMOCAO = 'Promocao'

def criar_conexao():
    credenciais = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parametros = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT, credentials=credenciais)
    return pika.BlockingConnection(parametros)

def criar_canal():
    conexao = criar_conexao()
    canal = conexao.channel()

    canal.exchange_declare(exchange=EXCHANGE_ECOMMERCE, exchange_type='direct', durable=True)

    canal.exchange_declare(exchange=EXCHANGE_PROMOCAO, exchange_type='topic', durable=True)

    return conexao, canal

def publicar_evento(canal, exchange, evento, chave_privada):
    auxi.assinar_evento(evento, chave_privada)
    canal.basic_publish(
        exchange=exchange,
        routing_key=evento["tipo"],
        body=auxi.evento_para_json(evento)
    )
    print(f"Evento publicado: {evento['tipo']} | Routing Key: {evento['tipo']}")