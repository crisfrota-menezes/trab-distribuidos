import pika

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