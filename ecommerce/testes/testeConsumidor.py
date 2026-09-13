from shared.rabbitmq import criar_canal, EXCHANGE_ECOMMERCE

conexao, canal = criar_canal()

canal.queue_declare(queue='minha_fila', durable=False)

canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue='minha_fila', routing_key='pedido criado')

def callback(ch, method, properties, body):
    mensagem = body.decode()

    print(f"Mensagem recebida: {mensagem}")
    print(f"Routing Key: {method.routing_key}")

canal.basic_consume(queue='minha_fila', on_message_callback=callback, auto_ack=True)

print("Aguardando mensagens. Pressione Ctrl+C para sair.")

canal.start_consuming()