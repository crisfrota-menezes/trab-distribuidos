from shared.rabbitmq import criar_canal, EXCHANGE_ECOMMERCE

conexao, canal = criar_canal()

canal.queue_declare(queue='minha_fila', durable=False)

canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue='minha_fila', routing_key='pedido criado')

mensagem = "Olá, RabbitMQ!"

canal.basic_publish(exchange=EXCHANGE_ECOMMERCE, routing_key='pedido criado', body=mensagem)

print(f"Mensagem enviada: {mensagem}")
print("Exchange: eCommerce")
print("Routing Key: pedido criado")

conexao.close()