from shared.rabbitmq import criar_canal, EXCHANGE_ECOMMERCE
import shared.crypto as crypto

CHAVE_PRIVADA = "principal/chaves/privada.pem"

def publicar_evento(canal, evento):
    canal.basic_publish(
        exchange=EXCHANGE_ECOMMERCE,
        routing_key=evento['tipo'],
        body=crypto.evento_para_json(evento),
    )

    print(f"Evento publicado: {evento['tipo']}")
    print(f"Tipo: {evento['tipo']}")
    print(f"Routing Key: {evento['tipo']}")
    print(f"Pedido: {evento['dados']['pedido_id']}")

def criar_pedido(canal, chave_privada):
    pedido = {
        "pedido_id": 1,
        "produtos": [
            {
                "produto_id": 10,
                "quantidade": 2
            }
        ],
        'status': 'criado'
    }

    evento = crypto.criar_evento("pedido.criado", pedido)

    crypto.assinar_evento(evento, chave_privada)

    publicar_evento(canal, evento)

    print(f"\nEvento completo: {evento}")


def main():

    conexao, canal = criar_canal()
    print("Conexão com RabbitMQ estabelecida.")

    print("Carregando chave privada...")
    chave_privada = crypto.carregar_chave_privada(CHAVE_PRIVADA)
    print("Chave privada carregada.")

    criar_pedido(canal, chave_privada)

    conexao.close()
    print("Conexão com RabbitMQ encerrada.")

if __name__ == "__main__":
    main()