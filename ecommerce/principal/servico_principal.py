import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import shared.auxi as auxi
from shared.rabbitmq import criar_canal, EXCHANGE_ECOMMERCE

FILA_PRINCIPAL = "fila_principal"

CHAVE_PRIVADA = os.path.join(BASE_DIR, "chaves", "privada.pem")

CHAVE_PUBLICA_ESTOQUE = os.path.join(BASE_DIR, "chaves", "publicas", "estoque.pem")
CHAVE_PUBLICA_PAGAMENTO = os.path.join(BASE_DIR, "chaves", "publicas", "pagamento.pem")
CHAVE_PUBLICA_ENTREGA = os.path.join(BASE_DIR, "chaves", "publicas", "entrega.pem")

#Guarda os pedidos
pedidos = {}

def publicar_evento(canal, evento, chave_privada):
    auxi.assinar_evento(evento, chave_privada)

    canal.basic_publish(
        exchange=EXCHANGE_ECOMMERCE,
        routing_key=evento['tipo'],
        body=auxi.evento_para_json(evento),
    )

    print(f"Evento publicado: {evento['tipo']}")
    print(f"Routing Key: {evento['tipo']}")
    print(f"Pedido: {evento['dados']['pedido_id']}")

def criar_pedido(canal, chave_privada):
    pedido_id = len(pedidos) +1

    pedido = {
        "pedido_id": pedido_id,
        "produtos": [
            {
                "produto_id": 10,
                "quantidade": 2
            }
        ],
        'status': 'criado'
    }

    pedidos[pedido_id] = pedido

    evento = auxi.criar_evento("pedido.criado", pedido)

    publicar_evento(canal, evento, chave_privada)

    print(f"\nPedido {pedido_id} criado.")
    print(f"Status: {pedido['status']}")

def processar_evento(canal, evento, chave_privada):
    tipo = evento["tipo"]
    dados = evento["dados"]
    pedido_id = dados["pedido_id"]

    if pedido_id not in pedidos:
        print(f"Pedido {pedido_id} não encontrado.")
        return

    pedido = pedidos[pedido_id]

    print(f"Processando evento: {tipo}")
    print(f"Pedido: {pedido_id}")

    if tipo == "pedido.estoque_ok":
        pedido["status"] = "estoque_ok"

        print("Estoque reservado com sucesso.")
        print("Status atualizado para: estoque_ok")

    elif tipo == "estoque.indisponivel":
        pedido["status"] = "estoque_indisponivel"

        print("Estoque indisponível.")
        print("Pedido será excluído.")

        evento_exclusao = auxi.criar_evento("pedido.excluido", 
                                              {"pedido_id": pedido_id, "produtos": pedido["produtos"]
                                            }
                                        )

        publicar_evento(canal, evento_exclusao, chave_privada)

        pedido["status"] = "excluido"

    elif tipo == "pagamento.aprovado":
        pedido["status"] = "pagamento_aprovado"

        print("Pagamento aprovado.")
        print(f"Status atualizado para: {pedido["status"]}")

    elif tipo == "pagamento.recusado":
        pedido["status"] = "pagamento_recusado"

        print("Pagamento recusado.")
        print("Pedido será excluído.")

        evento_exclusao = auxi.criar_evento("pedido.excluido", 
                                            {"pedido_id": pedido_id, "produtos": pedido["produtos"]
                                            }
                                        )

        publicar_evento(canal, evento_exclusao, chave_privada)

        pedido["status"] = "excluido"
        
    elif tipo == "pedido.enviado":
        pedido["status"] = "enviado"

        print("Pedido enviado!")
        print(f"Status atualizado para: {pedido["status"]}")    

    else:
        print(f"Evento desconhecido: {tipo}")

def receber_evento(canal, metodo, propriedades, corpo, chave_privada, chaves_publicas):
    evento = auxi.json_para_evento(corpo)

    tipo = evento["tipo"]

    print(f"\n================================")
    print(f"Evento recebido: {tipo}")
    print(f"================================")

    pedido_id = evento["dados"]["pedido_id"]
    print(f"Pedido: {pedido_id}")

    #Descobre qual chave publica usar
    if tipo in ["pedido.estoque_ok", "estoque.indisponivel"]:
        chave_publica = chaves_publicas["estoque"]

    elif tipo in ["pagamento.aprovado", "pagamento.recusado"]:
        chave_publica = chaves_publicas["pagamento"]

    elif tipo == "pedido.enviado":
        chave_publica = chaves_publicas["entrega"]

    else:
        print("Evento não esperado pelo Principal.")
        canal.basic_ack(delivery_tag=metodo.delivery_tag)
        return

    assinatura_valida = auxi.verificar_assinatura(evento, chave_publica)

    if not assinatura_valida:
        print("ERRO: assinatura inválida.")
        print("Evento descartado.")

        canal.basic_ack(delivery_tag=metodo.delivery_tag)

        return

    print("Assinatura válida")

    processar_evento(canal, evento, chave_privada)

    canal.basic_ack(delivery_tag=metodo.delivery_tag)

def main():

    conexao, canal = criar_canal()
    print("Conexão com RabbitMQ estabelecida.")

    print("===============================================")
    print("           PRINCIPAL E-COMMERCE")
    print("===============================================")

    canal.queue_declare(queue=FILA_PRINCIPAL, durable=True)

    #Eventos do estoque
    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pedido.estoque_ok")
    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="estoque.indisponivel")

    #Eventos do pagamento
    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pagamento.aprovado")
    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pagamento.recusado")

    #Evento da entrega
    canal.queue_bind(exchange=EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pedido.enviado")

    print(f"Fila criada: {FILA_PRINCIPAL}")

    #Carregar chaves
    print("Carregando chave privada...")
    chave_privada = auxi.carregar_chave_privada(CHAVE_PRIVADA)
    print("Chave privada carregada.")

    print("Carregando chave pública do Estoque...")
    chave_publica_estoque = auxi.carregar_chave_publica(CHAVE_PUBLICA_ESTOQUE)
    print("Chave pública do Estoque carregada.")

    print("Carregando chave pública do Pagamento...")
    chave_publica_pagamento = auxi.carregar_chave_publica(CHAVE_PUBLICA_PAGAMENTO)
    print("Chave pública do Pagamento carregada.")

    print("Carregando chave pública da Entrega...")
    chave_publica_entrega = auxi.carregar_chave_publica(CHAVE_PUBLICA_ENTREGA)
    print("Chave pública da Entrega carregada.")

    chaves_publicas = {
        "estoque": chave_publica_estoque,
        "pagamento": chave_publica_pagamento,
        "entrega": chave_publica_entrega
    }

    #Consumidor
    canal.basic_qos(prefetch_count=1)

    canal.basic_consume(queue=FILA_PRINCIPAL, on_message_callback=lambda ch, method, properties, body:
                        receber_evento(
                            ch, method, properties, body, chave_privada, chaves_publicas),
                        auto_ack=False
                    )

    #criar primeiro pedido
    criar_pedido(canal, chave_privada)

    print("\nAguardando eventos... Precione CTRL+C para encerrar.")

    canal.start_consuming()

if __name__ == "__main__":
    main()