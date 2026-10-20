import os
import sys
import threading
import uvicorn
import requests
from fastapi import FastAPI, HTTPException, Body

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import shared.auxi as auxi
import shared.rabbitmq as rmq

FILA_PAGAMENTO = "fila_pagamento"
CHAVE_PRIVADA = os.path.join(BASE_DIR, "chaves", "privada.pem")
CHAVE_PUBLICA_ESTOQUE = os.path.join(BASE_DIR, "chaves", "publicas", "estoque.pem")

MOCK_PAGAMENTO_URL = "http://127.0.0.1:5001/cobranca"
WEBHOOK_URL = "http://127.0.0.1:8002/webhook/pagamento"

app = FastAPI()

pedidos_pendentes = {}

@app.post("/webhook/pagamento")
async def processar_pagamento(dados: dict = Body(...)):
    pedido_id = dados['pedido_id']
    status = dados.get("status")

    if not pedido_id or not status:
        raise HTTPException(status_code=400, detail="Dados inválidos no Webhook")

    info_pedido = pedidos_pendentes.pop(pedido_id, {})
    produtos = info_pedido.get("produtos", [])
    valor_total = info_pedido.get("valor_total", 12.0)

    chave_privada = auxi.carregar_chave_privada(CHAVE_PRIVADA)
    tipo_evento = "pagamento.aprovado" if status == "APROVADO" else "pagamento.recusado"
    evento = auxi.criar_evento(tipo_evento, {"pedido_id": pedido_id, "produtos": produtos, "valor_total": valor_total})

    try:
        conexao, canal = rmq.criar_canal()
        rmq.publicar_evento(canal, rmq.EXCHANGE_ECOMMERCE, evento, chave_privada)
        conexao.close()
    except Exception as e:
        print(f"Erro ao publicar evento de pagamento no RabbitMQ: {e}")
        raise HTTPException(status_code=500, detail="Erro ao publicar evento no barramento.")

    return {"status": "recebido"}

def receber_evento_estoque(canal, metodo, propriedades, corpo, chave_privada, chave_publica_estoque):
    evento = auxi.json_para_evento(corpo)
    
    if evento['tipo'] == "pedido.estoque_ok":
        if not auxi.verificar_assinatura(evento, chave_publica_estoque):
            canal.basic_ack(delivery_tag=metodo.delivery_tag)
            return
    
    dados = evento["dados"]
    pedido_id = dados["pedido_id"]
    produtos = dados.get("produtos", [])

    valor_total = sum(p.get("quantidade", 1) * p.get("valor_unitario", 12.0) for p in produtos)
    if valor_total <= 0:
            valor_total = 12.0

    pedidos_pendentes[pedido_id] = {
        "valor_total": valor_total,
        "produtos": produtos
    }

    try:
        resp = requests.post(MOCK_PAGAMENTO_URL, json={
            "pedido_id": pedido_id,
            "valor": valor_total,
            "webhook_url": WEBHOOK_URL
        }, timeout=5)

        if resp.status_code == 200:
            checkout_url = resp.json().get("checkout_url")

            import webbrowser
            try:
                webbrowser.open(checkout_url)
            except Exception as e:
                print(f"Não foi possível abrir navegador automaticamente: {e}")
                print(f"\n💳 [PAGAMENTO] Link de Checkout para o pedido #{pedido_id}: {checkout_url}\n")

            evento_pagamento = auxi.criar_evento("pagamento.aguardando", {
                    "pedido_id": pedido_id,
                    "checkout_url": checkout_url
                })
            rmq.publicar_evento(canal, rmq.EXCHANGE_ECOMMERCE, evento_pagamento, chave_privada)
        else:
            print(f"Erro ao conectar ao Mock de Pagamento: Status {resp.status_code}")

    except Exception as e:
        print(f"Erro ao solicitar cobrança ao Mock de Pagamento: {e}")

    canal.basic_ack(delivery_tag=metodo.delivery_tag)


def iniciar(chave_privada, chave_publica_estoque):
    conexao, canal = rmq.criar_canal()
    canal.queue_declare(queue=FILA_PAGAMENTO, durable=True)
    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_PAGAMENTO, routing_key="pedido.estoque_ok")
    canal.basic_qos(prefetch_count=1)

    canal.basic_consume(
        queue=FILA_PAGAMENTO,
        on_message_callback=lambda ch, method, properties, body: receber_evento_estoque(
            ch, method, properties, body, chave_privada, chave_publica_estoque
        ),
        auto_ack=False
    )
    canal.start_consuming()

if __name__ == "__main__":
    chave_privada = auxi.carregar_chave_privada(CHAVE_PRIVADA)
    chave_publica_estoque = auxi.carregar_chave_publica(CHAVE_PUBLICA_ESTOQUE)

    t = threading.Thread(target=iniciar, args=(chave_privada, chave_publica_estoque), daemon=True)
    t.start()

    uvicorn.run(app, host="127.0.0.1", port=8002)