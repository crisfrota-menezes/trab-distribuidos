import os
import sys
from fastapi import FastAPI, HTTPException, Request, Body
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import threading
import httpx
import asyncio
import json
from fastapi.responses import StreamingResponse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)

if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import shared.auxi as auxi
import shared.rabbitmq as rmq

FILA_PRINCIPAL = "fila_principal"

CHAVE_PRIVADA = os.path.join(BASE_DIR, "chaves", "privada.pem")

CHAVE_PUBLICA_ESTOQUE = os.path.join(BASE_DIR, "chaves", "publicas", "estoque.pem")
CHAVE_PUBLICA_PAGAMENTO = os.path.join(BASE_DIR, "chaves", "publicas", "pagamento.pem")
CHAVE_PUBLICA_ENTREGA = os.path.join(BASE_DIR, "chaves", "publicas", "entrega.pem")

conexoes_sse: dict[int, list[asyncio.Queue]] = {}
loop_principal: asyncio.AbstractEventLoop = None 
pedidos = {}
produtos = []
proximo_pedido_id = 1
lock = threading.Lock()
evento_produtos = threading.Event()
evento_consumidor_pronto = threading.Event()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def notifica_cliente(pedido_id: int, dados_evento: dict):
    with lock:
        queues = conexoes_sse.get(pedido_id, [])
        for q in queues:
            if loop_principal and loop_principal.is_running():
                loop_principal.call_soon_threadsafe(q.put_nowait, dados_evento)

#def excluir_pedido(canal, chave_privada):
#    try:
#        pedido_id = int(input("\nDigite o ID do pedido: "))
#    except ValueError:
#        print("\nID inválido.")
#        return
#
#    with lock:
#       pedido = pedidos.get(pedido_id)
#       
#    if pedido is None:
#        print("\nPedido não encontrado.")
#        return        
#
#    if pedido["status"] in ["excluido", "enviado"]:
#        print("\nEsse pedido não pode ser excluído.")
#        return
#
#    evento = auxi.criar_evento("pedido.excluido", {"pedido_id": pedido_id, "produtos": pedido["produtos"]})
#    rmq.publicar_evento(canal, rmq.EXCHANGE_ECOMMERCE, evento, chave_privada)
#
#    with lock:
#        pedidos[pedido_id]["status"] = "excluido"
#
def processar_evento(canal, evento, chave_privada):
    global produtos

    tipo = evento["tipo"]
    dados = evento["dados"]
    pedido_id = dados.get("pedido_id")

    match tipo:

        case "produto.lista":
            with lock:
                produtos = dados["produtos"]
            evento_produtos.set()

        case "pedido.estoque_ok":
            with lock:
                if pedido_id in pedidos:
                    pedidos[pedido_id]["status"] = "estoque_ok"
            
            notifica_cliente(pedido_id, {"status": "estoque_ok", "mensagem": "Estoque reservado!"})

        case "estoque.indisponivel":
            with lock:
                if pedido_id in pedidos:
                    pedidos[pedido_id]["status"] = "excluido"
            
            notifica_cliente(pedido_id, {"status": "excluido", "mensagem": "Estoque indisponível! Pedido cancelado."})

            evento_exclusao = auxi.criar_evento("pedido.excluido", {"pedido_id": pedido_id, "produtos": dados.get("produtos", [])})
            rmq.publicar_evento(canal, rmq.EXCHANGE_ECOMMERCE, evento_exclusao, chave_privada)

        case "pagamento.aguardando":
            checkout_url = dados.get("checkout_url")
            with lock:
                if pedido_id in pedidos:
                    pedidos[pedido_id]["status"] = "pagamento_aguardando"
                    pedidos[pedido_id]["checkout_url"] = checkout_url
            
            notifica_cliente(pedido_id, {
                "status": "pagamento_aguardando",
                "mensagem": "Aguardando pagamento no checkout...",
                "checkout_url": checkout_url
            })

        case "pagamento.aprovado":
            with lock:
                if pedido_id in pedidos:
                    pedidos[pedido_id]["status"] = "pagamento_aprovado"
            
            notifica_cliente(pedido_id, {"status": "pagamento_aprovado", "mensagem": "Pagamento aprovado!"})

        case "pagamento.recusado":
            with lock:
                if pedido_id in pedidos:
                    pedidos[pedido_id]["status"] = "excluido"

            notifica_cliente(pedido_id, {"status": "excluido", "mensagem": "Pagamento recusado! Pedido cancelado."})

            evento_exclusao = auxi.criar_evento("pedido.excluido", {"pedido_id": pedido_id, "produtos": dados.get("produtos", [])})
            rmq.publicar_evento(canal, rmq.EXCHANGE_ECOMMERCE, evento_exclusao, chave_privada)                

        case "pedido.enviado":
            with lock:
                if pedido_id in pedidos:
                    pedidos[pedido_id]["status"] = "enviado"

            notifica_cliente(pedido_id, {"status": "enviado", "mensagem": "Pedido faturado e enviado para entrega!"})

        case _:
            print(f"\nEvento não esperado pelo Principal: {tipo}")

def receber_evento(canal, metodo, propriedades, corpo, chave_privada, chaves_publicas):
    evento = auxi.json_para_evento(corpo)

    tipo = evento["tipo"]

    #Descobre qual chave publica usar
    match tipo:
        case "produto.lista":
            chave_publica = chaves_publicas["estoque"]

        case "pedido.estoque_ok":
            chave_publica = chaves_publicas["estoque"]

        case "estoque.indisponivel":
            chave_publica = chaves_publicas["estoque"]

        case "pagamento.aguardando":
            chave_publica = chaves_publicas["pagamento"]

        case "pagamento.aprovado":
            chave_publica = chaves_publicas["pagamento"]

        case "pagamento.recusado":
            chave_publica = chaves_publicas["pagamento"]

        case "pedido.enviado":
            chave_publica = chaves_publicas["entrega"]

        case _:
            print("\nEvento não esperado pelo Principal.")
            canal.basic_ack(delivery_tag=metodo.delivery_tag)
            return

    assinatura_valida = auxi.verificar_assinatura(evento, chave_publica)

    if not assinatura_valida:
        canal.basic_ack(delivery_tag=metodo.delivery_tag)

        return

    processar_evento(canal, evento, chave_privada)

    canal.basic_ack(delivery_tag=metodo.delivery_tag)

def iniciar_consumidor(chave_privada, chaves_publicas):
    conexao, canal = rmq.criar_canal()

    canal.queue_declare(queue=FILA_PRINCIPAL, durable=True)

    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="produto.lista")
    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pedido.estoque_ok")
    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="estoque.indisponivel")

    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pagamento.aguardando")
    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pagamento.aprovado")
    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pagamento.recusado")

    canal.queue_bind(exchange=rmq.EXCHANGE_ECOMMERCE, queue=FILA_PRINCIPAL, routing_key="pedido.enviado")

    canal.basic_qos(prefetch_count=1)

    canal.basic_consume(queue=FILA_PRINCIPAL, on_message_callback=lambda ch, method, properties, body:
                        receber_evento(ch, method, properties, body, chave_privada, chaves_publicas),
                        auto_ack=False)

    evento_consumidor_pronto.set()

    canal.start_consuming()

@app.get("/produtos")
async def consulta_produtos():
    url = "http://localhost:8001/produtos"

    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            raise HTTPException(
                status_code=exc.response.status_code, 
                detail="Erro ao comunicar com o outro serviço"
            )
        except httpx.RequestError:
            raise HTTPException(
                status_code=503, 
                detail="Serviço externo indisponível"
            )

@app.get("/pedidos/stream/{pedido_id}")
async def stream_pedido(pedido_id: int):
    fila = asyncio.Queue()

    async def gerar_evento():
        with lock:
            if pedido_id not in conexoes_sse:
                conexoes_sse[pedido_id] = []
            conexoes_sse[pedido_id].append(fila)

        try:
            with lock:
                status_atual = pedidos.get(pedido_id, {}).get("status", "criado")
            yield f"data: {json.dumps({'status': status_atual, 'mensagem': 'Conexão SSE estabelecida.'})}\n\n"

            while True:
                dados_evento = await fila.get()
                yield f"data: {json.dumps(dados_evento)}\n\n"
                
                if dados_evento.get("status") in ["enviado", "excluido"]:
                    break

        except asyncio.CancelledError:
            print(f"\nErro no SSE do pedido: {pedido_id}")
            pass
        finally:
            with lock:
                if pedido_id in conexoes_sse and fila in conexoes_sse[pedido_id]:
                    conexoes_sse[pedido_id].remove(fila)

    return StreamingResponse(gerar_evento(), media_type="text/event-stream")

@app.post("/pedidos")
async def criar_pedido(request: Request):
    dados = await request.json()
    produtos = dados.get("produtos", [])

    if not produtos:
        raise HTTPException(status_code=400, detail="O pedido não possui produtos.")

    global proximo_pedido_id

    pedido_id = proximo_pedido_id
    proximo_pedido_id += 1

    pedido = {
        "pedido_id": pedido_id,
        "produtos": produtos
    }

    with lock:
        pedidos[pedido_id] = pedido

    evento = auxi.criar_evento("pedido.criado", pedido)

    chave_privada = auxi.carregar_chave_privada(CHAVE_PRIVADA)

    try:
        conexao, canal = rmq.criar_canal()
        rmq.publicar_evento(canal, rmq.EXCHANGE_ECOMMERCE, evento, chave_privada)
        conexao.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no RabbitMQ: {str(e)}")

    return {
        "status": "sucesso",
        "pedido_id": pedido_id,
        "mensagem": "Pedido criado e enviado para processamento."
    }

@app.post("/promocoes/inscrever")
async def inscrever_promocao(dados: dict = Body(...)):
    email = dados.get("email")
    categorias = dados.get("categorias", [])

    if not email or not categorias:
        raise HTTPException(status_code=400, detail="Email e categorias são obrigatórios.")

    conteudo = {"email": email, "categorias": categorias}
    evento = auxi.criar_evento("interesse.promocao", conteudo)

    evento["nome_evento"] = "interesse.promocao"
    evento["conteudo"] = conteudo

    chave_privada = auxi.carregar_chave_privada(CHAVE_PRIVADA)

    try:
        conexao, canal = rmq.criar_canal()
        rmq.publicar_evento(canal, rmq.EXCHANGE_PROMOCAO, evento, chave_privada, routing_key="interesse.promocao")
        conexao.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro no RabbitMQ: {str(e)}")

    return {
        "status": "sucesso",
        "mensagem": "Inscrição realizada com sucesso."
    }

@app.delete("/promocoes/cancelar")
async def cancelar_promocao(dados: dict = Body(...)):
    email = dados.get("email")

    if not email:
        raise HTTPException(status_code=400, detail="O e-mail é obrigatório para cancelar a inscrição.")

    conteudo = {
        "email": email
    }

    evento = auxi.criar_evento("inscricao.cancelada", conteudo)
    
    evento["nome_evento"] = "inscricao.cancelada"
    evento["conteudo"] = conteudo

    chave_privada = auxi.carregar_chave_privada(CHAVE_PRIVADA)

    try:
        conexao, canal = rmq.criar_canal()
        rmq.publicar_evento(
            canal, 
            rmq.EXCHANGE_PROMOCAO, 
            evento, 
            chave_privada, 
            routing_key="inscricao.cancelada"
        )
        conexao.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao publicar no RabbitMQ: {str(e)}")

    return {
        "status": "sucesso",
        "mensagem": "Inscrição cancelada com sucesso."
    }

@app.on_event("startup")
async def startup_event():
    global loop_principal
    loop_principal = asyncio.get_running_loop()

def iniciar():
    conexao, canal = rmq.criar_canal()
    print("Conexão com RabbitMQ estabelecida.")

    chave_privada = auxi.carregar_chave_privada(CHAVE_PRIVADA)

    chave_publica_estoque = auxi.carregar_chave_publica(CHAVE_PUBLICA_ESTOQUE)
    chave_publica_pagamento = auxi.carregar_chave_publica(CHAVE_PUBLICA_PAGAMENTO)
    chave_publica_entrega = auxi.carregar_chave_publica(CHAVE_PUBLICA_ENTREGA)

    chaves_publicas = {
        "estoque": chave_publica_estoque,
        "pagamento": chave_publica_pagamento,
        "entrega": chave_publica_entrega
    }

    thread_consumidor = threading.Thread(target= iniciar_consumidor, args=(chave_privada, chaves_publicas), daemon=True)
    thread_consumidor.start()

    evento_consumidor_pronto.wait()

    conexao.close()

if __name__ == "__main__":
    iniciar()
    uvicorn.run(app, host="127.0.0.1", port=8000)