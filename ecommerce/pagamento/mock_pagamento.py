import os
import uuid
import httpx
import uvicorn
from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

app = FastAPI(title="Mock de Pagamento Externo")

# Armazenamento em memória das cobranças
cobrancas = {}

class CobrancaRequest(BaseModel):
    pedido_id: int
    valor: float
    webhook_url: str

@app.post("/cobranca")
async def criar_cobranca(req: CobrancaRequest):
    cobranca_id = str(uuid.uuid4())
    cobrancas[cobranca_id] = {
        "cobranca_id": cobranca_id,
        "pedido_id": req.pedido_id,
        "valor": req.valor,
        "webhook_url": req.webhook_url,
        "status": "PENDENTE"
    }
    return {"checkout_url": f"http://localhost:5001/checkout/{cobranca_id}"}

@app.get("/checkout/{cobranca_id}", response_class=HTMLResponse)
async def pagina_checkout(cobranca_id: str):
    cobranca = cobrancas.get(cobranca_id)
    if not cobranca:
        return HTMLResponse("<h1>Cobrança não encontrada ou expirada</h1>", status_code=404)

    return f"""
    <!DOCTYPE html>
    <html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <title>Mock de Pagamento - Checkout</title>
        <style>
            body {{ font-family: system-ui, sans-serif; background: #000; color: #fff; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; }}
            .card {{ background: #171717; padding: 32px; border-radius: 12px; border: 1px solid #333; text-align: center; width: 360px; box-shadow: 0 4px 20px rgba(0,0,0,0.5); }}
            .title {{ font-size: 1.4rem; margin-bottom: 8px; color: #fff; }}
            .price {{ font-size: 2rem; color: #6BD41A; font-weight: 800; margin: 16px 0; }}
            .btn {{ width: 100%; padding: 14px; font-weight: bold; border: none; border-radius: 6px; cursor: pointer; margin-top: 10px; font-size: 1rem; transition: all 0.2s; }}
            .btn-approve {{ background: #25BC39; color: #000; }}
            .btn-approve:hover {{ background: #6BD41A; }}
            .btn-reject {{ background: #ff5252; color: #fff; }}
            .btn-reject:hover {{ opacity: 0.8; }}
        </style>
    </head>
    <body>
        <div class="card">
            <h2 class="title">Monster Store - Checkout</h2>
            <p style="color: #aaa;">Pedido #: <strong>{cobranca['pedido_id']}</strong></p>
            <div class="price">R$ {cobranca['valor']:.2f}</div>
            
            <form action="/checkout/{cobranca_id}/processar" method="post">
                <input type="hidden" name="status" value="APROVADO">
                <button type="submit" class="btn btn-approve">Aprovar Pagamento</button>
            </form>
            
            <form action="/checkout/{cobranca_id}/processar" method="post" style="margin-top: 8px;">
                <input type="hidden" name="status" value="RECUSADO">
                <button type="submit" class="btn btn-reject">Recusar Pagamento</button>
            </form>
        </div>
    </body>
    </html>
    """

@app.post("/checkout/{cobranca_id}/processar", response_class=HTMLResponse)
async def processar_checkout(cobranca_id: str, status: str = Form(...)):
    cobranca = cobrancas.get(cobranca_id)
    if not cobranca:
        return HTMLResponse("<h1>Erro ao processar cobrança</h1>", status_code=400)

    cobranca["status"] = status
    webhook_url = cobranca["webhook_url"]

    # Dispara o Webhook assíncrono para o MS Pagamento
    async with httpx.AsyncClient() as client:
        try:
            await client.post(webhook_url, json={
                "pedido_id": cobranca["pedido_id"],
                "status": status
            }, timeout=5.0)
        except Exception as e:
            print(f"Erro ao disparar Webhook: {e}")

    cor = "#25BC39" if status == "APROVADO" else "#ff5252"
    return f"""
    <!DOCTYPE html>
    <html>
    <body style="background: #000; color: #fff; font-family: sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh;">
        <div style="text-align: center;">
            <h1 style="color: {cor};">Pagamento {status}!</h1>
            <p>Pode fechar esta janela e voltar à loja.</p>
        </div>
    </body>
    </html>
    """

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=5001)