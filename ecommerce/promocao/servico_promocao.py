import os
import sys
import random
import json
import requests
import threading
import time
import resend

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import shared.auxi as auxi
import shared.rabbitmq as rmq

CHAVE_PRIVADA = os.path.join(BASE_DIR, 'chaves', 'privada.pem')
JSON_PATH = os.path.join(BASE_DIR, 'promocoes.json')
TEMPLATE_PATH = os.path.join(BASE_DIR, "promocao.html")

RESEND_API_KEY = os.getenv("RESEND_API_KEY", "chave")
resend.api_key = RESEND_API_KEY

def consulta_produtos():
    try:
        response = requests.get("http://localhost:8001/produtos", timeout=5)
        response.raise_for_status()
        lista_produtos = response.json()
        return {p["produto_id"]: p for p in lista_produtos}

    except Exception as error:
        print(f"Não foi possível obter os produtos do estoque: {error}")
        return {}

def carregar_interessados() -> dict:
    if not os.path.exists(JSON_PATH):
        return {"A": [], "B": [], "C": []}
    with open(JSON_PATH, 'r', encoding="utf-8") as f:
        return json.load(f)

def salvar_interessados(interessados: dict):
    with open(JSON_PATH, 'w', encoding="utf-8") as f:
        json.dump(interessados, f, indent=4, ensure_ascii=False)

interessados = carregar_interessados()

def enviar_email(email: str, dados_produto: dict):
    if os.path.exists(TEMPLATE_PATH):
        with open(TEMPLATE_PATH, "r", encoding="utf-8") as f:
            html = f.read()
        html = html.replace("{produto}", str(dados_produto['nome']))
        html = html.replace("{valor_promocional}", f"{dados_produto['valor_promocional']:.2f}")
        html = html.replace("{categoria}", str(dados_produto['categoria']))
    else:
        html = f"<h1>Monster Store</h1><p><b>Promoção Categoria {dados_produto['categoria']}:</b> {dados_produto['nome']} por R$ {dados_produto['valor_promocional']:.2f}</p>"

    params: resend.Emails.SendParams = {
        "from": "Monster Store <onboarding@resend.dev>",
        "to": [email],
        "subject": f"Promoção Categoria {dados_produto['categoria']}: {dados_produto['nome']}!",
        "html": html,
    }

    try:
        res = resend.Emails.send(params)
        print(f"E-mail enviado para {email}: ID {res.get('id')}")
    except Exception as error:
        print(f"Erro ao enviar e-mail para {email}: {error}")

def gerar_promocao(canal, chave_privada):
    while True:
        produtos = consulta_produtos()

        if not produtos:
            print("Estoque vazio!")
            time.sleep(10)
            continue

        item = random.choice(list(produtos.values()))
        categoria = random.choice(["A", "B", "C"])
        
        descontos = { "A": 0.9, "B": 0.8, "C": 0.7 }
        valor_promocional = item['valor'] * descontos[categoria]

        dados = {
            "produto_id": item['produto_id'],
            "nome": item['nome'],
            "valor_promocional": valor_promocional,
            "categoria": categoria
        }

        evento = auxi.criar_evento("promocao.criada", dados)
        routing_key = f"promocao.categoria.{categoria}"
        rmq.publicar_evento(canal, rmq.EXCHANGE_PROMOCAO, evento, chave_privada, routing_key)

        emails_destino = interessados.get(categoria, [])

        for email in emails_destino:
            enviar_email(email, dados)

        time.sleep(60)

def processar_evento_inscriacao(canal, method, properties, body):
    try:
        evento = json.loads(body.decode("utf-8"))
        nome_evento = evento.get("nome_evento")
        conteudo = evento.get("conteudo", {})
    
        if nome_evento == "interesse.promocao":
            email = conteudo.get("email")
            categorias = conteudo.get("categorias", [])

            if email:
                for cat in categorias:
                    if cat not in interessados:
                        interessados[cat] = []
                    if email not in interessados[cat]:
                        interessados[cat].append(email)
            
                salvar_interessados(interessados)

        elif nome_evento == "inscricao.cancelada":
            email = conteudo.get("email")
            if email:
                for cat in interessados:
                    if email in interessados[cat]:
                        interessados[cat].remove(email)
                salvar_interessados(interessados)
        
        canal.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Erro ao processar evento: {e}")
        canal.basic_ack(delivery_tag=method.delivery_tag)

def consumir_promocoes(): 
    conexao, canal = rmq.criar_canal()
    fila_promocoes = "fila_promocoes"

    canal.queue_declare(queue=fila_promocoes, durable=True)
    canal.queue_bind(exchange=rmq.EXCHANGE_PROMOCAO, queue=fila_promocoes, routing_key="interesse.promocao")
    canal.queue_bind(exchange=rmq.EXCHANGE_PROMOCAO, queue=fila_promocoes, routing_key="inscricao.cancelada")

    canal.basic_consume(queue=fila_promocoes, on_message_callback=processar_evento_inscriacao, auto_ack=False)
    
    canal.start_consuming()

if __name__ == "__main__":
    chave_privada = auxi.carregar_chave_privada(CHAVE_PRIVADA)

    thread_rabbit = threading.Thread(target=consumir_promocoes, daemon=True)
    thread_rabbit.start()

    conexao_p, canal_p = rmq.criar_canal()
    
    try:
        gerar_promocao(canal_p, chave_privada)
    except KeyboardInterrupt:
        print("Parando serviço...")
        conexao_p.close()
    