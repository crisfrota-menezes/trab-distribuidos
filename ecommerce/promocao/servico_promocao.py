import os
import sys
import random

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT_DIR = os.path.dirname(BASE_DIR)
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

import shared.auxi as auxi
import shared.rabbitmq as rmq

CHAVE_PRIVADA = os.path.join(BASE_DIR, 'chaves', 'privada.pem')

produtos = {
    1: {
        "produto_id": 1,
        "nome": "Monster Tradicional",
        "valor": 12.00,
        "quantidade": 30
    },

    2: {
        "produto_id": 2,
        "nome": "Monster Mango Loco",
        "valor": 12.00,
        "quantidade": 40
    },

    3: {
        "produto_id": 3,
        "nome": "Monster Ultra White",
        "valor": 12.00,
        "quantidade": 50
    },

    4: {
        "produto_id": 4,
        "nome": "Monster Pacific Punch",
        "valor": 12.00,
        "quantidade": 20
    },

    5: {
        "produto_id": 5,
        "nome": "Monster Pipeline Punch",
        "valor": 12.00,
        "quantidade": 15
    },

    6: {
        "produto_id": 6,
        "nome": "Monster Ultra Watermelon",
        "valor": 12.00,
        "quantidade": 10
    },

    7: {
        "produto_id": 7,
        "nome": "Monster Rio Punch",
        "valor": 12.00,
        "quantidade": 0
    }
}

def gerar_promocao():
    item = random.choice(list(produtos.values()))
    categoria = random.choice(["A", "B", "C"])
    
    match categoria:
        case "A":
            valor_promocional = item['valor'] * 0.9
        case "B":
            valor_promocional = item['valor'] * 0.8
        case "C":
            valor_promocional = item['valor'] * 0.7
        case _:
            return

    dados = {
        "produto_id": item['produto_id'],
        "nome": item['nome'],
        "valor_promocional": valor_promocional,
        "categoria": categoria
    }

    return dados

def processar_promocao(canal, chave_privada):
    dados = gerar_promocao()

    evento = auxi.criar_evento("promocao.criada", dados)

    routing_key = f"promocao.categoria.{dados['categoria']}"

    rmq.publicar_evento(canal, rmq.EXCHANGE_PROMOCAO, evento, chave_privada, routing_key)

    print("\nPromoção publicada!") 
    print(f"Produto: {dados['nome']}")
    print(f"Valor promocional: R$ {dados['valor_promocional']:.2f}") 
    print(f"Categoria: {dados['categoria']}") 
    print(f"Routing Key: {routing_key}")
    
def main(): 
    conexao, canal = rmq.criar_canal()
    
    print("Conexão com RabbitMQ estabelecida.")
    print("Carregando chave privada...")
    
    chave_privada = auxi.carregar_chave_privada(CHAVE_PRIVADA)
    
    print("Chave privada carregada.")
    
    processar_promocao(canal, chave_privada)
    
    conexao.close()

if __name__ == "__main__":
    main()