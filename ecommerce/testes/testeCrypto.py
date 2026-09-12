from shared.crypto import (
    criar_evento,
    carregar_chave_privada,
    carregar_chave_publica,
    assinar_evento,
    verificar_assinatura
)


print("1. Carregando chave privada do Principal...")

chave_privada = carregar_chave_privada(
    "principal/chaves/privada.pem"
)

print("OK")


print("\n2. Carregando chave pública do Principal armazenada no Estoque...")

chave_publica = carregar_chave_publica(
    "estoque/chaves/publicas/principal.pem"
)

print("OK")


print("\n3. Criando evento...")

evento = criar_evento(
    "pedido.criado",
    {
        "pedido_id": 1,
        "produtos": [
            {
                "produto_id": 10,
                "quantidade": 2
            }
        ]
    }
)

print(evento)


print("\n4. Assinando evento...")

assinar_evento(evento, chave_privada)

print("Assinatura criada!")
print("Tamanho da assinatura:", len(evento["assinatura"]))


print("\n5. Verificando assinatura...")

resultado = verificar_assinatura(
    evento,
    chave_publica
)

print("Resultado:", resultado)