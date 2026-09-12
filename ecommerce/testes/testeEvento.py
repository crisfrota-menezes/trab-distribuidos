from shared.crypto import criar_evento, evento_para_json, json_para_evento

evento = criar_evento(
    "pedido.criado",
    {
        "pedido_id": 123,
        "produtos": [
            {
                "produto_id": 456,
                "quantidade": 2
            }
        ]
    }
)

print("Evento criado:")
print(evento)

mensagem = evento_para_json(evento)

print("\nEvento convertido para JSON:")
print(mensagem)

evento_recuperado = json_para_evento(mensagem)

print("\nEvento recuperado do JSON:")
print(evento_recuperado)