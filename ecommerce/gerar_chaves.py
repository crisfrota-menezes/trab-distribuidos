from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

servicos = [
    'entrega',
    'estoque',
    'pagamento',
    'principal',
    'promocao'
]

chaves_publicas = {}

for servico in servicos:
    chave_privada = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )

    chave_privada_pem = chave_privada.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    chave_publica = chave_privada.public_key()
    chave_publica_pem = chave_publica.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo
    )

    chaves_publicas[servico] = chave_publica_pem

    pasta_chaves = os.path.join(BASE_DIR, servico, "chaves")
    os.makedirs(pasta_chaves, exist_ok=True)

    with open(os.path.join(pasta_chaves, "privada.pem"), "wb") as f:
        f.write(chave_privada_pem)

for servico in servicos:
    pasta_publicas = os.path.join(BASE_DIR, servico, "chaves", "publicas")
    os.makedirs(pasta_publicas, exist_ok=True)

    for produtor, pub_bytes in chaves_publicas.items():
        if produtor == servico:
            continue

        caminho_pub = os.path.join(pasta_publicas, f"{produtor}.pem")
        with open(caminho_pub, "wb") as f:
            f.write(pub_bytes)