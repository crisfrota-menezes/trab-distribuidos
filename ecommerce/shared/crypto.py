import hashlib
import json
import base64
from datetime import datetime


from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

def criar_evento(tipo, dados):
    evento = {
        'tipo': tipo,
        'dados': dados,
        'timestamp': datetime.now().isoformat(),
        'assinatura': ""
    }

    return evento

def preparar_conteudo(evento):
    conteudo = {
        'tipo': evento['tipo'],
        'dados': evento['dados'],
        'timestamp': evento['timestamp']
    }

    return json.dumps(conteudo, sort_keys=True, separators=(',', ':')).encode('utf-8')

def gerar_hash(evento):
    conteudo = preparar_conteudo(evento)
    return hashlib.sha256(conteudo).digest()


def carregar_chave_privada (caminho):
    with open(caminho, "rb") as arquivo:
        return serialization.load_pem_private_key(
            arquivo.read(),
            password=None
        )

def carregar_chave_publica(caminho):
    with open(caminho, "rb") as arquivo:
        return serialization.load_pem_public_key(arquivo.read())

def assinar_evento(evento, chave_privada):
    hash_evento = gerar_hash(evento)

    assinatura = chave_privada.sign(
        hash_evento,
        padding.PKCS1v15(),
        hashes.SHA256()
    )

    evento['assinatura'] = base64.b64encode(assinatura).decode('utf-8')

    return evento

def verificar_assinatura(evento, chave_publica):
    if not evento.get('assinatura'):
        return False

    try:
        assinatura = base64.b64decode(evento['assinatura'])

        hash_evento = gerar_hash(evento)

        chave_publica.verify(
            assinatura,
            hash_evento,
            padding.PKCS1v15(),
            hashes.SHA256()
        )

        return True

    except Exception:
        return False

def evento_para_json(evento):
    return json.dumps(evento).encode('utf-8')

def json_para_evento(mensagem):
    return json.loads(mensagem.decode('utf-8'))
