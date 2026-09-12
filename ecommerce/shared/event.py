import json
from datetime import datetime

def criar_evento(tipo, dados):
    evento = {
        'tipo': tipo,
        'dados': dados,
        'timestamp': datetime.now().isoformat(),
        'assinatura': ""
    }

    return evento

def evento_para_json(evento):
    return json.dumps(evento).encode('utf-8')

def json_para_evento(mensagem):
    return json.loads(mensagem.decode('utf-8'))
