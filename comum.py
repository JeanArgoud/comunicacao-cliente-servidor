# comum.py
import struct
import socket

PORTA_SERVICO = 8888

PORTA_INTERNA = 9999

TIPO_DESCOBERTA    = 1
TIPO_NOVO_SERVIDOR = 2
TIPO_ELEICAO       = 3
TIPO_OK            = 4
TIPO_COORDENADOR   = 5
TIPO_REPLICACAO    = 6
TIPO_REQUISICAO    = 7
TIPO_ACK           = 8

# Formato do pacote:
#   tipo         (I) - unsigned int
#   id_remetente (I) - unsigned int  (ID único do servidor)
#   num_reqs     (I) - unsigned int
#   ip_remetente (I) - IP do servidor codificado como inteiro de 32 bits
#   total_sum    (i) - signed int
FORMATO = "!IIIIi"
TAMANHO = struct.calcsize(FORMATO)

def empacotar(tipo, id_remetente, num_reqs, ip_remetente_int, total_sum):
    return struct.pack(FORMATO, tipo, id_remetente, num_reqs, ip_remetente_int, total_sum)

def desempacotar(dados):
    return struct.unpack(FORMATO, dados[:TAMANHO])

def ip_para_int(ip_str):
    """Converte '192.168.1.10' -> inteiro de 32 bits."""
    try:
        return struct.unpack('!I', socket.inet_aton(ip_str))[0]
    except OSError:
        return 0

def int_para_ip(ip_int):
    """Converte inteiro de 32 bits -> '192.168.1.10'."""
    try:
        return socket.inet_ntoa(struct.pack('!I', ip_int))
    except OSError:
        return '0.0.0.0'