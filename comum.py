import struct

FORMATO_PACOTE = "!BQQQQ"

TIPO_DESCOBERTA = 0
TIPO_REQUISICAO = 1
TIPO_ACK = 2
TIPO_NOVO_SERVIDOR = 3
TIPO_REPLICACAO = 4

TIPO_ELEICAO = 5
TIPO_OK = 6
TIPO_COORDENADOR = 7

PORTA_LIDER_INICIAL = 12345

def empacotar(tipo, id_req, num_req, valor, total_sum):
    return struct.pack(FORMATO_PACOTE, tipo, id_req, num_req, valor, total_sum)

def desempacotar(buffer):
    return struct.unpack(FORMATO_PACOTE, buffer)