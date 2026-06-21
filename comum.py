import struct

# Formato: 1 byte (Tipo), 8 bytes (ID Req), 8 bytes (Valor/Soma)
# '!' = Network Byte Order, 'B' = unsigned char, 'Q' = unsigned long long (64 bits)
FORMATO_PACOTE = "!BQQQ"

TIPO_DESCOBERTA = 0
TIPO_REQUISICAO = 1
TIPO_ACK = 2
TIPO_NOVO_SERVIDOR = 3

PORTA_LIDER_INICIAL = 12345

def empacotar(tipo, id_req, num_req, valor):
    return struct.pack(FORMATO_PACOTE, tipo, id_req, num_req, valor)

def desempacotar(buffer):
    return struct.unpack(FORMATO_PACOTE, buffer)