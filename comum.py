import struct
import socket

# Pacote padrão: tipo (1B), id_req (8B), num_req (8B), valor (8B)
FORMATO_PACOTE = "!BQQQ"

# Pacote de replicação: tipo (1B), num_reqs (8B), total_sum (8B), n_clientes (2B)
# Seguido de n_clientes × (ip uint32 (4B) + porta uint16 (2B) + last_req uint64 (8B))
FORMATO_REPLICACAO_HEADER = "!BQQH"
FORMATO_CLIENTE_ADDR = "!IHQ"

# Pacote de endereço (redirecionamento e coordenador): tipo (1B), ip uint32 (4B), porta uint16 (2B)
FORMATO_ADDR = "!BIH"

# Pacote de eleição: tipo (1B), porta uint16 (2B)
FORMATO_ELEICAO = "!BH"

TIPO_DESCOBERTA       = 0
TIPO_REQUISICAO       = 1
TIPO_ACK              = 2
TIPO_NOVO_SERVIDOR    = 3
TIPO_REPLICACAO       = 4
TIPO_REDIRECIONAMENTO = 5
TIPO_HEARTBEAT        = 6
TIPO_ELEICAO          = 7
TIPO_COORDENADOR      = 8
TIPO_ACK_ELEICAO      = 9

PORTA_DESCOBERTA_SERVIDORES = 8080
TAMANHO_BUFFER = 4096

def empacotar(tipo, id_req, num_req, valor):
    return struct.pack(FORMATO_PACOTE, tipo, id_req, num_req, valor)

def desempacotar(buffer):
    return struct.unpack(FORMATO_PACOTE, buffer)

def empacotar_replicacao(num_reqs, total_sum, clientes):
    header = struct.pack(FORMATO_REPLICACAO_HEADER, TIPO_REPLICACAO, num_reqs, total_sum, len(clientes))
    addrs = b''.join(
        struct.pack(FORMATO_CLIENTE_ADDR, struct.unpack("!I", socket.inet_aton(ip))[0], porta, last_req)
        for (ip, porta), last_req in clientes
    )
    return header + addrs

def desempacotar_replicacao(buffer):
    _, num_reqs, total_sum, n = struct.unpack(FORMATO_REPLICACAO_HEADER, buffer[:19])
    clientes = []
    for i in range(n):
        offset = 19 + i * 14
        ip_int, porta, last_req = struct.unpack(FORMATO_CLIENTE_ADDR, buffer[offset:offset + 14])
        clientes.append(((socket.inet_ntoa(struct.pack("!I", ip_int)), porta), last_req))
    return num_reqs, total_sum, clientes

def empacotar_redirecionamento(ip_str, porta):
    ip_int = struct.unpack("!I", socket.inet_aton(ip_str))[0]
    return struct.pack(FORMATO_ADDR, TIPO_REDIRECIONAMENTO, ip_int, porta)

def desempacotar_redirecionamento(buffer):
    _, ip_int, porta = struct.unpack(FORMATO_ADDR, buffer)
    return socket.inet_ntoa(struct.pack("!I", ip_int)), porta

def empacotar_coordenador(ip_str, porta):
    ip_int = struct.unpack("!I", socket.inet_aton(ip_str))[0]
    return struct.pack(FORMATO_ADDR, TIPO_COORDENADOR, ip_int, porta)

def desempacotar_coordenador(buffer):
    _, ip_int, porta = struct.unpack(FORMATO_ADDR, buffer)
    return socket.inet_ntoa(struct.pack("!I", ip_int)), porta

def empacotar_eleicao(porta):
    return struct.pack(FORMATO_ELEICAO, TIPO_ELEICAO, porta)

def desempacotar_eleicao(buffer):
    _, porta = struct.unpack(FORMATO_ELEICAO, buffer)
    return porta

def empacotar_ack_eleicao():
    return struct.pack("!B", TIPO_ACK_ELEICAO)

def get_meu_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(('8.8.8.8', 80))
        return s.getsockname()[0]
    finally:
        s.close()
