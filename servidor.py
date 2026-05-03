import socket
import sys
from datetime import datetime
from comum import *

def log_servidor(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}")

def main():
    if len(sys.argv) != 2:
        print("Uso: python3 servidor.py <porta>")
        return

    porta = int(sys.argv[1])
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(('', porta))

    total_sum = 0
    num_reqs = 0
    clientes = {} # {addr: {'last_req': int, 'last_total_sum': int}}

    log_servidor(f"num_reqs 0 total_sum 0")

    while True:
        data, addr = sock.recvfrom(1024)
        tipo, id_req, cont_reqs, valor = desempacotar(data)

        if tipo == TIPO_DESCOBERTA:
            if addr not in clientes:
                clientes[addr] = {'last_req': 0, 'last_total_sum': 0}
            sock.sendto(empacotar(TIPO_ACK, 0, 0, 0), addr)

        elif tipo == TIPO_REQUISICAO:
            cliente = clientes.get(addr)
            if not cliente: continue

            # Lógica de Confiabilidade (Exactly-Once)
            if id_req == cliente['last_req'] + 1:
                num_reqs += 1
                total_sum += valor
                cliente['last_req'] = id_req
                cliente['last_total_sum'] = total_sum
                log_servidor(f"client {addr[0]} id_req {id_req} value {valor} num_reqs {num_reqs} total_sum {total_sum}")
            elif id_req <= cliente['last_req']:
                log_servidor(f"client {addr[0]} DUP!! id_req {id_req} value {valor} num_reqs {num_reqs} total_sum {total_sum}")
            
            # Sempre responde com o estado atualizado do servidor para aquele cliente
            sock.sendto(empacotar(TIPO_ACK, cliente['last_req'], num_reqs, total_sum), addr)

if __name__ == "__main__":
    main()