import socket
import sys
from datetime import datetime
from comum import *

def log_servidor(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}")

class ServidorSoma:
    def __init__(self, porta, lider):
        self.porta = porta
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('', porta))
        self.total_sum = 0
        self.num_reqs = 0
        self.clientes = {}
        self.lider = lider

    def iniciar(self):
        log_servidor(f"num_reqs 0 total_sum 0")
        while True:
            data, addr = self.sock.recvfrom(1024)
            tipo, id_req, cont_reqs, valor = desempacotar(data)

            if tipo == TIPO_DESCOBERTA:
                if addr not in self.clientes:
                    self.clientes[addr] = {'last_req': 0, 'last_total_sum': 0}
                self.sock.sendto(empacotar(TIPO_ACK, 0, 0, 0), addr)

            elif tipo == TIPO_REQUISICAO:
                cliente = self.clientes.get(addr)
                if not cliente: continue

                if id_req == cliente['last_req'] + 1:
                    self.num_reqs += 1
                    self.total_sum += valor
                    cliente['last_req'] = id_req
                    cliente['last_total_sum'] = self.total_sum
                    log_servidor(f"client {addr[0]} id_req {id_req} value {valor} num_reqs {self.num_reqs} total_sum {self.total_sum}")
                elif id_req <= cliente['last_req']:
                    log_servidor(f"client {addr[0]} DUP!! id_req {id_req} value {valor} num_reqs {self.num_reqs} total_sum {self.total_sum}")

                self.sock.sendto(empacotar(TIPO_ACK, cliente['last_req'], self.num_reqs, self.total_sum), addr)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 servidor.py <porta>")
        sys.exit(1)

    lider = False
    if int(sys.argv[1]) == 12345:
        lider = True
        print("Eu sou o líder!")

    s = ServidorSoma(int(sys.argv[1]),lider)
    s.iniciar()
