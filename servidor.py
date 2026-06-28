import socket
import sys
import time
import threading
from datetime import datetime
from comum import *

HEARTBEAT_INTERVALO = 1    # segundos entre cada heartbeat enviado pelo líder
HEARTBEAT_TIMEOUT   = 3    # segundos sem heartbeat para considerar líder offline

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
        self.servidores = []
        self.ultimo_heartbeat = time.time()

    def sincronizar_servidores(self):
        pacote = empacotar(TIPO_NOVO_SERVIDOR, 0, 0, 0)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.settimeout(2)
        self.sock.sendto(pacote, ('<broadcast>', PORTA_LIDER_INICIAL))
        try:
            data, addr = self.sock.recvfrom(TAMANHO_BUFFER)
            if data[0] == TIPO_REPLICACAO:
                self.num_reqs, self.total_sum, clientes = desempacotar_replicacao(data)
                for c_addr, last_req in clientes:
                    self.clientes[c_addr] = {'last_req': last_req}
                self.servidores.append(addr)
                log_servidor(f"servidor {addr[0]} encontrado")
            else:
                log_servidor(f"servidor {addr[0]} enviou um pacote errado")
        except socket.timeout:
            log_servidor("servidor nao encontrado")
            sys.exit(1)
        self.sock.settimeout(None)

    def replicar_dados(self):
        clientes = [(addr, estado['last_req']) for addr, estado in self.clientes.items()]
        pacote = empacotar_replicacao(self.num_reqs, self.total_sum, clientes)
        for servidor in self.servidores:
            self.sock.sendto(pacote, servidor)

    def receber_replicacao(self, data):
        num_reqs, total_sum, clientes = desempacotar_replicacao(data)
        self.num_reqs = num_reqs
        self.total_sum = total_sum
        for c_addr, last_req in clientes:
            if c_addr not in self.clientes:
                self.clientes[c_addr] = {'last_req': last_req}
            else:
                self.clientes[c_addr]['last_req'] = last_req
        self.ultimo_heartbeat = time.time()
        log_servidor(f"replicacao recebida num_reqs {self.num_reqs} total_sum {self.total_sum}")

    def _thread_heartbeat(self):
        pacote = empacotar(TIPO_HEARTBEAT, 0, 0, 0)
        while True:
            time.sleep(HEARTBEAT_INTERVALO)
            for servidor in self.servidores:
                self.sock.sendto(pacote, servidor)

    def _thread_watchdog(self):
        while True:
            time.sleep(HEARTBEAT_INTERVALO)
            if time.time() - self.ultimo_heartbeat > HEARTBEAT_TIMEOUT:
                log_servidor("lider offline detectado — eleicao pendente de implementacao")
                for (ip, porta), estado in self.clientes.items():
                    log_servidor(f"  cliente {ip}:{porta} last_req {estado['last_req']}")

    def iniciar(self):
        if not self.lider:
            self.sincronizar_servidores()
            threading.Thread(target=self._thread_watchdog, daemon=True).start()
        else:
            threading.Thread(target=self._thread_heartbeat, daemon=True).start()

        log_servidor(f"num_reqs {self.num_reqs} total_sum {self.total_sum}")
        while True:
            data, addr = self.sock.recvfrom(TAMANHO_BUFFER)
            tipo = data[0]

            if tipo == TIPO_DESCOBERTA:
                if addr not in self.clientes:
                    self.clientes[addr] = {'last_req': 0}
                self.sock.sendto(empacotar(TIPO_ACK, 0, 0, 0), addr)

            elif tipo == TIPO_NOVO_SERVIDOR:
                if addr not in self.servidores:
                    self.servidores.append(addr)
                    log_servidor(f"servidor {addr[0]} registrado")
                else:
                    log_servidor(f"servidor {addr[0]} foi religado")
                self.sock.sendto(empacotar_replicacao(self.num_reqs, self.total_sum, list(self.clientes.keys())), addr)

            elif tipo == TIPO_REPLICACAO:
                self.receber_replicacao(data)

            elif tipo == TIPO_HEARTBEAT:
                self.ultimo_heartbeat = time.time()

            elif tipo == TIPO_REQUISICAO:
                _, id_req, _, valor = desempacotar(data)
                cliente = self.clientes.get(addr)
                if not cliente: continue

                if id_req == cliente['last_req'] + 1:
                    self.num_reqs += 1
                    self.total_sum += valor
                    cliente['last_req'] = id_req
                    log_servidor(f"client {addr[0]} id_req {id_req} value {valor} num_reqs {self.num_reqs} total_sum {self.total_sum}")
                elif id_req <= cliente['last_req']:
                    log_servidor(f"client {addr[0]} DUP!! id_req {id_req} value {valor} num_reqs {self.num_reqs} total_sum {self.total_sum}")

                self.replicar_dados()
                self.sock.sendto(empacotar(TIPO_ACK, cliente['last_req'], self.num_reqs, self.total_sum), addr)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 servidor.py <porta>")
        sys.exit(1)

    lider = int(sys.argv[1]) == PORTA_LIDER_INICIAL
    if lider:
        print("Eu sou o líder!")

    s = ServidorSoma(int(sys.argv[1]), lider)
    s.iniciar()
