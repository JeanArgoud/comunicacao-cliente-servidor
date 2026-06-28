import socket
import sys
import time
import threading
from datetime import datetime
from comum import *

HEARTBEAT_INTERVALO = 1    # segundos entre cada heartbeat enviado pelo líder
HEARTBEAT_TIMEOUT   = 3    # segundos sem heartbeat para considerar líder offline
TIMEOUT_ACK_ELEICAO = 2    # segundos aguardando ACK de processo com porta maior

def log_servidor(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}")

class ServidorSoma:
    def __init__(self, porta, lider):
        self.porta = porta
        self.meu_ip = get_meu_ip()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('', porta))
        self.total_sum = 0
        self.num_reqs = 0
        self.clientes = {}
        self.servidores = []
        self.ultimo_heartbeat = time.time()
        self.em_eleicao = False
        self.ack_eleicao = threading.Event()
        self.lider = (self.meu_ip, porta) if lider else None

    def _sou_lider(self):
        return self.lider == (self.meu_ip, self.porta)

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
                self.lider = (addr[0], addr[1])
                log_servidor(f"servidor {addr[0]}:{addr[1]} encontrado")
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
        while self._sou_lider():
            time.sleep(HEARTBEAT_INTERVALO)
            for servidor in self.servidores:
                self.sock.sendto(pacote, servidor)

    def _iniciar_eleicao(self):
        self.em_eleicao = True
        self.ack_eleicao.clear()
        log_servidor(f"iniciando eleicao (porta {self.porta})")

        superiores = [s for s in self.servidores if s[1] > self.porta]
        for s in superiores:
            self.sock.sendto(empacotar_eleicao(self.porta), s)

        if not superiores:
            self._tornarse_lider()
            return

        recebeu_ack = self.ack_eleicao.wait(timeout=TIMEOUT_ACK_ELEICAO)
        if not recebeu_ack:
            self._tornarse_lider()

    def _tornarse_lider(self):
        lider_antigo = self.lider
        self.lider = (self.meu_ip, self.porta)
        self.em_eleicao = False
        self.servidores = [s for s in self.servidores if s != lider_antigo]
        log_servidor(f"sou o novo lider ({self.meu_ip}:{self.porta})")

        pacote = empacotar_coordenador(self.meu_ip, self.porta)
        for servidor in self.servidores:
            self.sock.sendto(pacote, servidor)

        pacote_redirect = empacotar_redirecionamento(self.meu_ip, self.porta)
        for cliente_addr in self.clientes:
            self.sock.sendto(pacote_redirect, cliente_addr)

        threading.Thread(target=self._thread_heartbeat, daemon=True).start()

    def _thread_watchdog(self):
        while True:
            time.sleep(HEARTBEAT_INTERVALO)
            if not self._sou_lider() and time.time() - self.ultimo_heartbeat > HEARTBEAT_TIMEOUT:
                if not self.em_eleicao:
                    log_servidor("lider offline detectado")
                    for (ip, porta), estado in self.clientes.items():
                        log_servidor(f"  cliente {ip}:{porta} last_req {estado['last_req']}")
                    self._iniciar_eleicao()

    def iniciar(self):
        if not self._sou_lider():
            self.sincronizar_servidores()
        else:
            threading.Thread(target=self._thread_heartbeat, daemon=True).start()

        threading.Thread(target=self._thread_watchdog, daemon=True).start()

        log_servidor(f"num_reqs {self.num_reqs} total_sum {self.total_sum}")
        while True:
            try:
                data, addr = self.sock.recvfrom(TAMANHO_BUFFER)
            except ConnectionResetError:
                continue  # Windows: ICMP port unreachable do destinatário que fechou
            tipo = data[0]

            if tipo == TIPO_DESCOBERTA:
                if not self._sou_lider(): continue
                if addr not in self.clientes:
                    self.clientes[addr] = {'last_req': 0}
                self.sock.sendto(empacotar(TIPO_ACK, 0, 0, 0), addr)

            elif tipo == TIPO_NOVO_SERVIDOR:
                if not self._sou_lider(): continue
                if addr not in self.servidores:
                    self.servidores.append(addr)
                    log_servidor(f"servidor {addr[0]}:{addr[1]} registrado")
                else:
                    log_servidor(f"servidor {addr[0]}:{addr[1]} foi religado")
                clientes = [(c_addr, estado['last_req']) for c_addr, estado in self.clientes.items()]
                self.sock.sendto(empacotar_replicacao(self.num_reqs, self.total_sum, clientes), addr)

            elif tipo == TIPO_REPLICACAO:
                self.receber_replicacao(data)

            elif tipo == TIPO_HEARTBEAT:
                self.ultimo_heartbeat = time.time()

            elif tipo == TIPO_ELEICAO:
                porta_candidato = desempacotar_eleicao(data)
                if porta_candidato < self.porta:
                    self.sock.sendto(empacotar_ack_eleicao(), addr)
                    if not self.em_eleicao:
                        threading.Thread(target=self._iniciar_eleicao, daemon=True).start()

            elif tipo == TIPO_ACK_ELEICAO:
                self.ack_eleicao.set()

            elif tipo == TIPO_COORDENADOR:
                novo_ip, nova_porta = desempacotar_coordenador(data)
                self.lider = (novo_ip, nova_porta)
                self.em_eleicao = False
                self.ultimo_heartbeat = time.time()
                log_servidor(f"novo lider: {novo_ip}:{nova_porta}")

            elif tipo == TIPO_REQUISICAO:
                if not self._sou_lider(): continue
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
