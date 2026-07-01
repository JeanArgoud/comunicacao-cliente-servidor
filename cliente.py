import socket
import sys
import threading
from datetime import datetime
from comum import *

def log_cliente(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}")

class ClienteSoma:
    def __init__(self, porta):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.bind(('', porta))
        self.sock.settimeout(0.01) # 10ms conforme especificação
        self.servidor_addr = None
        self.id_atual = 1

    def descobrir_servidor(self):
        pacote = empacotar(TIPO_DESCOBERTA, 0, 0, 0)
        while not self.servidor_addr:
            try:
                self.sock.sendto(pacote, ('<broadcast>', PORTA_DESCOBERTA_SERVIDORES))
                data, addr = self.sock.recvfrom(TAMANHO_BUFFER)
                if data[0] == TIPO_ACK:
                    self.servidor_addr = addr
                    #log_cliente(f"server addr {addr[0]}:{addr[1]}")
            except socket.timeout:
                continue

    def enviar_com_confirmacao(self, valor):
        pacote = empacotar(TIPO_REQUISICAO, self.id_atual, self.id_atual, valor)
        precisa_enviar = True
        while True:
            try:
                if precisa_enviar:
                    self.sock.sendto(pacote, self.servidor_addr)
                precisa_enviar = True
                data, _ = self.sock.recvfrom(TAMANHO_BUFFER)

                if data[0] == TIPO_REDIRECIONAMENTO:
                    novo_ip, nova_porta = desempacotar_redirecionamento(data)
                    #log_cliente(f"redirecionado para {novo_ip}:{nova_porta}")
                    self.servidor_addr = (novo_ip, nova_porta)
                    # Drena erros ICMP pendentes do servidor antigo (comportamento Windows)
                    self.sock.settimeout(0)
                    while True:
                        try:
                            self.sock.recvfrom(TAMANHO_BUFFER)
                        except (BlockingIOError, ConnectionResetError):
                            break
                    self.sock.settimeout(0.01)
                    continue

                _, id_ack, num_reqs, soma_total = desempacotar(data)
                if id_ack == self.id_atual:
                    log_cliente(f"id_req {self.id_atual} value {valor} num_reqs {num_reqs} total_sum {soma_total}")
                    self.id_atual += 1
                    break
                else:
                    self.id_atual = id_ack + 1  
                    pacote = empacotar(TIPO_REQUISICAO, self.id_atual, self.id_atual, valor)
            except socket.timeout:
                continue
            except ConnectionResetError:
                precisa_enviar = False  # Aguarda redirect no buffer antes de reenviar

def thread_leitura(cliente):
    while True:
        try:
            linha = sys.stdin.readline()
            if not linha: break
            valor = int(linha.strip())
            cliente.enviar_com_confirmacao(valor)
        except (EOFError, KeyboardInterrupt, ValueError):
            break

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 cliente.py <porta>")
        sys.exit(1)
    c = ClienteSoma(int(sys.argv[1]))
    c.descobrir_servidor()
    t = threading.Thread(target=thread_leitura, args=(c,), daemon=True)
    t.start()
    t.join()
