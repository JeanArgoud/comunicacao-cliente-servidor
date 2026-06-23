import socket
import sys
import threading
import time
from datetime import datetime
from comum import *

def log_cliente(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}")

class ClienteSoma:
    def __init__(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(1.5)
        self.id_atual = 1
        self.porta_lider_atual = None

    def localizar_lider_na_rede(self):
        """ Manda broadcast na porta 9999 para descobrir quem é o líder ativo """
        log_cliente("Procurando o líder do sistema via Broadcast...")
        pacote = empacotar(TIPO_DESCOBERTA, 0, 0, 0, 0)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        
        while not self.porta_lider_atual:
            try:
                self.sock.sendto(pacote, ('255.255.255.255', 9999))
                data, addr = self.sock.recvfrom(1024)
                tipo, _, _, porta_real_lider, _ = desempacotar(data)
                
                if tipo == TIPO_ACK:
                    self.porta_lider_atual = porta_real_lider
                    self.ip_lider_atual = addr[0] 
                    log_cliente(f"Líder localizado em {self.ip_lider_atual}:{self.porta_lider_atual}")
                    return
            except OSError:
                time.sleep(1)
                continue

    def enviar_com_confirmacao(self, valor):
        while True:
            if not self.porta_lider_atual:
                self.localizar_lider_na_rede()
                
            pacote = empacotar(TIPO_REQUISICAO, self.id_atual, self.id_atual, valor, 0)
            try:
                self.sock.sendto(pacote, (self.ip_lider_atual, self.porta_lider_atual))
                data, addr = self.sock.recvfrom(1024)
                _, id_ack, num_reqs, _, soma_total = desempacotar(data)

                if id_ack == self.id_atual:
                    log_cliente(f"Confirmado pelo líder! num_reqs: {num_reqs} | total_sum: {soma_total}")
                    self.id_atual += 1
                    break
            except OSError:
                log_cliente("O líder atual parou de responder. Resetando rota...")
                self.porta_lider_atual = None  
                time.sleep(0.5)
                continue

def thread_leitura(cliente):
    print("Digite os números inteiros que deseja somar:")
    while True:
        try:
            linha = input()
            if not linha.strip():
                continue
            valor = int(linha)
            cliente.enviar_com_confirmacao(valor)
        except ValueError:
            print("Digite apenas números inteiros.")
        except (KeyboardInterrupt, EOFError):
            break

if __name__ == "__main__":
    c = ClienteSoma()
    
    t = threading.Thread(target=thread_leitura, args=(c,), daemon=True)
    t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nCliente encerrado.")