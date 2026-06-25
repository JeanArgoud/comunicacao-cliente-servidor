# cliente.py
import socket
import sys
import threading
import time
from datetime import datetime
from comum import *


class ClienteSoma:
    def __init__(self, ip_lider_inicial):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('', 0))
        self.sock.settimeout(0.15)

        self.id_atual = 1
        self.confirmado = threading.Event()
        self.total_sum = 0
        self.num_reqs = 0
        self._ativo = True

        self.ip_lider = ip_lider_inicial
        self._lock_ip = threading.Lock()

        threading.Thread(target=self._escutar_respostas, daemon=True).start()
        threading.Thread(target=self._escutar_coordenador, daemon=True).start()

    def _escutar_respostas(self):
        """Recebe ACKs do líder atual."""
        while self._ativo:
            try:
                data, _ = self.sock.recvfrom(1024)
                tipo, id_ack, num_reqs, _, soma_total = desempacotar(data)
                if tipo == TIPO_ACK and id_ack == self.id_atual:
                    self.total_sum = soma_total
                    self.num_reqs = num_reqs
                    self.confirmado.set()
            except socket.timeout:
                continue
            except OSError as e:
                if self._ativo:
                    print(f"[ERRO] Socket de recepção: {e}", flush=True)
                break

    def _escutar_coordenador(self):
        sock_bc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock_bc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock_bc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass
        sock_bc.bind(('0.0.0.0', PORTA_INTERNA))
        sock_bc.settimeout(1.0)

        while self._ativo:
            try:
                data, _ = sock_bc.recvfrom(1024)
                tipo, id_remetente, _, ip_int, _ = desempacotar(data)
                if tipo == TIPO_COORDENADOR and ip_int:
                    novo_ip = int_para_ip(ip_int)
                    with self._lock_ip:
                        if novo_ip != self.ip_lider and novo_ip not in ('0.0.0.0', ''):
                            print(
                                f"\n[{datetime.now().strftime('%H:%M:%S')}] "
                                f"Novo líder detectado: {novo_ip} (ID {id_remetente}). "
                                f"Redirecionando automaticamente.",
                                flush=True,
                            )
                            self.ip_lider = novo_ip
            except socket.timeout:
                continue
            except OSError:
                break
        sock_bc.close()

    def enviar_com_confirmacao(self, valor):
        """
        Envia para PORTA_SERVICO no IP do líder atual.
        Se o líder mudar, _escutar_coordenador atualiza self.ip_lider
        e o próximo reenvio já vai para o novo líder.
        """
        self.confirmado.clear()
        pacote = empacotar(TIPO_REQUISICAO, self.id_atual, self.id_atual, valor, 0)

        while not self.confirmado.is_set():
            with self._lock_ip:
                destino = (self.ip_lider, PORTA_SERVICO)
            try:
                self.sock.sendto(pacote, destino)
            except OSError as e:
                print(f"[AVISO] Erro ao enviar: {e}", flush=True)
            time.sleep(0.15)

        print(
            f"[{datetime.now().strftime('%H:%M:%S')}] "
            f"Confirmado! total_sum={self.total_sum}",
            flush=True,
        )
        self.id_atual += 1


def thread_leitura(cliente):
    print(
        f"Cliente conectado ao líder em {cliente.ip_lider}:{PORTA_SERVICO}\n"
        f"(O IP é atualizado automaticamente se o líder mudar)\n"
        "Digite os números inteiros que deseja somar:",
        flush=True,
    )
    acumulado_local = 0
    linhas_processadas = 0
    TAMANHO_LOTE = 1000
    while True:
        try:
            linha = input()
            if not linha.strip():
                continue
            acumulado_local += int(linha)
            linhas_processadas += 1
            
           
            if linhas_processadas % TAMANHO_LOTE == 0:
                cliente.enviar_com_confirmacao(acumulado_local)
                acumulado_local = 0
        except ValueError:
            print("Digite apenas números inteiros.", flush=True)
            acumulado_local += int(linha)
            linhas_processadas += 1
            
            
            if linhas_processadas % TAMANHO_LOTE == 0:
                cliente.enviar_com_confirmacao(acumulado_local)
                acumulado_local = 0
            break
        except (KeyboardInterrupt, EOFError):
            break


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Uso: python3 {sys.argv[0]} <ip_do_lider>")
        print(f"  Exemplo (mesma máquina): python3 {sys.argv[0]} 127.0.0.1")
        print(f"  Exemplo (rede local):    python3 {sys.argv[0]} 192.168.1.10")
        print(f"  Se o líder mudar de máquina, o cliente redireciona automaticamente.")
        sys.exit(1)
    c = ClienteSoma(sys.argv[1])
    t = threading.Thread(target=thread_leitura, args=(c,), daemon=True)
    t.start()
    while True:
        time.sleep(1)
