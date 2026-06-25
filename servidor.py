# servidor.py
import socket
import sys
import threading
import time
from datetime import datetime
from comum import *


def log_servidor(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def get_ip_local():

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except OSError:
        return '127.0.0.1'


class ServidorSoma:
    def __init__(self, id_servidor):
        self.id = id_servidor
        self.ip_local = get_ip_local()

        self.sock = None
        self.lider = False
        self.eleicao_ocorrendo = False
        self.total_sum = 0
        self.num_reqs = 0

        self.clientes = {}
        self.lock = threading.Lock()

        self.ultimo_heartbeat = time.time()
        self.fui_barrado = False

        self.sock_desc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_desc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            self.sock_desc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except AttributeError:
            pass
        self.sock_desc.bind(('0.0.0.0', PORTA_INTERNA))
        log_servidor(
            f"Servidor [ID: {self.id}] | IP local: {self.ip_local} | "
            f"Canal interno: {PORTA_INTERNA} | Serviço: {PORTA_SERVICO}"
        )



    def _snapshot(self):
        with self.lock:
            return self.num_reqs, self.total_sum

    def enviar_notificacao(self, tipo):
        num_reqs, total_sum = self._snapshot()
        ip_int = ip_para_int(self.ip_local)
        pacote = empacotar(tipo, self.id, num_reqs, ip_int, total_sum)
        sock_envio = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock_envio.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        try:
            sock_envio.sendto(pacote, ('127.0.0.1', PORTA_INTERNA))
            sock_envio.sendto(pacote, ('255.255.255.255', PORTA_INTERNA))
        except OSError:
            pass
        finally:
            sock_envio.close()

    def enviar_ok_para(self, ip_destino):
        """Envia TIPO_OK diretamente para o IP do remetente da eleição."""
        ip_int = ip_para_int(self.ip_local)
        pacote = empacotar(TIPO_OK, self.id, 0, ip_int, 0)
        sock_envio = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock_envio.sendto(pacote, (ip_destino, PORTA_INTERNA))
        except OSError:
            pass
        finally:
            sock_envio.close()


    def iniciar_eleicao(self):
        with self.lock:
            if self.eleicao_ocorrendo:
                return
            self.eleicao_ocorrendo = True
            self.fui_barrado = False

        log_servidor("Valentão: Detectou falha no líder. Convocando eleição...")
        self.enviar_notificacao(TIPO_ELEICAO)
        time.sleep(0.6)

        with self.lock:
            ainda_candidato = self.eleicao_ocorrendo and not self.fui_barrado

        if ainda_candidato:
            self.tornar_se_coordenador()
        else:
            with self.lock:
                self.eleicao_ocorrendo = False

    def tornar_se_coordenador(self):
        with self.lock:
            total_recuperado = self.total_sum
            self.lider = True
            self.eleicao_ocorrendo = False

        log_servidor(f"VALENTÃO: Assumi o controle. IP: {self.ip_local}. Soma: {total_recuperado}")
        threading.Thread(target=self.tentar_assumir_porta, daemon=True).start()
        self.enviar_notificacao(TIPO_COORDENADOR)

    def tentar_assumir_porta(self):
        while True:
            with self.lock:
                if not self.lider:
                    return
            novo_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            novo_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                novo_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except AttributeError:
                pass
            try:
                novo_sock.bind(('0.0.0.0', PORTA_SERVICO))
                with self.lock:
                    self.sock = novo_sock
                log_servidor(f"Porta {PORTA_SERVICO} capturada. Atendendo clientes em {self.ip_local}:{PORTA_SERVICO}")
                threading.Thread(target=self.escutar_cliente, args=(novo_sock,), daemon=True).start()
                return
            except OSError:
                novo_sock.close()
                time.sleep(0.05)


    def enviar_heartbeat_periodico(self):
        while True:
            with self.lock:
                eh_lider = self.lider
            if eh_lider:
                self.enviar_notificacao(TIPO_REPLICACAO)
                self.enviar_notificacao(TIPO_COORDENADOR)
            time.sleep(0.3)

    def checar_queda_do_lider(self):
        while True:
            with self.lock:
                eh_lider = self.lider
                eleicao = self.eleicao_ocorrendo
                ultimo = self.ultimo_heartbeat
            if not eh_lider and not eleicao:
                if time.time() - ultimo > 1.2:
                    threading.Thread(target=self.iniciar_eleicao, daemon=True).start()
            time.sleep(0.2)


    def tratar_requisicao(self, data, addr, socket_atendimento):
        tipo, id_req, num_reqs, valor, total_sum = desempacotar(data)

        resposta = None
        replicar = False
        with self.lock:
            if not self.lider:
                return

            if addr not in self.clientes:
                self.clientes[addr] = {'last_req': id_req - 1, 'total_na_epoca': 0}

            cliente = self.clientes[addr]

            if id_req == cliente['last_req'] + 1:
                self.num_reqs += 1
                self.total_sum += valor
                cliente['last_req'] = id_req
                cliente['total_na_epoca'] = self.total_sum
                log_servidor(f"PROCESSADO | ID Req: {id_req} | +{valor} | Total: {self.total_sum}")
                replicar = True
                resposta = empacotar(TIPO_ACK, id_req, self.num_reqs, 0, self.total_sum)

            elif id_req <= cliente['last_req']:
                log_servidor(f"Idempotência: duplicada ID {id_req}. Reenviando ACK.")
                resposta = empacotar(TIPO_ACK, id_req, self.num_reqs, 0, self.total_sum)

            else:
                log_servidor(f"Aviso: fora de ordem ID {id_req} (esperado {cliente['last_req'] + 1}). Descartando.")
                return

        if resposta:
            try:
                socket_atendimento.sendto(resposta, addr)
            except OSError:
                pass
        if replicar:
            self.enviar_notificacao(TIPO_REPLICACAO)

    def escutar_cliente(self, socket_atendimento):
        socket_atendimento.settimeout(0.1)
        while True:
            with self.lock:
                if not self.lider:
                    break
            try:
                data, addr = socket_atendimento.recvfrom(1024)
                tipo, _, _, _, _ = desempacotar(data)
                if tipo == TIPO_REQUISICAO:
                    threading.Thread(
                        target=self.tratar_requisicao,
                        args=(data, addr, socket_atendimento),
                        daemon=True,
                    ).start()
            except socket.timeout:
                continue
            except OSError:
                break


    def escutar_canal_descoberta(self):
        while True:
            try:
                data, addr = self.sock_desc.recvfrom(1024)
            except OSError as e:
                log_servidor(f"Aviso canal interno: {e}. Continuando...")
                time.sleep(0.05)
                continue

            try:
                tipo, id_remetente, num_reqs, ip_int_remetente, total_sum = desempacotar(data)
            except Exception:
                continue

            if id_remetente == self.id:
                continue


            ip_remetente = int_para_ip(ip_int_remetente) if ip_int_remetente else addr[0]
            if ip_remetente in ('0.0.0.0', ''):
                ip_remetente = addr[0]

            if tipo == TIPO_ELEICAO and self.id > id_remetente:
                self.enviar_ok_para(ip_remetente)
                threading.Thread(target=self.iniciar_eleicao, daemon=True).start()

            elif tipo == TIPO_OK and id_remetente > self.id:
                with self.lock:
                    self.fui_barrado = True
                    self.eleicao_ocorrendo = False

            elif tipo == TIPO_COORDENADOR:
                with self.lock:
                    self.lider = False
                    self.eleicao_ocorrendo = False
                    self.num_reqs = num_reqs
                    self.total_sum = total_sum
                    if self.sock:
                        self.sock.close()
                        self.sock = None
                self.ultimo_heartbeat = time.time()
                log_servidor(
                    f"Novo coordenador (ID {id_remetente}, IP {ip_remetente}) eleito. "
                    f"soma={total_sum}"
                )

            elif tipo == TIPO_REPLICACAO:
                self.ultimo_heartbeat = time.time()
                with self.lock:
                    if not self.lider:
                        if self.total_sum != total_sum:
                            log_servidor(f"Sync: {self.total_sum} -> {total_sum}")
                        self.num_reqs = num_reqs
                        self.total_sum = total_sum


    def iniciar(self):
        threading.Thread(target=self.escutar_canal_descoberta, daemon=True).start()
        time.sleep(0.2)
        self.enviar_notificacao(TIPO_NOVO_SERVIDOR)
        threading.Thread(target=self.iniciar_eleicao, daemon=True).start()
        threading.Thread(target=self.enviar_heartbeat_periodico, daemon=True).start()
        threading.Thread(target=self.checar_queda_do_lider, daemon=True).start()
        while True:
            time.sleep(1)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Uso: python3 {sys.argv[0]} <id_servidor>")
        print(f"  Exemplo: python3 {sys.argv[0]} 1")
        print(f"  O servidor com maior ID vence a eleição (Valentão)")
        print(f"  Clientes conectam na porta fixa {PORTA_SERVICO} no IP do líder")
        sys.exit(1)
    s = ServidorSoma(int(sys.argv[1]))
    s.iniciar()
