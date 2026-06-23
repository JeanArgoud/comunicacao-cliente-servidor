import socket
import sys
import threading
import time
from datetime import datetime
from comum import *

def log_servidor(msg):
    print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {msg}")

class ServidorSoma:
    def __init__(self, id_processo):
        self.id = id_processo 
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('', 0)) 
        self.porta = self.sock.getsockname()[1]
        
        self.sock_desc = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_desc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock_desc.bind(('', 9999))

        log_servidor(f"Servidor iniciado | ID: {self.id} | Porta Real de Operação: {self.porta}")
        
        self.lider = False 
        self.eleicao_ocorrendo = False
        self.total_sum = 0
        self.num_reqs = 0
        self.clientes = {}
        self.lock = threading.Lock() 
        self.ultimo_heartbeat = time.time()
        self.fui_barrado = False  

    def enviar_broadcast(self, tipo):
        """ Propaga dados de controle para toda a rede na porta 9999 """
        pacote = empacotar(tipo, self.id, self.num_reqs, self.porta, self.total_sum)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        try:
            self.sock.sendto(pacote, ('255.255.255.255', 9999))
        except OSError:
            pass

    def iniciar_eleicao(self):
        if self.eleicao_ocorrendo:
            return
            
        self.eleicao_ocorrendo = True
        self.fui_barrado = False
        log_servidor("Valentão: Convocando eleição na rede...")
       
        self.enviar_broadcast(TIPO_ELEICAO)

        time.sleep(1.2)
        
        if self.eleicao_ocorrendo and not self.fui_barrado:
            self.tornar_se_coordenador()
        else:
            log_servidor("Eleição finalizada. Outro nó possui maior prioridade.")
            self.eleicao_ocorrendo = False

    def tornar_se_coordenador(self):
        log_servidor("VALENTÃO: Eu venci a eleição! Notificando a rede como NOVO LÍDER.")
        with self.lock:
            self.lider = True
            self.eleicao_ocorrendo = False
        self.enviar_broadcast(TIPO_COORDENADOR)

    def enviar_heartbeat_periodico(self):
        while True:
            if self.lider:
                self.enviar_broadcast(TIPO_REPLICACAO)
            time.sleep(1)

    def checar_queda_do_lider(self):
        while True:
            if not self.lider and not self.eleicao_ocorrendo:
                if time.time() - self.ultimo_heartbeat > 4.0:
                    log_servidor("Ausência do líder detectada por timeout. Iniciando eleição...")
                    threading.Thread(target=self.iniciar_eleicao, daemon=True).start()
            time.sleep(1)

    def tratar_requisicao(self, data, addr):
        tipo, id_req, num_reqs, valor, total_sum = desempacotar(data)

        with self.lock:
            if addr not in self.clientes:
                self.clientes[addr] = {'last_req': id_req - 1}

            cliente = self.clientes[addr]
            
            if id_req == cliente['last_req'] + 1:
                self.num_reqs += 1
                self.total_sum += valor
                cliente['last_req'] = id_req
                log_servidor(f"Requisição PROCESSADA | ID Cliente: {id_req} | Valor: {valor} | Total Acumulado: {self.total_sum}")
                self.enviar_broadcast(TIPO_REPLICACAO)
            
            try:
                self.sock.sendto(empacotar(TIPO_ACK, cliente['last_req'], self.num_reqs, 0, self.total_sum), addr)
            except OSError:
                pass

    def escutar_canal_descoberta(self):
        """ Thread persistente: Centraliza TODA a comunicação inter-servidores """
        while True:
            try:
                data, addr = self.sock_desc.recvfrom(1024)
                tipo, id_remetente, num_reqs, porta_real, total_sum = desempacotar(data)
                
                if id_remetente == self.id:
                    continue  

                if tipo == TIPO_DESCOBERTA and self.lider:
                    
                    self.sock.sendto(empacotar(TIPO_ACK, 0, 0, self.porta, 0), addr)

                elif tipo == TIPO_NOVO_SERVIDOR and self.lider:
                    
                    log_servidor(f"Nó {id_remetente} entrou na rede. Enviando dados atualizados ({self.total_sum}).")
                    self.enviar_broadcast(TIPO_REPLICACAO)

                if tipo == TIPO_ELEICAO:
                    
                    if id_remetente > self.id:
                        log_servidor(f"Nó {id_remetente} tentou eleição. Enviando TIPO_OK (Meu ID {self.id} manda).")
                        pacote_ok = empacotar(TIPO_OK, self.id, 0, 0, 0)
                        
                        self.sock_desc.sendto(pacote_ok, (addr[0], 9999)) 
                        
                        threading.Thread(target=self.iniciar_eleicao, daemon=True).start()
                elif tipo == TIPO_OK:
                    
                    if id_remetente < self.id:
                        log_servidor(f"Fui barrado na eleição pelo nó prioritário {id_remetente}.")
                        self.fui_barrado = True
                        self.eleicao_ocorrendo = False

                elif tipo == TIPO_COORDENADOR:
                    log_servidor(f"Novo Coordenador assumiu: ID {id_remetente} na porta {porta_real}")
                    with self.lock:
                        self.lider = False
                        self.eleicao_ocorrendo = False
                        self.num_reqs = num_reqs
                        self.total_sum = total_sum
                    self.ultimo_heartbeat = time.time()

                elif tipo == TIPO_REPLICACAO:
                    
                    self.ultimo_heartbeat = time.time()
                    with self.lock:
                        self.num_reqs = num_reqs
                        self.total_sum = total_sum
                    log_servidor(f"Backup Atualizado -> num_reqs: {self.num_reqs} | total_sum: {self.total_sum}")

            except OSError:
                break

    def iniciar(self):
        
        threading.Thread(target=self.escutar_canal_descoberta, daemon=True).start()
        time.sleep(0.1) 

        log_servidor("Anunciando presença e iniciando checagem do Valentão...")
        self.enviar_broadcast(TIPO_NOVO_SERVIDOR)
        threading.Thread(target=self.iniciar_eleicao, daemon=True).start()

        threading.Thread(target=self.enviar_heartbeat_periodico, daemon=True).start()
        threading.Thread(target=self.checar_queda_do_lider, daemon=True).start()
        
        while True:
            try:
                data, addr = self.sock.recvfrom(1024)
                tipo, _, _, _, _ = desempacotar(data)
                
                if tipo == TIPO_REQUISICAO and self.lider:
                    threading.Thread(target=self.tratar_requisicao, args=(data, addr), daemon=True).start()
            except OSError:
                continue

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python servidor.py <ID_UNICO_INTEIRO>")
        sys.exit(1)

    id_input = int(sys.argv[1])
    s = ServidorSoma(id_input)
    s.iniciar()