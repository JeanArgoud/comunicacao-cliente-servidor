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


def thread_leitura_interativo(cliente):
    """Modo interativo: envia cada número imediatamente ao pressionar Enter."""
    print(
        f"Cliente conectado ao líder em {cliente.ip_lider}:{PORTA_SERVICO}\n"
        f"(O IP é atualizado automaticamente se o líder mudar)\n"
        "Modo interativo — digite um número por linha e pressione Enter para enviar.\n"
        "Pressione Ctrl+C ou Ctrl+D para sair.",
        flush=True,
    )
    while True:
        try:
            linha = input("> ")
            if not linha.strip():
                continue
            valor = int(linha)
            cliente.enviar_com_confirmacao(valor)
        except ValueError:
            print("Entrada inválida — digite apenas números inteiros.", flush=True)
        except (KeyboardInterrupt, EOFError):
            print("\nEncerrando.", flush=True)
            break


def _ler_stream(cliente, stream, nome, tamanho_lote):

    acumulado = 0
    lidos = 0
    erros = 0
    try:
        for linha in stream:
            linha = linha.strip()
            if not linha:
                continue
            try:
                acumulado += int(linha)
                lidos += 1
                if lidos % tamanho_lote == 0:
                    cliente.enviar_com_confirmacao(acumulado)
                    acumulado = 0
            except ValueError:
                erros += 1
                print(f"[AVISO] Linha ignorada (não é inteiro): '{linha}'", flush=True)
    except (KeyboardInterrupt, EOFError):
        print(f"\nInterrompido durante '{nome}'. Encerrando.", flush=True)
        if acumulado != 0:
            cliente.enviar_com_confirmacao(acumulado)
        raise

    if acumulado != 0:
        cliente.enviar_com_confirmacao(acumulado)

    print(
        f"'{nome}' concluído. {lidos} números lidos, {erros} linhas ignoradas.\n"
        f"Total no servidor até agora: {cliente.total_sum}",
        flush=True,
    )


def thread_leitura_arquivo(cliente, caminho, tamanho_lote=1000):
    """Modo arquivo: abre o arquivo e delega para _ler_stream."""
    print(
        f"Cliente conectado ao líder em {cliente.ip_lider}:{PORTA_SERVICO}\n"
        f"Modo arquivo — lendo '{caminho}' em lotes de {tamanho_lote}.",
        flush=True,
    )
    try:
        with open(caminho, 'r') as f:
            _ler_stream(cliente, f, caminho, tamanho_lote)
    except FileNotFoundError:
        print(f"[ERRO] Arquivo não encontrado: '{caminho}'", flush=True)

    print("Entrando no modo interativo — Ctrl+C ou Ctrl+D para sair.", flush=True)


def thread_leitura_stdin(cliente, tamanho_lote=1000):

    print(
        f"Cliente conectado ao líder em {cliente.ip_lider}:{PORTA_SERVICO}\n"
        f"Modo stdin — lendo entrada redirecionada em lotes de {tamanho_lote}.",
        flush=True,
    )
    _ler_stream(cliente, sys.stdin, "stdin", tamanho_lote)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(
        description="Cliente de soma distribuída.",
        epilog=(
            "Exemplos:\n"
            "  python3 cliente.py 127.0.0.1                        # interativo\n"
            "  python3 cliente.py 127.0.0.1 < numeros.txt          # stdin, lote 1000\n"
            "  python3 cliente.py 127.0.0.1 --lote 5000 < arq.txt  # stdin, lote 5000\n"
            "  python3 cliente.py 127.0.0.1 numeros.txt            # arquivo → interativo\n"
            "  python3 cliente.py 127.0.0.1 numeros.txt --lote 500 # arquivo, lote 500\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('ip_lider', help="IP do líder atual")
    parser.add_argument('arquivo', nargs='?', default=None, help="Arquivo de números (opcional)")
    parser.add_argument('--lote', type=int, default=1000, metavar='N',
                        help="Números por requisição no modo arquivo/stdin (padrão: 1000)")
    args = parser.parse_args()

    ip_lider = args.ip_lider
    arquivo = args.arquivo
    tamanho_lote = args.lote
    stdin_redirecionado = not sys.stdin.isatty()

    c = ClienteSoma(ip_lider)

    def sessao(cliente, arquivo, tamanho_lote, stdin_redirecionado):
        try:
            if arquivo:
              
                thread_leitura_arquivo(cliente, arquivo, tamanho_lote)
                thread_leitura_interativo(cliente)
            elif stdin_redirecionado:
        
                thread_leitura_stdin(cliente, tamanho_lote)
            else:
        
                thread_leitura_interativo(cliente)
        except (KeyboardInterrupt, EOFError):
            pass

    t = threading.Thread(
        target=sessao,
        args=(c, arquivo, tamanho_lote, stdin_redirecionado),
        daemon=True,
    )
    t.start()
    while True:
        time.sleep(1)
