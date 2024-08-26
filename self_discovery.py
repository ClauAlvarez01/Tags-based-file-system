import threading
import socket
import time
from const import *

SELF_DISC_SYMBOL = "🔎"

class SelfDiscovery:
    def __init__(self, ip: str, port: int = DEFAULT_NODE_PORT) -> None:
        self.ip = ip
        self.port = port

        self.target_ip = None

        threading.Thread(target=self._recv, daemon=True).start()

    
    def find(self) -> str:
        print(f"[{SELF_DISC_SYMBOL}] Self Discovery started")

        self._send(f'{DISCOVER},{self.ip},{self.port}')

        while not self.target_ip:
            time.sleep(0.25)
        
        print(f"[{SELF_DISC_SYMBOL}] discovered {self.target_ip}")
        return self.target_ip
        

    def _send(self, message: str):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)

        sock.sendto(message.encode(), ('255.255.255.255', DEFAULT_BROADCAST_PORT))
        print(f"[{SELF_DISC_SYMBOL}] Broadcasted DISCOVERY")


    def _recv(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.port))
            s.listen(5)

            while True:
                conn, addr = s.accept()

                # Refuse self messages
                if addr[0] == self.ip:
                    continue

                data = conn.recv(1024).decode().split(',')
                option = int(data[0])

                if option == ENTRY_POINT:
                    self.target_ip = data[1]
                    conn.close()
                    s.close()
                    break

