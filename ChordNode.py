import socket
import threading
import sys
import time
from const import *
from utils import *
from ipaddress import ip_address
from logger import Logger
from ChordNodeReference import ChordNodeReference


class ChordNode:
    def __init__(self, ip: str, port: int = 8001, m: int = 3):
        self.ip = ip
        self.id = getShaRepr(ip)
        self.port = port
        self.ref: ChordNodeReference = ChordNodeReference(self.ip, self.port)
        self.succ: ChordNodeReference = self.ref  # Initial successor is itself
        self.pred: ChordNodeReference = None  # Initially no predecessor
        self.m = m  # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m  # Finger table
        self.next = 0  # Finger table index to fix next

        # Start logger
        self.logger = Logger(self)

        # Start threads
        threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        # threading.Thread(target=self.fix_fingers, daemon=True).start()  # Start fix fingers thread
        # threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check predecessor thread
        threading.Thread(target=self.start_server, daemon=True).start()  # Start server thread
    
    # Helper method to check if a value is in the range (start, end]
    def _inbetween(self, k: int, start: int, end: int) -> bool:
        if start < end:
            return start < k <= end
        else:  # The interval wraps around 0
            return start < k or k <= end

    # Method to find the successor of a given id
    def find_succ(self, id: int) -> 'ChordNodeReference':
        # if id == self.id:
        #     return self.ref
        node = self.find_pred(id)  # Find predecessor of id
        return node.succ
        # return self.succ if node.id == id else node.successor

    # Method to find the predecessor of a given id
    def find_pred(self, id: int) -> 'ChordNodeReference':
        node = self
        while not self._inbetween(id, node.id, node.succ.id):
            node = node.pred
            # node = node.closest_preceding_finger(id)
        return node.ref if isinstance(node, ChordNode) else node

    # Method to find the closest preceding finger of a given id
    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        pass

    # Method to join a Chord network using 'node' as an entry point
    def join(self, node: 'ChordNodeReference' = None):
        print("[*] Joining...")
        if node:
            self.pred = None
            self.succ = node.find_successor(self.id)
            print(f"[-] seteo de sucessor a {self.succ}")

            # Second node joins to chord ring
            if self.succ.succ.id == self.succ.id:
                # Notify node he is not alone
                self.succ.not_alone_notify(self.ref)
                self.pred = self.succ
        else:
            self.succ = self.ref
            self.pred = None
        print("[*] end join")

    # Stabilize method to periodically verify and update the successor and predecessor
    def stabilize(self):
        while True:
            if self.succ.id != self.id:
                print('[*] Stabilizating...')
                x = self.succ.pred

                if x.id != self.id:
                    print("[-] mi sucesor dice que su predecesor no soy yo")
                    
                    # Check is there is anyone between me and my successor
                    if x and self._inbetween(x.id, self.id, self.succ.id):
                        # Setearlo si no es el mismo
                        if x.id != self.succ.id:
                            print("[-] hay alguien entre mi sucesor y yo")
                            print(f"es: {x.id}")
                            self.succ = x
                    
                    # Notify mi successor
                    self.succ.notify(self.ref)
                    print('[*] end stabilize...')
                else:
                    print("[*] already stable")

            self.logger.refresh()
            time.sleep(10)

    # Notify method to inform the node about another node
    def notify(self, node: 'ChordNodeReference'):
        print(f"[*] Node {node.id} notified me, acting...")
        if node.id == self.id:
            pass
        else:
            if self.pred is None:
                self.pred = node
                
            # Check node still exists
            elif node.check_node():
                # Check if node is between my predecessor and me
                if self._inbetween(node.id, self.pred.id, self.id):
                    print(f"[-] cambié mi predecesor a {node.id}")
                    self.pred = node
        print(f"[*] end act...")
            
    def not_alone_notify(self, node: 'ChordNodeReference'):
        print(f"[*] Node {node.id} say I am not alone now, acting..")
        self.succ = node
        self.pred = node
        print(f"[*] end act...")
        

    # Fix fingers method to periodically update the finger table
    def fix_fingers(self):
        pass

    # Check predecessor method to periodically verify if the predecessor is alive
    def check_predecessor(self):
        pass

    # Store key method to store a key-value pair and replicate to the successor
    def store_key(self, key: str, value: str):
        pass

    # Retrieve key method to get a value for a given key
    def retrieve_key(self, key: str) -> str:
        pass





    def request_handler(self, conn: socket, addr, data: list):
        data_resp = None
        option = int(data[0])

        # Switch operation
        if option == FIND_SUCCESSOR:
            print(f"[*] {addr} requested FIND_SUCCESSOR...")
            target_id = int(data[1])
            data_resp = self.find_succ(target_id)

        elif option == FIND_PREDECESSOR:
            print(f"[*] {addr} requested FIND_PREDECESSOR...")
            target_id = int(data[1])
            data_resp = self.find_pred(target_id)

        elif option == GET_SUCCESSOR:
            print(f"[*] {addr} requested GET_SUCCESSOR...")
            data_resp = self.succ if self.succ else self.ref

        elif option == GET_PREDECESSOR:
            print(f"[*] {addr} requested GET_PREDECESSOR...")
            data_resp = self.pred if self.pred else self.ref

        elif option == NOTIFY:
            print(f"[*] {addr} requested NOTIFY...")
            ip = data[2]
            self.notify(ChordNodeReference(ip, self.port))

        elif option == NOT_ALONE_NOTIFY:
            print(f"[*] {addr} requested NOT_ALONE_NOTIFY...")
            ip = data[2]
            self.not_alone_notify(ChordNodeReference(ip, self.port))

        elif option == CHECK_NODE:
            print(f"[*] {addr} requested CHECK_PREDECESSOR...")
            data_resp = self.ref



        # Send response
        if data_resp:
            response = f'{data_resp.id},{data_resp.ip}'.encode()
            conn.sendall(response)
            print(f"[*] response gived")
        conn.close()



    # Start server method to handle incoming requests
    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.port))
            s.listen(10)

            while True:
                conn, addr = s.accept()
                data = conn.recv(1024).decode().split(',')

                threading.Thread(target=self.request_handler, args=(conn, addr, data)).start()

                


if __name__ == "__main__":
    # Get current IP
    ip = socket.gethostbyname(socket.gethostname())

    # Create node
    node = ChordNode(ip)

    # Single node case
    if len(sys.argv) == 1:
        node.join()

    # Join node case
    elif len(sys.argv) == 2:
        try:
            target_ip = ip_address(sys.argv[1])
        except:
            raise Exception(f"Parameter {sys.argv[1]} is not a valid IP address")
        
        node.join(ChordNodeReference(sys.argv[1]))
    else:
        raise Exception("Incorrect params")

    while True:
        pass