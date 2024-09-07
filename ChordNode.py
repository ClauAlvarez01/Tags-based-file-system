import socket
import threading
import sys
import time
from const import *
from utils import *
from ipaddress import ip_address
from logger import Logger
from ChordNodeReference import ChordNodeReference
from leader_election import LeaderElection
from self_discovery import SelfDiscovery


class ChordNode:
    def __init__(self, ip: str, m: int = 160, update_replication = None):
        self.ip = ip
        self.id = getShaRepr(ip)
        self.chord_port = DEFAULT_NODE_PORT
        self.ref: ChordNodeReference = ChordNodeReference(self.ip, self.chord_port)
        self.succ: ChordNodeReference = self.ref
        self.pred: ChordNodeReference = None
        self.m = m  # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m  # Finger table
        self.next = 0  # Finger table index to fix next

        self.update_replication = update_replication

        self.election = LeaderElection()
        
        # Start threads
        threading.Thread(target=self.stabilize, daemon=True).start()              # Stabilize thread
        threading.Thread(target=self.check_predecessor, daemon=True).start()      # Check predecessor thread
        threading.Thread(target=self.start_server, daemon=True).start()           # Server thread
        threading.Thread(target=self.election.loop, daemon=True).start()          # Leader election thread
        threading.Thread(target=self._leader_checker, daemon=True).start()        # Periodical leader check TEMPORAL
        threading.Thread(target=self.start_broadcast_server, daemon=True).start() # Broadcast server thread
        threading.Thread(target=self.fix_fingers, daemon=True).start()            # Fix fingers thread
        

    # TEMPORAL - Periodical leader check
    def _leader_checker(self):
        while True:
            time.sleep(10)
            leader_node = ChordNodeReference(self.election.get_leader())
            if not leader_node.check_node()  and not self.election.in_election:
                print("[🔷] Leader lost")
                self.election.leader_lost()

    

    # Method to find the predecessor of a given id
    def find_pred(self, id: int) -> 'ChordNodeReference':
        node = self
        while not inbetween(id, node.id, node.succ.id):
            node = node.succ
        return node.ref if isinstance(node, ChordNode) else node
    

    # Method to find the successor of a given id
    # def find_succ(self, id: int) -> 'ChordNodeReference':
    #     node = self.find_pred(id)
    #     return node.succ


    def lookup(self, id: int) -> 'ChordNodeReference':
        if self.id == id:
            return self.ref
        # If the id is in the interval (this node's ID, its successor's ID], return the successor.
        if inbetween(id, self.id, self.succ.id):
            return self.succ
        
        # Otherwise, find the closest preceding node in the finger table and ask it.
        for i in range(len(self.finger) - 1, -1, -1):
            if self.finger[i] and inbetween(self.finger[i].id, self.id, id):
                if self.finger[i].check_node():
                    return self.finger[i].lookup(id)
        
        return self.succ



    # Fix fingers method to periodically update the finger table
    def fix_fingers(self):
        batch_size = 10
        while True:
            for _ in range(batch_size):
                try:
                    self.next += 1
                    if self.next >= self.m:
                        self.next = 0
                    self.finger[self.next] = self.lookup((self.id + 2 ** self.next) % 2 ** self.m)
                except Exception as e:
                    # print(f"Error in fix_fingers: {e}")
                    pass
            time.sleep(5)



    # Method to join a Chord network using 'node' as an entry point
    def join(self, node: 'ChordNodeReference' = None):
        self.election.adopt_leader(self.ip)
        print("[*] Joining...")
        if node:
            if not node.check_node():
                raise Exception(f"There is no node using the address {node.ip}")
            
            self.pred = None
            self.succ = node.lookup(self.id)
            self.election.adopt_leader(node.get_leader())

            # Second node joins to chord ring
            if self.succ.succ.id == self.succ.id:
                self.pred = self.succ
                # Notify node he is not alone
                self.succ.not_alone_notify(self.ref)
        else:
            self.succ = self.ref
            self.pred = None
            self.election.adopt_leader(self.ip)

        print("[*] end join")

    # Stabilize method to periodically verify and update the successor and predecessor
    def stabilize(self):
        while True:
            if self.succ.id != self.id:
                print('[⚖] Stabilizating...')

                # Check successor is alve before stabilization
                if self.succ.check_node():
                    x = self.succ.pred

                    if x.id != self.id:
                        
                        # Check is there is anyone between me and my successor
                        if x and inbetween(x.id, self.id, self.succ.id):
                            # Setearlo si no es el mismo
                            if x.id != self.succ.id:
                                self.succ = x
                        
                        # Notify mi successor
                        self.succ.notify(self.ref)
                        print('[⚖] end stabilize...')
                    else:
                        print("[⚖] 🟢 Already stable")
                else:
                    print("[⚖] I lost my successor, waiting for predecesor check...")

            time.sleep(10)

    # Notify method to inform the node about another node
    def notify(self, node: 'ChordNodeReference'):
        print(f"[*] Node {node.ip} notified me, acting...")
        if node.id == self.id:
            pass
        else:
            if self.pred is None:
                self.pred = node
                self.update_replication(False, True)
                
            # Check node still exists
            elif node.check_node():
                # Check if node is between my predecessor and me
                if inbetween(node.id, self.pred.id, self.id):
                    self.pred = node
                    self.update_replication(True, False)
        print(f"[*] end act...")

    def reverse_notify(self, node: 'ChordNodeReference'):
        print(f"[*] Node {node.id} reversed notified me, acting...")
        self.succ = node
        print(f"[*] end act...")

            
    def not_alone_notify(self, node: 'ChordNodeReference'):
        print(f"[*] Node {node.ip} say I am not alone now, acting..")
        self.succ = node
        self.pred = node

        # Update replication with new successor
        self.update_replication(True, False)

        print(f"[*] end act...")
        

    # Check predecessor method to periodically verify if the predecessor is alive
    def check_predecessor(self):
        while True:
            if self.pred: print("[*] Checking predecesor...")
            try:
                if self.pred and not self.pred.check_node():
                    print("[-] Predecesor failed")

                    self.pred = self.find_pred(self.pred.id)
                    self.pred.reverse_notify(self.ref)

                    if self.pred.id == self.id:
                        self.pred = None

                    # Assume 
                    self.update_replication(False, False, True)
                        

            except Exception as e:
                self.pred = None
                self.succ = self.ref

            time.sleep(10)
            pass




    def request_handler(self, conn: socket, addr, data: list):
        data_resp = None
        option = int(data[0])

        # Switch operation
        # if option == FIND_SUCCESSOR:
        #     target_id = int(data[1])
        #     data_resp = self.find_succ(target_id)

        if option == FIND_PREDECESSOR:
            target_id = int(data[1])
            data_resp = self.find_pred(target_id)

        elif option == LOOKUP:
            target_id = int(data[1])
            data_resp = self.lookup(target_id)

        elif option == GET_SUCCESSOR:
            data_resp = self.succ if self.succ else self.ref

        elif option == GET_PREDECESSOR:
            data_resp = self.pred if self.pred else self.ref

        elif option == NOTIFY:
            ip = data[2]
            self.notify(ChordNodeReference(ip, self.chord_port))

        elif option == REVERSE_NOTIFY:
            ip = data[2]
            self.reverse_notify(ChordNodeReference(ip, self.chord_port))

        elif option == NOT_ALONE_NOTIFY:
            ip = data[2]
            self.not_alone_notify(ChordNodeReference(ip, self.chord_port))

        elif option == CHECK_NODE:
            data_resp = self.ref

        elif option == GET_LEADER:
            leader_ip = self.election.get_leader()
            data_resp = ChordNodeReference(leader_ip)



        # Send response
        if data_resp:
            response = f'{data_resp.id},{data_resp.ip}'.encode()
            conn.sendall(response)
        conn.close()



    # Start server method to handle incoming requests
    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.chord_port))
            s.listen(10)

            while True:
                conn, addr = s.accept()
                data = conn.recv(1024).decode().split(',')

                threading.Thread(target=self.request_handler, args=(conn, addr, data)).start()



    def start_broadcast_server(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind(('', DEFAULT_BROADCAST_PORT))

        while True:
            data, addr = sock.recvfrom(1024)

            # Refuse self messages
            if addr[0] == self.ip:
                continue

            print(f"[*] Broadcast message received from {addr}")
            data = data.decode().split(',')
            option = int(data[0])

            if option == DISCOVER:
                sender_ip = data[1]
                sender_port = int(data[2])
                response = f'{ENTRY_POINT},{self.ip}'


            # Send response
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((sender_ip, sender_port))
                    s.sendall(response.encode('utf-8'))
            except Exception as e:
                if isinstance(e, ConnectionRefusedError):
                    print(f"[*] someone already responded")
                else:
                    print(e)
