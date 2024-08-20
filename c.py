import socket
import threading
import sys
import time
import hashlib

# Operation codes
FIND_SUCCESSOR = 1
FIND_PREDECESSOR = 2
GET_SUCCESSOR = 3
GET_PREDECESSOR = 4
NOTIFY = 5
CHECK_PREDECESSOR = 6
CLOSEST_PRECEDING_FINGER = 7
STORE_KEY = 8
RETRIEVE_KEY = 9


# Function to hash a string using SHA-1 and return its integer representation
def getShaRepr(data: str):
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)


class ChordNodeReference:
    def __init__(self, ip: str, port: int = 8001):
        self.id = getShaRepr(ip)
        self.ip = ip
        self.port = port

    # Internal method to send data to the referenced node (this node)
    def _send_data(self, op: int, data: str = None) -> bytes:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.ip, self.port))
                s.sendall(f'{op},{data}'.encode('utf-8'))
                return s.recv(1024)
        except Exception as e:
            print(f"Error sending data: {e}")
            return b''

    # Method to find the successor of a given id
    def find_successor(self, id: int) -> 'ChordNodeReference':
        response = self._send_data(FIND_SUCCESSOR, str(id)).decode().split(',')
        ip = response[1]
        return ChordNodeReference(ip, self.port)

    # Method to find the predecessor of a given id
    def find_predecessor(self, id: int) -> 'ChordNodeReference':
        response = self._send_data(FIND_PREDECESSOR, str(id)).decode().split(',')
        ip = response[1]
        return ChordNodeReference(ip, self.port)

    # Property to get the successor of the current node
    @property
    def successor(self) -> 'ChordNodeReference':
        response = self._send_data(GET_SUCCESSOR).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    # Property to get the predecessor of the current node
    @property
    def predecesor(self) -> 'ChordNodeReference':
        response = self._send_data(GET_PREDECESSOR).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    # Method to notify the current node about another node
    def notify(self, node: 'ChordNodeReference'):
        self._send_data(NOTIFY, f'{node.id},{node.ip}')

    # Method to check if the predecessor is alive
    def check_predecessor(self):
        self._send_data(CHECK_PREDECESSOR)

    # Method to find the closest preceding finger of a given id
    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        response = self._send_data(CLOSEST_PRECEDING_FINGER, str(id)).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    # Method to store a key-value pair in the current node
    def store_key(self, key: str, value: str):
        pass

    # Method to retrieve a value for a given key from the current node
    def retrieve_key(self, key: str) -> str:
        pass

    def __str__(self) -> str:
        return f'{self.id},{self.ip},{self.port}'

    def __repr__(self) -> str:
        return str(self)




class ChordNode:
    def __init__(self, ip: str, port: int = 8001, m: int = 160):
        self.ip = ip
        self.id = getShaRepr(ip)
        self.port = port
        self.ref: ChordNodeReference = ChordNodeReference(self.ip, self.port)
        self.succ: ChordNodeReference = self.ref  # Initial successor is itself
        self.pred: ChordNodeReference = None  # Initially no predecessor
        self.m = m  # Number of bits in the hash/key space
        self.finger = [self.ref] * self.m  # Finger table
        self.next = 0  # Finger table index to fix next

        threading.Thread(target=self.stabilize, daemon=True).start()  # Start stabilize thread
        threading.Thread(target=self.fix_fingers, daemon=True).start()  # Start fix fingers thread
        threading.Thread(target=self.check_predecessor, daemon=True).start()  # Start check predecessor thread
        threading.Thread(target=self.start_server, daemon=True).start()  # Start server thread
    
    # Helper method to check if a value is in the range (start, end]
    def _inbetween(self, k: int, start: int, end: int) -> bool:
        if start < end:
            return start < k <= end
        else:  # The interval wraps around 0
            return start < k or k <= end

    # Method to find the successor of a given id
    def find_succ(self, id: int) -> 'ChordNodeReference':
        if id == self.id:
            return self.ref
        node = self.find_pred(id)  # Find predecessor of id
        return self.succ if node.id == id else node.successor

    # Method to find the predecessor of a given id
    def find_pred(self, id: int) -> 'ChordNodeReference':
        node = self
        while not self._inbetween(id, node.id, node.succ.id):
            node = node.pred
            # node = node.closest_preceding_finger(id)
        return node.ref

    # Method to find the closest preceding finger of a given id
    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        pass

    # Method to join a Chord network using 'node' as an entry point
    def join(self, node: 'ChordNodeReference'):
        if node:
            # self.pred = None
            self.succ = node.find_successor(self.id)
            self.pred = self.succ.predecesor
            self.succ.notify(self.ref)
        else:
            self.succ = self.ref
            self.pred = None

    # Stabilize method to periodically verify and update the successor and predecessor
    def stabilize(self):
        while True:
            if self.succ.id != self.id:
                print('[*] Stabilize...')
                x = self.succ.predecesor
                if x.id != self.id:
                    print("[-] mi sucesor cree que su predecesor no soy yo")
                    if x and self._inbetween(x.id, self.id, self.succ.id):
                        print("[-] hay alguien entre mi sucesor y yo")
                        print(f"es: {x.id}")
                        self.succ = x
                    self.succ.notify(self.ref)
                else:
                    print("[-] mi sucesor sabe que su predecesor soy yo")
                    self.succ = self.ref
                    print("[-] actualicé mi sucesor a mí mismo")

            print("\n==============================")
            print(f"[IP] {self.ip}")
            print(f"[ID] {self.id}")
            print(f"[Succ] {self.succ}")
            print(f"[Pred] {self.pred}\n")
            time.sleep(10)

    # Notify method to inform the node about another node
    def notify(self, node: 'ChordNodeReference'):
        print(f"[*] me notificó {node.id}")
        if node.id == self.id:
            pass
        elif not self.pred:
            self.pred = node
            if self.id == self.succ.id:
                self.succ = node
                self.succ.notify(self.ref)
        elif self._inbetween(node.id, self.pred.id, self.id):
            self.pred = node

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

    # Start server method to handle incoming requests
    def start_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.port))
            s.listen(10)

            while True:
                conn, addr = s.accept()
                data = conn.recv(1024).decode().split(',')

                data_resp = None
                option = int(data[0])


                # Switch operation
                if option == FIND_SUCCESSOR:
                    print(f"[*] {addr} requested FIND_SUCCESSOR")
                    target_id = int(data[1])
                    data_resp = self.find_succ(target_id)

                elif option == FIND_PREDECESSOR:
                    print(f"[*] {addr} requested FIND_PREDECESSOR")
                    target_id = int(data[1])
                    data_resp = self.find_pred(target_id)

                elif option == GET_SUCCESSOR:
                    print(f"[*] {addr} requested GET_SUCCESSOR")
                    data_resp = self.succ if self.succ else self.ref

                elif option == GET_PREDECESSOR:
                    print(f"[*] {addr} requested GET_PREDECESSOR")
                    data_resp = self.pred if self.pred else self.ref

                elif option == NOTIFY:
                    print(f"[*] {addr} requested NOTIFY")
                    ip = data[2]
                    self.notify(ChordNodeReference(ip, self.port))


                
                # Send response
                if data_resp:
                    response = f'{data_resp.id},{data_resp.ip}'.encode()
                    conn.sendall(response)
                conn.close()





if __name__ == "__main__":
    # Get current IP
    ip = socket.gethostbyname(socket.gethostname())
    print(f"NODE-IP: {ip}")

    # Create node
    node = ChordNode(ip)

    # Single node case
    if len(sys.argv) == 2:
        try:
            id = int(sys.argv[1])
            print(f"ID: {id}")

            target_ip = ip

        except:
            raise Exception(f"Parameter {sys.argv[1]} cannot be interpreted as an ID")

    # Join node case
    elif len(sys.argv) == 3:
        try:
            target_node = sys.argv[2].split(":")
            _, target_ip = target_node
        except:
            raise Exception(f"Parameter {sys.argv[2]} cannot be interpreted as target ID:IP")
    
    else:
        raise Exception("Incorrect params")

    # Finally, connect reference
    print(f"target_ip = {target_ip}")
    # node.join(ChordNodeReference(target_ip, node.port))

    while True:
        pass