import socket
from const import *
from utils import *

class ChordNodeReference:
    def __init__(self, ip: str, port: int = DEFAULT_NODE_PORT):
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
    def succ(self) -> 'ChordNodeReference':
        response = self._send_data(GET_SUCCESSOR).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    # Property to get the predecessor of the current node
    @property
    def pred(self) -> 'ChordNodeReference':
        response = self._send_data(GET_PREDECESSOR).decode().split(',')
        return ChordNodeReference(response[1], self.port)

    # Method to notify the current node about another node
    def notify(self, node: 'ChordNodeReference'):
        self._send_data(NOTIFY, f'{node.id},{node.ip}')

    def reverse_notify(self, node: 'ChordNodeReference'):
        self._send_data(REVERSE_NOTIFY, f'{node.id},{node.ip}')

    def not_alone_notify(self, node: 'ChordNodeReference'):
        self._send_data(NOT_ALONE_NOTIFY, f'{node.id},{node.ip}')

    # Method to check if the predecessor is alive
    def check_node(self) -> bool:
        response = self._send_data(CHECK_NODE)
        if response != b'' and len(response.decode()) > 0:
            # Node provide a response
            return True
        return False
    
    def get_leader(self) -> str:
        leader = self._send_data(GET_LEADER).decode().split(',')[1]
        return leader
    
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


