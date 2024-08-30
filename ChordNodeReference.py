import socket
from const import *
from utils import *

class ChordNodeReference:
    def __init__(self, ip: str, chord_port: int = DEFAULT_NODE_PORT, data_port: int = DEFAULT_DATA_PORT):
        self.id = getShaRepr(ip)
        self.ip = ip
        self.chord_port = chord_port
        self.data_port = data_port

    # Internal method to send data to the referenced node (this node)
    def _send_chord_data(self, op: int, data: str = None) -> bytes:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.ip, self.chord_port))
                s.sendall(f'{op},{data}'.encode('utf-8'))
                return s.recv(1024)
        except Exception as e:
            print(f"Error sending data: {e}")
            return b''

    # Method to find the successor of a given id
    def find_successor(self, id: int) -> 'ChordNodeReference':
        response = self._send_chord_data(FIND_SUCCESSOR, str(id)).decode().split(',')
        ip = response[1]
        return ChordNodeReference(ip, self.chord_port)

    # Method to find the predecessor of a given id
    def find_predecessor(self, id: int) -> 'ChordNodeReference':
        response = self._send_chord_data(FIND_PREDECESSOR, str(id)).decode().split(',')
        ip = response[1]
        return ChordNodeReference(ip, self.chord_port)

    # Property to get the successor of the current node
    @property
    def succ(self) -> 'ChordNodeReference':
        response = self._send_chord_data(GET_SUCCESSOR).decode().split(',')
        return ChordNodeReference(response[1], self.chord_port)

    # Property to get the predecessor of the current node
    @property
    def pred(self) -> 'ChordNodeReference':
        response = self._send_chord_data(GET_PREDECESSOR).decode().split(',')
        return ChordNodeReference(response[1], self.chord_port)

    # Method to notify the current node about another node
    def notify(self, node: 'ChordNodeReference'):
        self._send_chord_data(NOTIFY, f'{node.id},{node.ip}')

    def reverse_notify(self, node: 'ChordNodeReference'):
        self._send_chord_data(REVERSE_NOTIFY, f'{node.id},{node.ip}')

    def not_alone_notify(self, node: 'ChordNodeReference'):
        self._send_chord_data(NOT_ALONE_NOTIFY, f'{node.id},{node.ip}')

    # Method to check if the predecessor is alive
    def check_node(self) -> bool:
        response = self._send_chord_data(CHECK_NODE)
        if response != b'' and len(response.decode()) > 0:
            # Node provide a response
            return True
        return False
    
    def get_leader(self) -> str:
        leader = self._send_chord_data(GET_LEADER).decode().split(',')[1]
        return leader
    
    # Method to find the closest preceding finger of a given id
    def closest_preceding_finger(self, id: int) -> 'ChordNodeReference':
        response = self._send_chord_data(CLOSEST_PRECEDING_FINGER, str(id)).decode().split(',')
        return ChordNodeReference(response[1], self.chord_port)

    # Method to store a key-value pair in the current node
    def store_key(self, key: str, value: str):
        pass

    # Method to retrieve a value for a given key from the current node
    def retrieve_key(self, key: str) -> str:
        pass




    # ========================== Data Node ==============================

    # Internal method to send data to the referenced node (this node)
    def _send_data_data(self, op: int, data: str = None) -> bytes:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.ip, self.data_port))
                s.sendall(f'{op},{data}'.encode('utf-8'))
                return s.recv(1024)
        except Exception as e:
            print(f"Error sending data: {e}")
            return b''
    

    def insert_tag(self, tag: str) -> str:
        response = self._send_data_data(INSERT_TAG, tag).decode()
        return response

    def delete_tag(self, tag: str) -> str:
        response = self._send_data_data(DELETE_TAG, tag).decode()
        return response
    
    def append_file(self, tag: str, file_name: str):
        response = self._send_data_data(APPEND_FILE, f"{tag},{file_name}").decode()
        return response
    
    def remove_file(self, tag: str, file_name: str):
        response = self._send_data_data(REMOVE_FILE, f"{tag},{file_name}").decode()
        return response
    
    
    def insert_file(self, file_name: str) -> str:
        response = self._send_data_data(INSERT_FILE, file_name).decode()
        return response

    def delete_file(self, file_name: str) -> str:
        response = self._send_data_data(DELETE_FILE, file_name).decode()
        return response
    
    def append_tag(self, file_name: str, tag: str):
        response = self._send_data_data(APPEND_TAG, f"{file_name},{tag}").decode()
        return response
    
    def remove_tag(self, file_name: str, tag: str):
        response = self._send_data_data(REMOVE_TAG, f"{file_name},{tag}").decode()
        return response


    def insert_bin(self, file_name: str, bin: bytes):
        response = send_bin(f"{INSERT_BIN}", file_name, bin, self.ip, self.data_port, end_msg=True)
        return response
    
    def delete_bin(self, file_name: str):
        response = self._send_data_data(f"{DELETE_BIN}", file_name)
        return response


    # ====================================================================




    def __str__(self) -> str:
        return f'{self.id},{self.ip},{self.chord_port}'

    def __repr__(self) -> str:
        return str(self)
    


