import json
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
            # print(f"Error sending data: {e}")
            return b''

    # Method to find the successor of a given id
    # def find_successor(self, id: int) -> 'ChordNodeReference':
    #     response = self._send_chord_data(FIND_SUCCESSOR, str(id)).decode('utf-8').split(',')
    #     ip = response[1]
    #     return ChordNodeReference(ip, self.chord_port)

    # Method to find the predecessor of a given id
    def find_predecessor(self, id: int) -> 'ChordNodeReference':
        response = self._send_chord_data(FIND_PREDECESSOR, str(id)).decode('utf-8').split(',')
        ip = response[1]
        return ChordNodeReference(ip, self.chord_port)

    # Property to get the successor of the current node
    @property
    def succ(self) -> 'ChordNodeReference':
        response = self._send_chord_data(GET_SUCCESSOR).decode('utf-8').split(',')
        return ChordNodeReference(response[1], self.chord_port)

    # Property to get the predecessor of the current node
    @property
    def pred(self) -> 'ChordNodeReference':
        response = self._send_chord_data(GET_PREDECESSOR).decode('utf-8').split(',')
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
        if response != b'' and len(response.decode('utf-8')) > 0:
            # Node provide a response
            return True
        return False
    
    def get_leader(self) -> str:
        leader = self._send_chord_data(GET_LEADER).decode('utf-8').split(',')[1]
        return leader
    
    def lookup(self, id: int):
        response = self._send_chord_data(LOOKUP, str(id)).decode('utf-8').split(',')
        return ChordNodeReference(response[1], self.chord_port)
    



    # ========================== Data Node ==============================

    # Internal method to send data to the referenced node (this node)
    def _send_data_data(self, op: int, data: str = None) -> bytes:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self.ip, self.data_port))
                s.sendall(f'{op},{data}'.encode('utf-8'))
                return s.recv(1024)
        except Exception as e:
            # print(f"Error sending data: {e}")
            return b''
    

    def insert_tag(self, tag: str) -> str:
        """Inserts a tag in system, if tag already exists, throw no error (works from any node)"""
        response = self._send_data_data(INSERT_TAG, tag).decode('utf-8')
        return response

    def delete_tag(self, tag: str) -> str:
        """Deletes a tag from system, if tag does not exists, throw no error (works from any node)"""
        response = self._send_data_data(DELETE_TAG, tag).decode('utf-8')
        return response
    
    def append_file(self, tag: str, file_name: str):
        """Appends file name to tag (works from any node)"""
        response = self._send_data_data(APPEND_FILE, f"{tag},{file_name}").decode('utf-8')
        return response
    
    def remove_file(self, tag: str, file_name: str):
        """Removes file name from tag (works from any node)"""
        response = self._send_data_data(REMOVE_FILE, f"{tag},{file_name}").decode('utf-8')
        return response
    
    def retrieve_tag(self, tag: str) -> list[str]:
        """Retrieves files list from given tag (only works from owner node)"""
        json_str_data = self._send_data_data(RETRIEVE_TAG, tag).decode('utf-8')
        json_data = json.loads(json_str_data)
        return json_data['data']
    
    
    
    def insert_file(self, file_name: str) -> str:
        """Inserts a file name in system, if already exists, throw no error (works from any node)"""
        response = self._send_data_data(INSERT_FILE, file_name).decode('utf-8')
        return response

    def delete_file(self, file_name: str) -> str:
        """Deletes a file name from system, if does not exists, throw no error (works from any node)"""
        response = self._send_data_data(DELETE_FILE, file_name).decode('utf-8')
        return response
    
    def append_tag(self, file_name: str, tag: str):
        """Appends tag to file (works from any node)"""
        response = self._send_data_data(APPEND_TAG, f"{file_name},{tag}").decode('utf-8')
        return response
    
    def remove_tag(self, file_name: str, tag: str):
        """Removes tag from file (works from any node)"""
        response = self._send_data_data(REMOVE_TAG, f"{file_name},{tag}").decode('utf-8')
        return response

    def retrieve_file(self, file_name: str) -> list[str]:
        """Retrieves tags list from given file name (only works from owner node)"""
        json_str_data = self._send_data_data(RETRIEVE_FILE, file_name).decode('utf-8')
        json_data = json.loads(json_str_data)
        return json_data['data']
    
    def owns_file(self, file_name: str):
        """Returns '1' if node owns file name, else '0' (only works from owner node)"""
        response = self._send_data_data(OWNS_FILE, file_name).decode('utf-8')
        return response == "1"



    # Must be called from owner node
    def insert_bin(self, file_name: str, bin: bytes):
        """Inserts binary file in system (works from any node)"""
        response = send_bin(f"{INSERT_BIN}", file_name, bin, self.ip, self.data_port, end_msg=True)
        return response
    
    def delete_bin(self, file_name: str):
        """Deletes binary file from system (works from any node)"""
        response = self._send_data_data(f"{DELETE_BIN}", file_name)
        return response

    def retrieve_bin(self, file_name: str):
        """Retrieves file binary content"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((self.ip, self.data_port))

            s.sendall(f"{RETRIEVE_BIN},{file_name}".encode('utf-8'))

            file_bin = b''
            end_file = f"{END_FILE}".encode('utf-8')
            while True:
                fragment = s.recv(1024)
                if end_file in fragment:
                    file_bin += fragment.split(end_file)[0]
                    break
                else:
                    file_bin += fragment
                    
            s.close()
            return file_bin

    # ====================================================================




    def __str__(self) -> str:
        return f'{self.id},{self.ip},{self.chord_port}'

    def __repr__(self) -> str:
        return str(self)
    


