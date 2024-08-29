import sys
import time
import socket
import threading
from const import *
from utils import *
from logger import Logger
from database import Database
from ChordNode import ChordNode
from self_discovery import SelfDiscovery
from ChordNodeReference import ChordNodeReference

class DataNode(ChordNode):
    def __init__(self, ip: str, chord_port: int = DEFAULT_NODE_PORT, data_port = DEFAULT_DATA_PORT, m: int = 3):
        super().__init__(ip, chord_port, m, self.update_replication)
        self.logger = Logger(self)
        # self.ip
        # self.id
        # self.port
        # self.ref
        # self.succ
        # self.pred
        # self.m
        self.data_port = data_port
        self.database = Database(ip)

        threading.Thread(target=self.start_data_server, daemon=True).start()
        threading.Thread(target=self.temporal_insert, daemon=True).start()
        

    def temporal_insert(self):
        time.sleep(5)
        if self.ip == '172.17.0.2':
            print("🔼 Insertando tag")
            response = self.ref.insert_tag("rojo").split(",")
            print(response)
            print("🔼 Insertando tag")
            response = self.ref.insert_tag("azul").split(",")
            print(response)

            print("🔼 Insertando file en tag")
            response = self.ref.append_file("rojo", "file1").split(",")
            print(response)

            print("🔼 Insertando file en tag")
            response = self.ref.append_file("azul", "file2").split(",")
            print(response)
            



    def update_replication(self, delegate_data: bool = False, pull_data: bool = True, assume_data: bool = False):
        
        if delegate_data:
            self.database.delegate_data(self.pred.ip, self.succ.ip)

        if pull_data:
            self.database.pull_replication(self.pred.ip)

        if assume_data:
            succ_ip = self.succ.ip
            pred_ip = self.pred.ip if self.pred else None
            self.database.assume_data(succ_ip, pred_ip)




    def _handle_insert_tag(self, data: list):
        tag = data[0]
        tag_hash = getShaRepr(tag)
        owner = self.find_succ(tag_hash)

        # I am owner
        if owner.id == self.id:
            if self.database.owns_tag(tag):
                return "ERROR,Key already exists"
            else:
                self.database.store_tag(tag, self.succ.ip)
                return "OK,Data inserted"
        # I am not owner, foward
        else:
            response = owner.insert_tag(tag)
            return response
        

        
    def _handle_delete_tag(self, data: list):
        tag = data[0]
        tag_hash = getShaRepr(tag)
        owner = self.find_succ(tag_hash)

        # I am owner
        if owner.id == self.id:
            if not self.database.owns_tag(tag):
                return "ERROR,Key does not exists"
            else:
                self.database.delete_tag(tag, self.succ.ip)
                return "OK,Data deleted"
        # I am not owner
        else:
            response = owner.delete_tag(tag)
            return response


    def _handle_append_file(self, data: list):
        tag, file_name = data[0], data[1]
        tag_hash = getShaRepr(tag)
        owner = self.find_succ(tag_hash)

        # I am owner
        if owner.id == self.id:
            self.database.append_file(tag, file_name, self.succ.ip)
            return "OK,Data appended"
        # I am not owner
        else:
            response = owner.append_file(tag, file_name)
            return response
        

    def _handle_remove_file(self, data: list):
        tag, file_name = data[0], data[1]
        tag_hash = getShaRepr(tag)
        owner = self.find_succ(tag_hash)

        # I am owner
        if owner.id == self.id:
            self.database.remove_file(tag, file_name, self.succ.ip)
            return "OK,Data deleted"
        # I am not owner
        else:
            response = owner.remove_file(tag, file_name)
            return response
        

        
    def request_data_handler(self, conn: socket, addr, data: list):
        response = None
        option = int(data[0])

        # Switch operation
        if option == INSERT_TAG:
            response = self._handle_insert_tag(data[1:])
            
        elif option == DELETE_TAG:
            response = self._handle_delete_tag(data[1:])

        elif option == APPEND_FILE:
            response = self._handle_append_file(data[1:])

        elif option == REMOVE_FILE:
            response = self._handle_remove_file(data[1:])

        
        if response:
            # format must be: "<op>,<data>""
            response = response.encode()
            conn.sendall(response)
        conn.close()

    
    def start_data_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.data_port))
            s.listen(10)

            while True:
                conn, addr = s.accept()
                data = conn.recv(1024).decode().split(',')

                threading.Thread(target=self.request_data_handler, args=(conn, addr, data)).start()


        



if __name__ == "__main__":
    # Get current IP
    ip = socket.gethostbyname(socket.gethostname())


    # First node case
    if len(sys.argv) == 1:

        # Create node
        node = DataNode(ip)
        print(f"[IP]: {ip}")

        node.join()


    # Join node case
    elif len(sys.argv) == 2:
        flag = sys.argv[1]

        if flag == "-c":
            target_ip = SelfDiscovery(ip).find()

            # Create node
            node = DataNode(ip)
            print(f"[IP]: {ip}")

            node.join(ChordNodeReference(target_ip))


        else:
            raise Exception(f"Missing flag: {flag} does not exist")

    else:
        raise Exception("Incorrect params")

    while True:
        pass
