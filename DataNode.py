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
            print("🔼 Insertando")
            response = self.ref.insert("Hola").split(",")
            print(response)
            # time.sleep(1)
            print("🔼 Insertando")
            response = self.ref.insert("WXWXW").split(",")
            print(response)

            # time.sleep(1)
            print("🔼 Insertando")
            response = self.ref.insert("08mwx4r7gr2864x9").split(",")
            print(response)

            # time.sleep(5)
            print("🔼 Insertando")
            response = self.ref.insert("kkkkkkkkkkae akwme awe aw w3mrvw3mklw3klrvw3lkmlw").split(",")
            print(response)

            # time.sleep(5)
            print("🔼 Insertando")
            response = self.ref.insert("                   U                      ").split(",")
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




    def _handle_insert(self, data: list):
        name = data[0]
        info_hash = getShaRepr(name)
        owner = self.find_succ(info_hash)

        # I am owner
        if owner.id == self.id:
            if self.database.constains(info_hash):
                return "ERROR,Key already exists"
            else:
                self.database.store(info_hash, name, self.succ.ip)
                return "OK,Data inserted"
        # I am not owner
        else:
            response = owner.insert(name)
            return response
        

        
    def _handle_delete(self, data: list):
        name = data[0]
        name_hash = getShaRepr(name)
        owner = self.find_succ(name_hash)

        # I am owner
        if owner.id == self.id:
            if not self.database.constains(name_hash):
                return "ERROR,Key does not exists"
            else:
                self.database.delete(name_hash, self.succ.ip)
                return "OK,Data deleted"
        # I am not owner
        else:
            response = owner.delete(name)
            return response

        

        
    def request_data_handler(self, conn: socket, addr, data: list):
        response = None
        option = int(data[0])

        # Switch operation
        if option == INSERT:
            response = self._handle_insert(data[1:])
            
        elif option == DELETE:
            response = self._handle_delete(data[1:])


        
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
