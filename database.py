import socket
import threading
from const import *
from utils import *

END = 0
OK = 1
PULL_REPLICATION = 2
# PUSH_REPLICATION = 3
PUSH_DATA = 4
PUSH_THIS_REPLICA = 6
PUSH_DELETE_THIS_REPLICA = 7
FETCH_REPLICA = 8

class Database:
    def __init__(self, db_ip: str, db_port: str = DEFAULT_DB_PORT) -> None:
        self.db_ip = db_ip
        self.db_port = db_port

        self.data = {}
        self.replicated_data = {}

        threading.Thread(target=self._recv, daemon=True).start()



    def store(self, key, value, successor_ip: str):
        self.data[key] = value
        self.push_this(key, value, successor_ip)
        
    def delete(self, key, successor_ip: str):
        del self.data[key]
        self.push_delete_this(key, successor_ip)

    def retrieve(self, key):
        return self.data[key]
    
    def constains(self, key: int) -> bool:
        return key in self.data
    

    def assume_data(self, successor_ip: str, new_predecessor_ip: str = None):
        print(f"[📥] Assuming predecesor data")
        i = 0

        # Assume replicated data
        for k, v in self.replicated_data.items():
            self.data[k] = v
            i+=1
        
        print(f"[📥] {i} files assumed")

        # Let successor know my data has changed
        self.send_fetch_notification(successor_ip)
        
        # Pull replication from predecessor
        if new_predecessor_ip:
            self.pull_replication(new_predecessor_ip)



    def delegate_data(self, new_owner_ip: str, listener_ip: str):
        print(f"[📤] Delegating data to {new_owner_ip}")
        i = 0

        # Delgar datos
        new_owner_id = getShaRepr(new_owner_ip)
        my_id = getShaRepr(self.db_ip)

        data_to_delegate = {}

        for k, v in self.data.items():
            if not inbetween(int(k), new_owner_id, my_id):
                data_to_delegate[k] = v
                i+=1

        # Send corresponding data to new owner
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((new_owner_ip, self.db_port))
            s.sendall(f"{PUSH_DATA}".encode())

            ack = s.recv(1024).decode()
            if ack != f"{OK}":
                raise Exception("ACK negativo")
            
            for k, v in data_to_delegate.items():
                s.sendall(f"{str(k)},{v}".encode())

                ack = s.recv(1024).decode()
                if ack != f"{OK}":
                    raise Exception("ACK negativo")
                
            s.sendall(f"{END}".encode())
            s.sendall(f"{self.db_ip}".encode())
            s.close()

        print(f"[📤] {i} files delegated")

        # Delete not corresponding data
        for k, v in data_to_delegate.items():
            del self.data[k]

        # Let know my successor i have new data
        self.send_fetch_notification(listener_ip)


    # Function to pull all data from node's predecessor and store it in replication dict
    def pull_replication(self, owner_ip: str):

        # Delete current replicates
        self.replicated_data = {}
        
        # Get actual predecesor replicas
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((owner_ip, self.db_port))

            # Ask for replication
            s.sendall(f"{PULL_REPLICATION}".encode())

            while True:
                data = s.recv(1024).decode()

                if data != f"{END}":
                    data = data.split(',')
                    key, value = data[0], data[1]
                    self.replicated_data[int(key)] = value
                    s.sendall(f"{OK}".encode())
                else:
                    break
            s.close()

        



    # def push_replication(self, target_ip: str):
    #     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    #         s.connect((target_ip, self.db_port))
    #         s.sendall(f"{PUSH_REPLICATION}".encode())

    #         ack = s.recv(1024).decode()

    #         if ack != f"{OK}":
    #             raise Exception("ACK negativo")
            
    #         for k, v in self.data.items():
    #             file = f"{str(k)},{v}".encode()
    #             s.sendall(file)
    #             # wait OK (a futuro cronometrar para cancelar si se demora en responder)
    #             ack = s.recv(1024).decode()
    #             if ack != f"{OK}":
    #                 raise Exception("ACK negativo")

    #         s.sendall(f"{END}".encode())


    # Function to push one new data to replicated storage
    def push_this(self, key, value, target_ip: str):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((target_ip, self.db_port))
            s.sendall(f"{PUSH_THIS_REPLICA}".encode())
        
            ack = s.recv(1024).decode()
            if ack != f"{OK}":
                raise Exception("ACK negativo")
            
            file = f"{str(key)},{value}".encode()
            s.sendall(file)
            
            # wait OK (a futuro cronometrar para cancelar si se demora en responder)
            if ack != f"{OK}":
                raise Exception("ACK negativo")
    
    # Function to delete an existing data of replicated storage
    def push_delete_this(self, key, target_ip: str):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((target_ip, self.db_port))
            s.sendall(f"{PUSH_DELETE_THIS_REPLICA}".encode())

            ack = s.recv(1024).decode()
            if ack != f"{OK}":
                raise Exception("ACK negativo")
            
            id = f"{str(key)}".encode()
            s.sendall(id)

            # wait OK (a futuro cronometrar para cancelar si se demora en responder)
            if ack != f"{OK}":
                raise Exception("ACK negativo")

    # 
    def send_fetch_notification(self, target_ip: str):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((target_ip, self.db_port))
            s.sendall(f"{FETCH_REPLICA}".encode())

            ack = s.recv(1024).decode()
            if ack != f"{OK}":
                raise Exception("ACK negativo")
            
            s.sendall(self.db_ip.encode())
            
            # wait OK (a futuro cronometrar para cancelar si se demora en responder)
            if ack != f"{OK}":
                raise Exception("ACK negativo")




    def _recv(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.db_ip, self.db_port))
        sock.listen(10)

        while True:
            conn, _ = sock.accept()
            data = conn.recv(1024).decode()
            threading.Thread(target=self._handle_recv, args=(conn, data)).start()


    def _handle_recv(self, conn: socket.socket, data: str):
        
        # Send all my stored data
        if data == f"{PULL_REPLICATION}":
            # format: <key>,<value>
            for k, v in self.data.items():
                file = f"{str(k)},{v}".encode()
                conn.sendall(file)
                # wait OK (a futuro cronometrar para cancelar si se demora en responder)
                ack = conn.recv(1024).decode()
                if ack != f"{OK}":
                    raise Exception("ACK negativo")
            
            # last message 
            conn.sendall(f"{END}".encode())


        # elif data == f"{PUSH_REPLICATION}":
        #     conn.sendall(f"{OK}".encode())

        #     while True:
        #         data = conn.recv(1024).decode()

        #         if data != f"{END}":
        #             data = data.split(',')
        #             key, value = data[0], data[1]
        #             self.replicated_data[key] = value
        #             conn.sendall(f"{OK}".encode())
        #         else:
        #             break
        #     conn.close()  

        elif data == f"{PUSH_DATA}":
            conn.sendall(f"{OK}".encode())

            while True:
                data = conn.recv(1024).decode()

                if data != f"{END}":
                    data = data.split(",")
                    k, v = data[0], data[1]

                    self.data[int(k)] = v

                    conn.sendall(f"{OK}".encode())
                else:
                    break

            ip = conn.recv(1024).decode()
            # Let my sucessor know i have new data
            self.send_fetch_notification(ip)
            



        elif data == f"{PUSH_THIS_REPLICA}":
            conn.sendall(f"{OK}".encode())

            data = conn.recv(1024).decode().split(',')
            key, value = data[0], data[1]

            self.replicated_data[int(key)] = value

            conn.sendall(f"{OK}".encode())


        elif data == f"{PUSH_DELETE_THIS_REPLICA}":
            conn.sendall(f"{OK}".encode())

            key = conn.recv(1024).decode()

            del self.replicated_data[int(key)]

            conn.sendall(f"{OK}".encode())

        
        elif data == f"{FETCH_REPLICA}":
            conn.sendall(f"{OK}".encode())

            ip = conn.recv(1024).decode()
            
            self.pull_replication(ip)

            conn.sendall(f"{OK}".encode())


        conn.close()
    


