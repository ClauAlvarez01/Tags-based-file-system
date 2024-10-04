import os
import shutil
import socket
import threading
import json
from const import *
from utils import *
from const import *
from typing import Dict, List


class Database:
    def __init__(self, db_ip: str, db_port: str = DEFAULT_DB_PORT) -> None:
        self.db_ip = db_ip
        self.db_port = db_port

        # For tags and correspondings file names
        self.tags: Dict[str, List[str]] = {}
        self.replicated_pred_tags: Dict[str, List[str]] = {}
        # For file names and correspondings tags
        self.files: Dict[str, List[str]] = {}
        self.replicated_pred_files: Dict[str, List[str]] = {}

        # Paths
        self.dir_path = f"database/{self.db_ip}"
        self.tags_path = f"{self.dir_path}/tags.json"
        self.files_path = f"{self.dir_path}/files.json"
        self.replicated_pred_tags_path = f"{self.dir_path}/replicated_pred_tags.json"
        self.replicated_pred_files_path = f"{self.dir_path}/replicated_pred_files.json"
        self.bins_path = f"{self.dir_path}/bins"
        self.replicated_pred_bins_path = f"{self.dir_path}/replicated_pred_bins"

        # Prepare storage
        self.set_up_storage()

        threading.Thread(target=self._recv, daemon=True).start()



    def set_up_storage(self):
        print("[💾] Setting up storage...")

        # Create all empty files if they dont exist
        if not os.path.exists(self.dir_path):            
            os.makedirs(self.dir_path)

        if os.path.isfile(self.tags_path):
            os.remove(self.tags_path)
        with open(self.tags_path, 'w') as json_file:
            json.dump(self.tags, json_file, indent=4)
        
        if os.path.isfile(self.files_path):
            os.remove(self.files_path)
        with open(self.files_path, 'w') as json_file:
            json.dump(self.files, json_file, indent=4)

        if os.path.isfile(self.replicated_pred_tags_path):
            os.remove(self.replicated_pred_tags_path)
        with open(self.replicated_pred_tags_path, 'w') as json_file:
            json.dump(self.replicated_pred_tags, json_file, indent=4)
        
        if os.path.isfile(self.replicated_pred_files_path):  
            os.remove(self.replicated_pred_files_path)
        with open(self.replicated_pred_files_path, 'w') as json_file:
            json.dump(self.replicated_pred_files, json_file, indent=4)

        if os.path.exists(self.bins_path):
            shutil.rmtree(self.bins_path)
        os.makedirs(self.bins_path)
        
        if os.path.exists(self.replicated_pred_bins_path):
            shutil.rmtree(self.replicated_pred_bins_path)
        os.makedirs(self.replicated_pred_bins_path)

        print("[💾] Successfull set up")



    ################################ Savers ###################################
    def save_tags(self):
        with open(self.tags_path, 'w') as json_file:
            json.dump(self.tags, json_file, indent=4)
    
    def save_files(self):
        with open(self.files_path, 'w') as json_file:
            json.dump(self.files, json_file, indent=4)

    def save_replicated_tags(self):
        with open(self.replicated_pred_tags_path, 'w') as json_file:
            json.dump(self.replicated_pred_tags, json_file, indent=4)

    def save_replicated_files(self):
        with open(self.replicated_pred_files_path, 'w') as json_file:
            json.dump(self.replicated_pred_files, json_file, indent=4)



    

    ################################# REQUEST FUNCTIONS ####################################
    # TAGS
    def owns_tag(self, tag: str) -> bool:
        return tag in self.tags
    
    def contains_tag(self, tag: str) -> bool:
        return tag in self.tags or tag in self.replicated_pred_tags

    def store_tag(self, tag: str, successor_ip: str):
        """Adds tag key to storage with empty list"""
        self.tags[tag] = []
        op = f"{REPLICATE_STORE_TAG}"
        msg = tag
        send_2(op, msg, successor_ip, self.db_port)      # Replicate it
        self.save_tags()

    def append_file(self, tag: str, file_name: str, successor_ip: str):
        """Appends file name to given tag storage"""
        self.tags[tag].append(file_name)
        op = f"{REPLICATE_APPEND_FILE}"
        msg = f"{tag};{file_name}"
        send_2(op, msg, successor_ip, self.db_port)      # Replicate it
        self.save_tags()
    
    def delete_tag(self, tag: str, successor_ip: str):
        """Deletes tag key from storage"""
        del self.tags[tag]
        op = f"{REPLICATE_DELETE_TAG}"
        msg = tag
        send_2(op, msg, successor_ip, self.db_port)      # Replicate it
        self.save_tags()

    def remove_file(self, tag: str, file_name: str, successor_ip: str):
        """Removes file name from given tag storage"""
        self.tags[tag].remove(file_name)
        if len(self.tags[tag]) == 0:
            del self.tags[tag]
        op = f"{REPLICATE_REMOVE_FILE}"
        msg = f"{tag};{file_name}"
        send_2(op, msg, successor_ip, self.db_port)      # Replicate it
        self.save_tags()

    def retrieve_tag(self, tag: str) -> str:
        """Retrieve list of files name associated with given tag"""
        data = {}
        if tag in self.tags:
            value = self.tags[tag]
            data["data"] = value
        else:
            data["data"] = []
        return json.dumps(data)
    
    ########################
    # FILES
    def owns_file(self, file_name: str) -> bool:
        return file_name in self.files
    
    def contains_file(self, file_name: str) -> bool:
        return file_name in self.files or file_name in self.replicated_pred_files

    def store_file(self, file_name: str, successor_ip: str):
        """Adds file name key to storage with empty list"""
        self.files[file_name] = []
        op = f"{REPLICATE_STORE_FILE}"
        msg = file_name
        send_2(op, msg, successor_ip, self.db_port)       # Replicate it
        self.save_files()

    def append_tag(self, file_name: str, tag: str, successor_ip: str):
        """Appends tag to given file name storage"""
        self.files[file_name].append(tag)
        op = f"{REPLICATE_APPEND_TAG}"
        msg = f"{file_name};{tag}"
        send_2(op, msg, successor_ip, self.db_port)       # Replicate it
        self.save_files()

    def delete_file(self, file_name: str, successor_ip: str):
        """Deletes file name key from storage"""
        del self.files[file_name]
        op = f"{REPLICATE_DELETE_FILE}"
        msg = file_name
        send_2(op, msg, successor_ip, self.db_port)       # Replicate it
        self.save_files()

    def remove_tag(self, file_name: str, tag: str, successor_ip: str):
        """Removes tag from given file name storage"""
        self.files[file_name].remove(tag)
        op = f"{REPLICATE_REMOVE_TAG}"
        msg = f"{file_name};{tag}"
        send_2(op, msg, successor_ip, self.db_port)       # Replicate it
        self.save_files()

    def retrieve_file(self, file_name: str) -> str:
        """Retrieve list of tags associated with given file name"""
        data = {}
        if file_name in self.files:
            value = self.files[file_name]
            data["data"] = value
        else:
            data["data"] = []
        return json.dumps(data)
    
    #######################
    # BINS
    def store_bin(self, file_name: str, bin: bytes, successor_ip: str):
        """Stores file content"""
        file_path = f"{self.bins_path}/{file_name}"
        with open(file_path, 'wb') as file:
            file.write(bin)

        op = f"{REPLICATE_STORE_BIN}"
        send_bin(op, file_name, bin, successor_ip, self.db_port)    # Replicate it

    def delete_bin(self, file_name: str, successor_ip: str):
        """Deletes file content"""
        file_path = f"{self.bins_path}/{file_name}"
        os.remove(file_path)

        op = f"{REPLICATE_DELETE_BIN}"
        msg = file_name
        send_2(op, msg, successor_ip, self.db_port)                 # Replicate it

    def retrieve_bin(self, file_name: str) -> bytes:
        file_path = f"{self.bins_path}/{file_name}"
        
        content = []
        with open(file_path, 'rb') as file:
            while True:
                fragment = file.read(1024)
                if not fragment:
                    break
                content.append(fragment)
            bin = b''.join(content)

        return bin
    
    ############################################################################################



    ################################### REPLICATION FUNCS ######################################
    # Function to assume data from old failed owner
    def assume_data(self, successor_ip: str, new_predecessor_ip: str = None):
        print(f"[📥] Assuming predecesor data")

        # Assume replicated tags
        self.tags.update(self.replicated_pred_tags)
        print(f"[📥] {len(self.replicated_pred_tags.items())} tags assumed")
        self.replicated_pred_tags = {}
        self.save_tags()
        self.save_replicated_tags()

        # Assume replicated bins
        for k, _ in self.replicated_pred_files.items():
            # Read bin
            file_path = f"{self.replicated_pred_bins_path}/{k}"
            content = []
            with open(file_path, 'rb') as file:
                while True:
                    data = file.read(1024)
                    if not data:
                        break
                    content.append(data)
                content = b''.join(content)

            # Write bin
            new_file_path = f"{self.bins_path}/{k}"
            with open(new_file_path, 'wb') as file:
                file.write(content)

            # Delete replicated bin
            os.remove(file_path)

        # Assume replicated files
        self.files.update(self.replicated_pred_files)
        print(f"[📥] {len(self.replicated_pred_files.items())} files assumed")
        self.replicated_pred_files = {}
        self.save_files()
        self.save_replicated_files()

        # Let successor know my data has changed
        self.send_fetch_notification(successor_ip)
        
        # Pull replication from predecessor
        if new_predecessor_ip:
            self.pull_replication(new_predecessor_ip)


    # Function to delegate data to the new incoming owner
    def delegate_data(self, new_owner_ip: str, listener_ip: str):
        print(f"[📤] Delegating data to {new_owner_ip}")
        i_t = 0
        i_f = 0

        new_owner_id = getShaRepr(new_owner_ip)
        my_id = getShaRepr(self.db_ip)

        tags_to_delegate = {}
        for k, v in self.tags.items():
            tag_hash = getShaRepr(k)
            if not inbetween(tag_hash, new_owner_id, my_id):
                tags_to_delegate[k] = v
                i_t+=1

        files_to_delegate = {}
        for k, v in self.files.items():
            file_name_hash = getShaRepr(k)
            if not inbetween(file_name_hash, new_owner_id, my_id):
                files_to_delegate[k] = v
                i_f+=1

        # Send corresponding data to new owner
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((new_owner_ip, self.db_port))
            s.sendall(f"{PUSH_DATA}".encode('utf-8'))
            
            ack = s.recv(1024).decode('utf-8')
            if ack != f"{OK}":
                raise Exception("ACK negativo")

            # Send tags
            tags_json_str = json.dumps(tags_to_delegate)
            s.sendall(tags_json_str.encode('utf-8'))

            ack = s.recv(1024).decode('utf-8')
            if ack != f"{OK}":
                raise Exception("ACK negativo")

            # Send files
            files_json_str = json.dumps(files_to_delegate)
            s.sendall(files_json_str.encode('utf-8'))

            ack = s.recv(1024).decode('utf-8')
            if ack != f"{OK}":
                raise Exception("ACK negativo")
            
            # Send bins
            send_bins(s, files_to_delegate, self.bins_path)
            
            # Send ip
            s.sendall(f"{self.db_ip}".encode('utf-8'))
            s.close()

        print(f"[📤] {i_t} tags delegated")
        print(f"[📤] {i_f} files delegated")

        # Delete not corresponding data
        for k, v in tags_to_delegate.items():
            del self.tags[k]
        for k, v in files_to_delegate.items():
            del self.files[k]
        for k, _ in files_to_delegate.items():
            file_path = f"{self.bins_path}/{k}"
            os.remove(file_path)

        self.save_tags()
        self.save_files()

        # Let know my successor i have new data
        self.send_fetch_notification(listener_ip)


    # Function to pull all data from node's predecessor and store it in replication dict
    def pull_replication(self, owner_ip: str):
        # Delete current replicates
        for k, _ in self.replicated_pred_files.items():
            os.remove(f"{self.replicated_pred_bins_path}/{k}")
        self.replicated_pred_tags = {}
        self.replicated_pred_files = {}
        
        # Get actual predecesor replicas
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((owner_ip, self.db_port))

            # Ask for replication
            s.sendall(f"{PULL_REPLICATION}".encode('utf-8'))

            # Receive tags
            tags_json_str = s.recv(1024).decode('utf-8')
            tags_data = json.loads(tags_json_str)

            s.sendall(f"{OK}".encode('utf-8'))

            # Receive files
            files_json_str = s.recv(1024).decode('utf-8')
            files_data = json.loads(files_json_str)

            s.sendall(f"{OK}".encode('utf-8'))
            
            # Receive and write bins
            recv_write_bins(s, self.replicated_pred_bins_path)

            # Overwrite replicated tags and files
            self.replicated_pred_tags = tags_data
            self.replicated_pred_files = files_data

            self.save_replicated_tags()
            self.save_replicated_files()

            print(f"[📩] I pulled replication from {owner_ip}")
            s.close()

    

    # Function to notify my replications listeners, that my data has changed
    def send_fetch_notification(self, target_ip: str):
        send_2(f"{FETCH_REPLICA}", self.db_ip, target_ip, self.db_port)

    ########################################################################################





    def _recv(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.db_ip, self.db_port))
        sock.listen(10)

        while True:
            conn, _ = sock.accept()
            data = conn.recv(1024).decode('utf-8')
            threading.Thread(target=self._handle_recv, args=(conn, data)).start()


    def _handle_recv(self, conn: socket.socket, data: str):

        if data == f"{REPLICATE_STORE_TAG}":
            conn.sendall(f"{OK}".encode('utf-8'))
            tag = conn.recv(1024).decode('utf-8')
            self.replicated_pred_tags[tag] = []
            conn.sendall(f"{OK}".encode('utf-8'))
            self.save_replicated_tags()

        elif data == f"{REPLICATE_APPEND_FILE}":
            conn.sendall(f"{OK}".encode('utf-8'))
            data = conn.recv(1024).decode('utf-8').split(';')
            tag, file_name = data[0], data[1]
            self.replicated_pred_tags[tag].append(file_name)
            conn.sendall(f"{OK}".encode('utf-8'))
            self.save_replicated_tags()

        elif data == f"{REPLICATE_DELETE_TAG}":
            conn.sendall(f"{OK}".encode('utf-8'))
            tag = conn.recv(1024).decode('utf-8')
            del self.replicated_pred_tags[tag]
            conn.sendall(f"{OK}".encode('utf-8'))
            self.save_replicated_tags()

        elif data == f"{REPLICATE_REMOVE_FILE}":
            conn.sendall(f"{OK}".encode('utf-8'))
            data = conn.recv(1024).decode('utf-8').split(';')
            tag, file_name = data[0], data[1]
            self.replicated_pred_tags[tag].remove(file_name)
            if len(self.replicated_pred_tags[tag]) == 0:
                del self.replicated_pred_tags[tag]
            conn.sendall(f"{OK}".encode('utf-8'))
            self.save_replicated_tags()


        
        elif data == f"{REPLICATE_STORE_FILE}":
            conn.sendall(f"{OK}".encode('utf-8'))
            file_name = conn.recv(1024).decode('utf-8')
            self.replicated_pred_files[file_name] = []
            conn.sendall(f"{OK}".encode('utf-8'))
            self.save_replicated_files()

        elif data == f"{REPLICATE_APPEND_TAG}":
            conn.sendall(f"{OK}".encode('utf-8'))
            data = conn.recv(1024).decode('utf-8').split(';')
            file_name, tag = data[0], data[1]
            self.replicated_pred_files[file_name].append(tag)
            conn.sendall(f"{OK}".encode('utf-8'))
            self.save_replicated_files()

        elif data == f"{REPLICATE_DELETE_FILE}":
            conn.sendall(f"{OK}".encode('utf-8'))
            file_name = conn.recv(1024).decode('utf-8')
            del self.replicated_pred_files[file_name]
            conn.sendall(f"{OK}".encode('utf-8'))
            self.save_replicated_files()

        elif data == f"{REPLICATE_REMOVE_TAG}":
            conn.sendall(f"{OK}".encode('utf-8'))
            data = conn.recv(1024).decode('utf-8').split(';')
            file_name, tag = data[0], data[1]
            self.replicated_pred_files[file_name].remove(tag)
            conn.sendall(f"{OK}".encode('utf-8'))
            self.save_replicated_files()


        
        elif data == f"{REPLICATE_STORE_BIN}":
            conn.sendall(f"{OK}".encode('utf-8'))
            file_name = conn.recv(1024).decode('utf-8')
            conn.sendall(f"{OK}".encode('utf-8'))
            bin = conn.recv(1024)
            with open(f"{self.replicated_pred_bins_path}/{file_name}", 'wb') as file:
                file.write(bin)
            conn.sendall(f"{OK}".encode('utf-8'))

        elif data == f"{REPLICATE_DELETE_BIN}":
            conn.sendall(f"{OK}".encode('utf-8'))
            file_name = conn.recv(1024).decode('utf-8')
            file_path = f"{self.replicated_pred_bins_path}/{file_name}"
            os.remove(file_path)
            conn.sendall(f"{OK}".encode('utf-8'))



        elif data == f"{PUSH_DATA}":
            conn.sendall(f"{OK}".encode('utf-8'))

            # Receive and update tags
            tags_json_str = conn.recv(1024).decode('utf-8')
            new_tags = json.loads(tags_json_str)
            self.tags.update(new_tags)
            self.save_tags()

            conn.sendall(f"{OK}".encode('utf-8'))

            # Receive and update files
            files_json_str = conn.recv(1024).decode('utf-8')
            new_files = json.loads(files_json_str)
            self.files.update(new_files)
            self.save_files()

            conn.sendall(f"{OK}".encode('utf-8'))

            # Receive and write bins
            recv_write_bins(conn, self.bins_path)
            
            # Send IP
            ip = conn.recv(1024).decode('utf-8')

            # Let my sucessor know i have new data
            self.send_fetch_notification(ip)
        

        # Send all my stored data
        elif data == f"{PULL_REPLICATION}":
            # Send tags
            tags_json_str = json.dumps(self.tags)
            conn.sendall(tags_json_str.encode('utf-8'))

            ack = conn.recv(1024).decode('utf-8')
            if ack != f"{OK}":
                raise Exception("ACK negativo")

            # Send files
            files_json_str = json.dumps(self.files)
            conn.sendall(files_json_str.encode('utf-8'))

            ack = conn.recv(1024).decode('utf-8')
            if ack != f"{OK}":
                raise Exception("ACK negativo")
            
            # Send bins
            send_bins(conn, self.files, self.bins_path)
    

        # Pull data to replicate
        elif data == f"{FETCH_REPLICA}":
            conn.sendall(f"{OK}".encode('utf-8'))

            ip = conn.recv(1024).decode('utf-8')
            
            self.pull_replication(ip)

            conn.sendall(f"{OK}".encode('utf-8'))



        conn.close()
    


