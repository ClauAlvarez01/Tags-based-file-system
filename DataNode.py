import socket
import threading
from const import *
from utils import *
from logger import Logger
from database import Database
from ChordNode import ChordNode

class DataNode(ChordNode):
    def __init__(self, ip: str):
        super().__init__(ip, update_replication=self.update_replication)
        self.logger = Logger(self)
        # self.ip
        # self.id
        # self.port
        # self.ref
        # self.succ
        # self.pred
        # self.m
        self.data_port = DEFAULT_DATA_PORT
        self.database = Database(ip)

        threading.Thread(target=self.start_data_server, daemon=True).start()
        



    ####################### Functions to use from upper layer ############################
    def tag_query(self, tags: list[str]) -> list[str]:
        """Return all files name that contain all given tags"""
        all_files_list: list[list[str]] = []
        for tag in tags:
            tag_hash = getShaRepr(tag)
            owner = self.lookup(tag_hash)
            files_list = owner.retrieve_tag(tag)
            all_files_list.append(files_list)

        if all_files_list == []: return []

        # Intersect all lists
        intersection = list(set.intersection(*map(set, all_files_list)))
        return intersection

    def copy(self, file_name: str, bin: bytes, tags: list[str]) -> bool:
        """Copy a file to the system, returns False if value already exists"""
        file_name_hash = getShaRepr(file_name)
        file_owner = self.lookup(file_name_hash)

        # Check already exist file error
        if file_owner.owns_file(file_name):
            return False, f"A file named {file_name} already exists in the system"

        # Copy binary file
        file_owner.insert_bin(file_name, bin)
        
        # Copy file name and tags
        self.handle_insert_file(file_name)
        for tag in tags:
            # Copy each tag to file
            self.handle_append_tag(file_name, tag)

            # Copy file name to each tag
            self.handle_insert_tag(tag)
            self.handle_append_file(tag, file_name)

        return True, ""
    
    def remove(self, file_name: str) -> bool:
        """Remove a file from system, returns False if value does not exists"""
        file_name_hash = getShaRepr(file_name)
        file_owner = self.lookup(file_name_hash)

        # Check does not exist file error
        if not file_owner.owns_file(file_name):
            return False, f"No file named {file_name} in the system"
        
        # Delete binary file
        file_owner.delete_bin(file_name)

        # Delete file name from all tags
        tags = file_owner.retrieve_file(file_name)
        for tag in tags:
            self.handle_remove_file(tag, file_name)

        # Delete file name and associated tags
        self.handle_delete_file(file_name)

        return True, ""

    def inspect(self, file_name: str) -> list[str]:
        """Returns the list of tags associated to given file name"""
        file_name_hash = getShaRepr(file_name)
        file_owner = self.lookup(file_name_hash)

        tags = file_owner.retrieve_file(file_name)
        return tags
    
    def add_tags(self, file_name: str, tags: list[str]) -> bool:
        """Adds tags to given file name"""
        file_name_hash = getShaRepr(file_name)
        file_owner = self.lookup(file_name_hash)

        current_file_tags = self.inspect(file_name)
        for tag in tags:
            if tag in current_file_tags:
                return False, f"tag ({tag}) already exists in this file"

        for tag in tags:
            # Add file name to tags
            self.handle_insert_tag(tag)
            self.handle_append_file(tag, file_name)

            # Add tags to file tags list
            file_owner.append_tag(file_name, tag)
        
        return True, ""

    def delete_tags(self, file_name: str, tags: list[str]):
        file_name_hash = getShaRepr(file_name)
        file_owner = self.lookup(file_name_hash)

        current_file_tags = self.inspect(file_name)
        for tag in tags:
            if tag not in current_file_tags:
                return False, f"tag ({tag}) is not associated to this file"

        if len(current_file_tags) == len(tags):
            return False, "file cannot have 0 tags"

        for tag in tags:
            # Remove file name from tag
            self.handle_remove_file(tag, file_name)

            # Remove tag from file tags list
            file_owner.remove_tag(file_name, tag)

        return True, ""
    
    def download(self, file_name: str) -> bytes:
        """Returns the binary content of given file name"""
        file_name_hash = getShaRepr(file_name)
        file_owner = self.lookup(file_name_hash)

        bin = file_owner.retrieve_bin(file_name)
        return bin
    ######################################################################################






    def update_replication(self, delegate_data: bool = False, pull_data: bool = True, assume_data: bool = False, is_pred: bool = True, case_2: bool = False, assume_predpred: str = None):
        
        if delegate_data:
            self.database.delegate_data(self.pred.ip, self.succ.ip, self.pred.ip, case_2)

        if pull_data:
            if is_pred:
                self.database.pull_replication(self.pred.ip, True)
            else:
                self.database.pull_replication(self.succ.ip, False)

        if assume_data:
            succ_ip = self.succ.ip
            pred_ip = self.pred.ip if self.pred else None
            # print(self.succ)
            # print(self.pred)
            # print(f"Se llama a asumir con succ: {succ_ip} y pred: {pred_ip}")
            self.database.assume_data(succ_ip, pred_ip, assume_predpred)





   

    def request_data_handler(self, conn: socket.socket, addr, data: list):
        response = None
        option = int(data[0])

        # Switch operation
        if option == INSERT_TAG:
            response = self.handle_insert_tag(data[1])
            
        elif option == DELETE_TAG:
            response = self.handle_delete_tag(data[1])

        elif option == APPEND_FILE:
            response = self.handle_append_file(data[1], data[2])

        elif option == REMOVE_FILE:
            response = self.handle_remove_file(data[1], data[2])

        elif option == RETRIEVE_TAG:
            response = self.handle_retrieve_tag(data[1])
            


        elif option == INSERT_FILE:
            response = self.handle_insert_file(data[1])
            
        elif option == DELETE_FILE:
            response = self.handle_delete_file(data[1])

        elif option == APPEND_TAG:
            response = self.handle_append_tag(data[1], data[2])

        elif option == REMOVE_TAG:
            response = self.handle_remove_tag(data[1], data[2])

        elif option == RETRIEVE_FILE:
            response = self.handle_retrieve_file(data[1])

        elif option == OWNS_FILE:
            owns_file = self.database.owns_file(data[1])
            response = "1" if owns_file else "0"


        elif option == INSERT_BIN:
            conn.sendall(f"{OK}".encode('utf-8'))
            file_name = conn.recv(1024).decode('utf-8')
            conn.sendall(f"{OK}".encode('utf-8'))

            bin = b''
            end_file = f"{END_FILE}".encode('utf-8')
            while True:
                fragment = conn.recv(1024)
                if end_file in fragment:
                    bin += fragment.split(end_file)[0]
                    break
                bin += fragment

            response = self.handle_insert_bin(file_name, bin)
            conn.sendall(response.encode('utf-8'))


        elif option == DELETE_BIN:
            response = self.handle_delete_bin(data[1])


        elif option == RETRIEVE_BIN:
            file_name = data[1]
            file_bin = self.database.retrieve_bin(file_name)

            conn.sendall(file_bin)
            conn.sendall(f"{END_FILE}".encode('utf-8'))



        if response:
            response = response.encode('utf-8')
            conn.sendall(response)
        conn.close()

    
    def start_data_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.data_port))
            s.listen(10)

            while True:
                conn, addr = s.accept()
                data = conn.recv(1024).decode('utf-8').split(',')

                threading.Thread(target=self.request_data_handler, args=(conn, addr, data)).start()






    ############################### HANDLERS ###############################
    def handle_insert_tag(self, tag: str):
        tag_hash = getShaRepr(tag)
        owner = self.lookup(tag_hash)
        # I am owner
        if owner.id == self.id:
            if self.database.owns_tag(tag):
                return "OK,Tag already exists"
            else:
                self.database.store_tag(tag, self.succ.ip, self.pred.ip if self.pred else None)
        # I am not owner, foward
        else:
            response = owner.insert_tag(tag)
            return response
        
    def handle_delete_tag(self, tag: str):
        tag_hash = getShaRepr(tag)
        owner = self.lookup(tag_hash)
        # I am owner
        if owner.id == self.id:
            if not self.database.owns_tag(tag):
                return "OK,Key does not exists"
            else:
                self.database.delete_tag(tag, self.succ.ip, self.pred.ip if self.pred else None)
                return "OK,Data deleted"
        # I am not owner
        else:
            response = owner.delete_tag(tag)
            return response

    def handle_append_file(self, tag: str, file_name: str):
        tag_hash = getShaRepr(tag)
        owner = self.lookup(tag_hash)
        # I am owner
        if owner.id == self.id:
            self.database.append_file(tag, file_name, self.succ.ip, self.pred.ip if self.pred else None)
            return "OK,Data appended"
        # I am not owner
        else:
            response = owner.append_file(tag, file_name)
            return response

    def handle_remove_file(self, tag: str, file_name: str):
        tag_hash = getShaRepr(tag)
        owner = self.lookup(tag_hash)

        # I am owner
        if owner.id == self.id:
            self.database.remove_file(tag, file_name, self.succ.ip, self.pred.ip if self.pred else None)
            return "OK,Data removed"
        # I am not owner
        else:
            response = owner.remove_file(tag, file_name)
            return response
        
    def handle_retrieve_tag(self, tag: str):
        return self.database.retrieve_tag(tag)
        
        

    def handle_insert_file(self, file_name: str):
        file_name_hash = getShaRepr(file_name)
        owner = self.lookup(file_name_hash)
        # I am owner
        if owner.id == self.id:
            if self.database.owns_file(file_name):
                return "OK,File already exists"
            else:
                self.database.store_file(file_name, self.succ.ip, self.pred.ip if self.pred else None)
                return "OK,Data inserted"
        # I am not owner, foward
        else:
            response = owner.insert_file(file_name)
            return response
        
    def handle_delete_file(self, file_name: str):
        file_name_hash = getShaRepr(file_name)
        owner = self.lookup(file_name_hash)
        # I am owner
        if owner.id == self.id:
            if not self.database.owns_file(file_name):
                return "OK,Key does not exists"
            else:
                self.database.delete_file(file_name, self.succ.ip, self.pred.ip if self.pred else None)
                return "OK,Data deleted"
        # I am not owner
        else:
            response = owner.delete_file(file_name)
            return response
        
    def handle_append_tag(self, file_name: str, tag: str):
        file_name_hash = getShaRepr(file_name)
        owner = self.lookup(file_name_hash)
        # I am owner
        if owner.id == self.id:
            self.database.append_tag(file_name, tag, self.succ.ip, self.pred.ip if self.pred else None)
            return "OK,Data appended"
        # I am not owner
        else:
            response = owner.append_tag(file_name, tag)
            return response
        
    def handle_remove_tag(self, file_name: str, tag: str):
        file_name_hash = getShaRepr(file_name)
        owner = self.lookup(file_name_hash)

        # I am owner
        if owner.id == self.id:
            self.database.remove_tag(file_name, tag, self.succ.ip, self.pred.ip if self.pred else None)
            return "OK,Data removed"
        # I am not owner
        else:
            response = owner.remove_tag(file_name, tag)
            return response

    def handle_retrieve_file(self, file_name: str):
        return self.database.retrieve_file(file_name)
        

    def handle_insert_bin(self, file_name: str, bin: bytes):
        file_name_hash = getShaRepr(file_name)
        owner = self.lookup(file_name_hash)

        # I am owner
        if owner.id == self.id:
            self.database.store_bin(file_name, bin, self.succ.ip, self.pred.ip if self.pred else None)
            return "OK,Binary file inserted"
        # I am not owner
        else:
            response = owner.insert_bin(file_name, bin)
            return response
        
    def handle_delete_bin(self, file_name: str):
        file_name_hash = getShaRepr(file_name)
        owner = self.lookup(file_name_hash)

        # I am owner
        if owner.id == self.id:
            self.database.delete_bin(file_name, self.succ.ip, self.pred.ip if self.pred else None)
            return "OK,Binary file deleted"
        # I am not owner
        else:
            response = owner.delete_bin(file_name)
            return response
    ###########################################################################
