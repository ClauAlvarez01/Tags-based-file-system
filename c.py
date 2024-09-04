import json
import os
import sys
import socket
import threading
import time
from const import *
from leader import Leader
from DataNode import DataNode
from ChordNodeReference import ChordNodeReference
from self_discovery import SelfDiscovery


class QueryNode(DataNode):
    def __init__(self, ip: str):
        super().__init__(ip)

        Leader(ip, self.tag_query)

        threading.Thread(target=self.start_query_server, daemon=True).start()



    def _request_with_permission(self, tags, files_names, query_tags, callback):
        leader_ip = self.election.get_leader()
        leader_port = DEFAULT_LEADER_PORT

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((leader_ip, leader_port))
            
            # Send data
            packed_permission_request = self._pack_permission_request(tags, files_names, query_tags)
            s.sendall(packed_permission_request)

            # Wait permission
            permission = s.recv(1024).decode()
            if permission != f"{OK}":
                raise Exception(f"No permision was send, leader sent: {permission}")
            
            # Invoke callback function
            callback()

            # Send END of operation
            s.sendall(f"{END}".encode())
            


    def _query_add(self, files_names: list[str], files_bins: list[bytes], tags: list[str]):
        # ['failed']: A list of files name that failed to insert
        # ['succeded']: A list of files name that succeded
        # ['failed_msg']: A list of fail messages associated with failed index in ['failed']
        # ['msg']: A response message
        response: dict = {}
        response['failed'] = []
        response['succeded'] = []
        response['failed_msg'] = []
        response['msg'] = "Action completed"

        def callback_func():
            # Copy every file into system
            for i in range(len(files_names)):
                file_name = files_names[i]
                file_bin = files_bins[i]
                success, fail_msg = self.copy(file_name, file_bin, tags)
                if not success:
                    response['failed'].append(file_name)
                    response['failed_msg'].append(fail_msg)
                else:
                    response['succeded'].append(file_name)
        
        self._request_with_permission(tags, files_names, [], callback=callback_func)
        return response


    def _query_delete(self, query_tags: list[str]): 
        # ['failed']: A list of files name that failed to delete
        # ['succeded']: A list of files name that succeded
        # ['failed_msg']: A list of fail messages associated with failed index in ['failed']
        # ['msg']: A response message
        response: dict = {}
        response['failed'] = []
        response['succeded'] = []
        response['failed_msg'] = []
        response['msg'] = "Action completed"

        def callback_func():
            files_to_delete = self.tag_query(query_tags)
            for file in files_to_delete:
                success, fail_msg = self.remove(file)
                if not success:
                    response['failed'].append(file)
                    response['failed_msg'].append(fail_msg)
                else:
                    response['succeded'].append(file)

        self._request_with_permission([], [], query_tags, callback=callback_func)
        return response


    def _query_list(self, query_tags: list[str]):
        # ['files_name']: A list of files name retrieved from query tags
        # ['tags']: A list of elements, where each element is list of associated tags to file name in the same index
        # ['msg']: A response message
        response: dict = {}
        response['files_name'] = []
        response['tags'] = []

        def callback_func():
            files_to_list = self.tag_query(query_tags)
            for file in files_to_list:
                tags = self.inspect(file)
                response['files_name'].append(file)
                response['tags'].append(tags)

        self._request_with_permission([], [], query_tags, callback=callback_func)
        response['msg'] = f"{len(response['files_name'])} files retrieved"
        return response


    def _query_add_tags(self, query_tags: list[str], tags: list[str]):
        # ['failed']: A list of files name that failed to be edited with tags
        # ['succeded']: A list of files name that succeded
        # ['failed_msg']: A list of fail messages associated with failed index in ['failed']
        # ['msg']: A response message
        response: dict = {}
        response['failed'] = []
        response['succeded'] = []
        response['failed_msg'] = []
        response['msg'] = "Action completed"

        def callback_func():
            files_to_edit = self.tag_query(query_tags)
            for file in files_to_edit:
                success, fail_msg = self.add_tags(file, tags)
                if not success:
                    response['failed'].append(file)
                    response['failed_msg'].append(fail_msg)
                else:
                    response['succeded'].append(file)

        self._request_with_permission(tags, [], query_tags, callback=callback_func)
        return response


    def _query_delete_tags(self, query_tags: list[str], tags: list[str]):
        # ['failed']: A list of files name that failed to be edited with tags
        # ['succeded']: A list of files name that succeded
        # ['failed_msg']: A list of fail messages associated with failed index in ['failed']
        # ['msg']: A response message
        response: dict = {}
        response['failed'] = []
        response['succeded'] = []
        response['failed_msg'] = []
        response['msg'] = "Action completed"

        def callback_func():
            files_to_edit = self.tag_query(query_tags)
            for file in files_to_edit:
                success, fail_msg = self.delete_tags(file, tags)
                if not success:
                    response['failed'].append(file)
                    response['failed_msg'].append(fail_msg)
                else:
                    response['succeded'].append(file)

        self._request_with_permission(tags, [], query_tags, callback=callback_func)
        return response

    def _query_download(self, query_tags: list[str]):
        pass



    # Server
    def start_query_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, DEFAULT_QUERY_PORT))
            s.listen(10)
            
            while True:
                client_socket, client_address = s.accept()

                threading.Thread(target=self.handle_request, args=(client_socket, client_address), daemon=True).start()

                

    def handle_request(self, client_socket: socket.socket, client_addr):
        with client_socket:
            print(f"Request by {client_addr}")
            
            # Receive operation
            operation = client_socket.recv(1024).decode()

            # Send ACK if operation is correct
            if operation in {'add', 'delete', 'list', 'add-tags', 'delete-tags'}:
                client_socket.sendall(f"{OK}".encode())
            else:
                client_socket.sendall(f"Unrecognized operation: {operation}".encode())
                return
            
            response = {}

            if operation == 'add':
                files_names = []
                files_bins = []

                while True:
                    file_name = client_socket.recv(1024).decode()
                    if file_name == f"{END}":
                        break
                    
                    # Send file name received ACK
                    client_socket.sendall(f"{OK}".encode())

                    file_bin = b''
                    while True:
                        fragment = client_socket.recv(1024)
                        if fragment.decode() == f"{END_FILE}":
                            break
                        else:
                            file_bin += fragment
                    
                    # Send file bin received ACK
                    client_socket.sendall(f"{OK}".encode())

                    files_names.append(file_name)
                    files_bins.append(file_bin)

                client_socket.sendall(f"{OK}".encode())

                tags = client_socket.recv(1024).decode().split(';')

                response = self._query_add(files_names, files_bins, tags)


            elif operation == 'delete':
                query_tags = client_socket.recv(1024).decode().split(';')
                response = self._query_delete(query_tags)
            

            elif operation == 'list':
                query_tags = client_socket.recv(1024).decode().split(';')
                response = self._query_list(query_tags)


            elif operation == 'add-tags':
                query_tags = client_socket.recv(1024).decode().split(';')
                client_socket.sendall(f"{OK}".encode())

                tags = client_socket.recv(1024).decode().split(';')

                response = self._query_add_tags(query_tags, tags)


            elif operation == 'delete-tags':
                query_tags = client_socket.recv(1024).decode().split(';')
                client_socket.sendall(f"{OK}".encode())

                tags = client_socket.recv(1024).decode().split(';')

                response = self._query_delete_tags(query_tags, tags)
            
            
            response_str = json.dumps(response)
            client_socket.sendall(str(response_str).encode())


    def _pack_permission_request(self, tags: list[str], files_names: list[str], query_tags: list[str]) -> bytes:
        data = {}
        data['tags'] = tags
        data['files'] = files_names
        data['query_tags'] = query_tags
        return json.dumps(data).encode()




if __name__ == "__main__":
    # Get current IP
    ip = socket.gethostbyname(socket.gethostname())


    # First node case
    if len(sys.argv) == 1:

        # Create node
        node = QueryNode(ip)
        print(f"[IP]: {ip}")

        node.join()


    # Join node case
    elif len(sys.argv) == 2:
        flag = sys.argv[1]

        if flag == "-c":
            target_ip = SelfDiscovery(ip).find()

            # Create node
            node = QueryNode(ip)
            print(f"[IP]: {ip}")

            node.join(ChordNodeReference(target_ip))


        else:
            raise Exception(f"Missing flag: {flag} does not exist")

    else:
        raise Exception("Incorrect params")

    while True:
        pass
