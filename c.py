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

        # threading.Thread(target=self.temp, daemon=True).start()
        
    # def temp(self):
    #     if self.ip == '172.17.0.2':
    #         time.sleep(10)
    #         self._query_add(["archivo1"], [b'Contenido de archivo1'], ['azul'])


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
            


    # poner esta funcion en un hilo pq se va a quedar parada esperando repuesta hasta q se complete
    def _query_add(self, files_names: list[str], files_bins: list[bytes], tags: list[str]):
        # ['failed']: A list of files name that failed to insert
        # ['msg']: A response message
        response: dict = {}
        response['failed'] = []
        response['msg'] = "Operation completed, but some insertions failed"

        def callback_func():
            # Copy every file into system
            for i in range(len(files_names)):
                file_name = files_names[i]
                file_bin = files_bins[i]
                success = self.copy(file_name, file_bin, tags)
                if not success:
                    response['failed'].append(file_name)
        
        if len(response['failed']) == 0: response['msg'] = "All insertions completed successfully"

        self._request_with_permission(tags, files_names, [], callback=callback_func)
        return response


    def _query_delete(self, query_tags: list[str]): 
        # ['failed']: A list of files name that failed to delete
        # ['msg']: A response message
        response: dict = {}
        response['failed'] = []
        response['msg'] = "Operation completed, but some deletions failed"

        def callback_func():
            files_to_delete = self.tag_query(query_tags)
            for file in files_to_delete:
                success = self.remove(file)
                if not success:
                    response['failed'].append(file)

        if len(response['failed']) == 0: response['msg'] = "All deletions completed successfully"

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

        response['msg'] = f"{len(response['files_name'])} files retrieved"

        self._request_with_permission([], [], query_tags, callback=callback_func)
        return response


    def _query_add_tags(self, query_tags: list[str], tags: list[str]):
        # ['failed']: A list of files name that failed to be edited with tags
        # ['msg']: A response message
        response: dict = {}
        response['failed'] = []
        response['msg'] = "Operation completed, but some files failed to add tags"

        def callback_func():
            files_to_edit = self.tag_query(query_tags)
            for file in files_to_edit:
                success = self.add_tags(file, tags)
                if not success:
                    response['failed'].append(file)

        if len(response['failed']) == 0: response['msg'] = "All tags added successfully"

        self._request_with_permission(tags, [], query_tags, callback=callback_func)
        return response


    def _query_delete_tags(self, query_tags: list[str], tags: list[str]):
        # ['failed']: A list of files name that failed to be edited with tags
        # ['msg']: A response message
        response: dict = {}
        response['failed'] = []
        response['msg'] = "Operation completed, but some files failed to delete tags"

        def callback_func():
            files_to_edit = self.tag_query(query_tags)
            for file in files_to_edit:
                success = self.delete_tags(file, tags)
                if not success:
                    response['failed'].append(file)

        if len(response['failed']) == 0: response['msg'] = "All tags deleted successfully"

        self._request_with_permission(tags, [], query_tags, callback=callback_func)
        return response





    # Server
    def start_query_server(self):
        # Iniciar aqui el servidor, q debe escuchar las consultas del cliente
        # en dependencia de la consulta, llamar a la funcion de arriba encargada
        pass



    

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
