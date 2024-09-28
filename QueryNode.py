import json
import os
import sys
import socket
import threading
import ipaddress
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

        in_election = False if self.election.leader else True

        # Check leader is alive
        if not in_election:
            leader_ref = ChordNodeReference(self.election.leader)
            if not leader_ref.check_node():
                self.election.leader_lost()
                in_election = True

        # Wait up to 10 seconds for leader election
        wait_time = 10
        while in_election:
            if wait_time == 0:
                print("[*] Request waiting for leader election to continue")

            in_election = False if self.election.leader else True
            time.sleep(1)
            wait_time -= 1
            if wait_time == 0:
                return False
            
        
        leader_ip = self.election.leader
        leader_port = DEFAULT_LEADER_PORT

        # Send request
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((leader_ip, leader_port))
            
            # Send data
            packed_permission_request = self._pack_permission_request(tags, files_names, query_tags)
            s.sendall(packed_permission_request)

            # Wait permission
            permission = s.recv(1024).decode('utf-8')
            if permission != f"{OK}":
                raise Exception(f"No permision was send, leader sent: {permission}")
            
            # Invoke callback function
            callback()

            # Send END of operation
            s.sendall(f"{END}".encode('utf-8'))

        return True
            


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
        
        success = self._request_with_permission(tags, files_names, [], callback=callback_func)
        if not success:
            response['msg'] = "Failed to send request"
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

        success = self._request_with_permission([], [], query_tags, callback=callback_func)
        if not success:
            response['msg'] = "Failed to send request"
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

        success = self._request_with_permission([], [], query_tags, callback=callback_func)
        response['msg'] = f"{len(response['files_name'])} files retrieved"
        if not success:
            response['msg'] = "Failed to send request"
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

        success = self._request_with_permission(tags, [], query_tags, callback=callback_func)
        if not success:
            response['msg'] = "Failed to send request"
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

        success = self._request_with_permission(tags, [], query_tags, callback=callback_func)
        if not success:
            response['msg'] = "Failed to send request"
        return response


    def _query_download(self, query_tags: list[str]):
        response: dict = {}
        response['files_name'] = []
        response['bins'] = []

        def callback_func():
            files_to_download = self.tag_query(query_tags)
            for file_name in files_to_download:
                response['files_name'].append(file_name)
                bin = self.download(file_name)
                response['bins'].append(bin)

        success = self._request_with_permission([], [], query_tags, callback=callback_func)
        if not success:
            response['msg'] = "Failed to send request"
        return response


    def _query_inspect_tag(self, tag: str):
        response: dict = {}
        response['file_names'] = []
        response['tag'] = tag

        def callback_func():
            files_by_tag = self.tag_query([tag])
            response['file_names'] = files_by_tag
        
        success = self._request_with_permission([tag], [], [], callback=callback_func)
        response ['msg'] = f"{len(response['file_names'])} files retrieved"
        if not success:
            response ['msg'] = "Failed to send request"
        return response


    def _query_inspect_file(self, file_name: str):
        response: dict = {}
        response['file_name'] = file_name
        response['tags'] = []

        def callback_func():
            tags_by_file = self.inspect(file_name)
            response['tags'] = tags_by_file
        
        success = self._request_with_permission([], [file_name], [], callback=callback_func)
        response ['msg'] = f"{len(response['tags'])} files retrieved"
        if not success:
            response ['msg'] = "Failed to send request"
        return response



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
            # Receive operation
            operation = client_socket.recv(1024).decode('utf-8')
            
            print(f"[*] {client_addr[0]} requested {operation}")

            # Send ACK if operation is correct
            if operation in {'add', 'delete', 'list', 'add-tags', 'delete-tags', 'download', 'inspect-tag', 'inspect-file'}:
                client_socket.sendall(f"{OK}".encode('utf-8'))
            else:
                client_socket.sendall(f"Unrecognized operation: {operation}".encode('utf-8'))
                return
            
            response = {}

            if operation == 'add':
                files_names = []
                files_bins = []

                while True:
                    file_name = client_socket.recv(1024).decode('utf-8')
                    if file_name == f"{END}":
                        break
                    
                    # Send file name received ACK
                    client_socket.sendall(f"{OK}".encode('utf-8'))

                    file_bin = b''
                    end_file = f"{END_FILE}".encode('utf-8')
                    while True:
                        fragment = client_socket.recv(1024)
                        if end_file in fragment:
                            file_bin += fragment.split(end_file)[0]
                            break
                        else:
                            file_bin += fragment
                    
                    # Send file bin received ACK
                    client_socket.sendall(f"{OK}".encode('utf-8'))

                    files_names.append(file_name)
                    files_bins.append(file_bin)

                client_socket.sendall(f"{OK}".encode('utf-8'))

                tags = client_socket.recv(1024).decode('utf-8').split(';')

                response = self._query_add(files_names, files_bins, tags)


            elif operation == 'delete':
                query_tags = client_socket.recv(1024).decode('utf-8').split(';')
                response = self._query_delete(query_tags)
            

            elif operation == 'list':
                query_tags = client_socket.recv(1024).decode('utf-8').split(';')
                response = self._query_list(query_tags)


            elif operation == 'add-tags':
                query_tags = client_socket.recv(1024).decode('utf-8').split(';')
                client_socket.sendall(f"{OK}".encode('utf-8'))

                tags = client_socket.recv(1024).decode('utf-8').split(';')

                response = self._query_add_tags(query_tags, tags)


            elif operation == 'delete-tags':
                query_tags = client_socket.recv(1024).decode('utf-8').split(';')
                client_socket.sendall(f"{OK}".encode('utf-8'))

                tags = client_socket.recv(1024).decode('utf-8').split(';')

                response = self._query_delete_tags(query_tags, tags)
            

            elif operation == 'download':
                query_tags = client_socket.recv(1024).decode('utf-8').split(';')
                file_resp = self._query_download(query_tags)
                file_names = file_resp['files_name']
                file_bins = file_resp['bins']

                for i in range(len(file_names)):
                    client_socket.sendall(file_names[i].encode('utf-8'))

                    ack = client_socket.recv(1024).decode('utf-8')
                    if ack != f"{OK}": raise Exception("Negative ACK")

                    client_socket.sendall(file_bins[i])
                    client_socket.sendall(f"{END_FILE}".encode('utf-8'))

                    ack = client_socket.recv(1024).decode('utf-8')
                    if ack != f"{OK}": raise Exception("Negative ACK")

                client_socket.sendall(f"{END}".encode('utf-8'))

                # Wait for OK
                ack = client_socket.recv(1024).decode('utf-8')
                if ack != f"{OK}": raise Exception("Negative ACK")
                
                return


            elif operation == 'inspect-tag':
                tag = client_socket.recv(1024).decode('utf-8')
                response = self._query_inspect_tag(tag)


            elif operation == 'inspect-file':
                file_name = client_socket.recv(1024).decode('utf-8')
                response = self._query_inspect_file(file_name)


            response_str = json.dumps(response)
            client_socket.sendall(str(response_str).encode('utf-8'))


    def _pack_permission_request(self, tags: list[str], files_names: list[str], query_tags: list[str]) -> bytes:
        data = {}
        data['tags'] = tags
        data['files'] = files_names
        data['query_tags'] = query_tags
        return json.dumps(data).encode('utf-8')




if __name__ == "__main__":
    # Get current IP
    ip = socket.gethostbyname(socket.gethostname())


    # First node case
    if len(sys.argv) == 1:

        # Create node
        node = QueryNode(ip)
        print(f"[IP]: {ip}")
        node.join()

    # Join node cases
    elif len(sys.argv) in [2, 3]:
        flag = sys.argv[1]

        if flag == "-c":
            # Connect using self discovery
            if len(sys.argv) == 2:
                target_ip = SelfDiscovery(ip).find()

            # Connect using especific ip addres
            else:
                target_ip = sys.argv[2]
                try:
                    ipaddress.ip_address(target_ip)
                except:
                    raise Exception(f"{target_ip} cannot be interpreted as an IP address")

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
