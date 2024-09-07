import os
import sys
import json
import time
import socket
import threading
import ipaddress

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

RESOURCES_PATH = "resources/"
OK = 0
END = 100
END_FILE = 200







DISCOVER = 13
ENTRY_POINT = 14
DEFAULT_BROADCAST_PORT = 8255
SELF_DISC_SYMBOL = "🔎"


class SelfDiscovery:
    def __init__(self, ip: str, port: int = 8200) -> None:
        self.ip = ip
        self.port = port

        self.target_ip = None

        threading.Thread(target=self._recv, daemon=True).start()

    
    def find(self) -> str:
        print(f"[{SELF_DISC_SYMBOL}] Self Discovery started")

        self._send(f'{DISCOVER},{self.ip},{self.port}')

        wait_time = 12 # 3 seconds
        while not self.target_ip:
            time.sleep(0.25)
            wait_time -= 1
            if wait_time == 0:
                raise Exception("Timeout: Looks like there is no Chord Node in this network")
        
        print(f"[{SELF_DISC_SYMBOL}] discovered {self.target_ip}")
        return self.target_ip
        

    def _send(self, message: str):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.sendto(message.encode('utf-8'), ('255.255.255.255', DEFAULT_BROADCAST_PORT))


    def _recv(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.port))
            s.listen(5)

            while True:
                conn, addr = s.accept()

                # Refuse self messages
                if addr[0] == self.ip:
                    continue

                data = conn.recv(1024).decode('utf-8').split(',')
                option = int(data[0])

                if option == ENTRY_POINT:
                    self.target_ip = data[1]
                    conn.close()
                    s.close()
                    break










class Client:
    def __init__(self, target_ip=None, target_port=8003):
        self.target_ip = target_ip
        self.target_port = target_port

        self.downloads_path = 'client/downloads'

        self.start()

    


    def start(self):
        info = f"""=COMMANDS===================================
=> {bcolors.OKBLUE}add           {bcolors.ENDC}<file-list>  <tag-list>  <=
=> {bcolors.OKBLUE}delete        {bcolors.ENDC}<tag-query>              <=
=> {bcolors.OKBLUE}list          {bcolors.ENDC}<tag-query>              <=
=> {bcolors.OKBLUE}add-tags      {bcolors.ENDC}<tag-query>  <tag-list>  <=
=> {bcolors.OKBLUE}delete-tags   {bcolors.ENDC}<tag-query>  <tag-list>  <=
=> {bcolors.OKBLUE}download      {bcolors.ENDC}<tag-query>              <=
=> {bcolors.OKBLUE}inspect-tag   {bcolors.ENDC}<tag>                    <=
=> {bcolors.OKBLUE}inspect-file  {bcolors.ENDC}<file-name>              <=
=> {bcolors.OKBLUE}info          {bcolors.ENDC}                         <=
=> {bcolors.OKBLUE}exit          {bcolors.ENDC}                         <=
============================================
{bcolors.WARNING}⚠ Use (;) separator for lists of elements
example {bcolors.ENDC} <tag-list> {bcolors.OKBLUE} as {bcolors.ENDC} red;blue
{bcolors.WARNING}⚠ {bcolors.OKGREEN} green text {bcolors.WARNING}means succeded
{bcolors.WARNING}⚠ {bcolors.FAIL} red text   {bcolors.WARNING}means failed
{bcolors.ENDC}============================================"""
        print(info)
        

        while True:
            user_input = input(bcolors.OKBLUE + "> " + bcolors.ENDC)

            if ',' in user_input:
                self.display_error("Error: No comma allowed in files name or tags")
                continue

            user_input = user_input.split(" ")
            user_input = [x for x in user_input if x != ""]

            if len(user_input) == 0:
                self.display_error("Error: Empty input")
                continue

            if len(user_input) == 1 and user_input[0] == "exit": break
            if len(user_input) == 1 and user_input[0] == "info":
                print(info)
                continue

            cmd = user_input[0]
            params = user_input[1:]


            if cmd == "add":
                if len(params) != 2: 
                    self.display_error(f"'add' command require 2 params but {len(params)} were given")
                    continue

                files_name = params[0].split(';')
                files_bin, correct = self.load_bins(files_name)

                if not correct: continue

                tags = params[1]

                try:

                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((self.target_ip, self.target_port))

                        # Send operation
                        s.sendall('add'.encode('utf-8'))

                        # Wait for OK
                        ack = s.recv(1024).decode('utf-8')
                        if ack != f"{OK}": raise Exception("Negative ACK")

                        # Send each file
                        for i in range(len(files_name)):
                            # Send name
                            s.sendall(files_name[i].encode('utf-8'))

                            # Wait for OK
                            ack = s.recv(1024).decode('utf-8')
                            if ack != f"{OK}": raise Exception("Negative ACK")

                            # Send bin and END_FILE
                            s.sendall(files_bin[i])
                            s.sendall(f"{END_FILE}".encode('utf-8'))

                            # Wait for OK
                            ack = s.recv(1024).decode('utf-8')
                            if ack != f"{OK}": raise Exception("Negative ACK")

                        s.sendall(f"{END}".encode('utf-8'))

                        # Wait for OK
                        ack = s.recv(1024).decode('utf-8')
                        if ack != f"{OK}": raise Exception("Negative ACK")

                        # Send tags
                        s.sendall(tags.encode('utf-8'))

                        # Wait response
                        response = s.recv(1024).decode('utf-8')
                        response = json.loads(response)
                        s.close()
                        self.show_results(response)
                except:
                    self.display_error("The operation could not be completed successfully.")


            elif cmd == "delete":
                if len(params) != 1: 
                    self.display_error(f"'delete' command require 1 param but {len(params)} were given")
                    continue

                tags_query = params[0]

                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((self.target_ip, self.target_port))

                        # Send operation
                        s.sendall('delete'.encode('utf-8'))

                        # Wait for OK
                        ack = s.recv(1024).decode('utf-8')
                        if ack != f"{OK}": raise Exception("Negative ACK")

                        # Send query tags
                        s.sendall(tags_query.encode('utf-8'))

                        # Wait response
                        response = s.recv(1024).decode('utf-8')
                        response = json.loads(response)
                        s.close()
                        self.show_results(response)
                except:
                    self.display_error("The operation could not be completed successfully.")

            
            elif cmd == "list":
                if len(params) != 1: 
                    self.display_error(f"'list' command require 1 param but {len(params)} were given")
                    continue

                tags_query = params[0]
                
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((self.target_ip, self.target_port))

                        # Send operation
                        s.sendall('list'.encode('utf-8'))

                        # Wait for OK
                        ack = s.recv(1024).decode('utf-8')
                        if ack != f"{OK}": raise Exception("Negative ACK")

                        # Send query tags
                        s.sendall(tags_query.encode('utf-8'))

                        # Wait response
                        response = s.recv(1024).decode('utf-8')
                        response = json.loads(response)
                        s.close()
                        self.show_list(response)
                except:
                    self.display_error("The operation could not be completed successfully.")


            elif cmd == "add-tags":
                if len(params) != 2: 
                    self.display_error(f"'add-tags' command require 2 params but {len(params)} were given")
                    continue

                tags_query = params[0]
                tags = params[1]

                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((self.target_ip, self.target_port))

                        # Send operation
                        s.sendall('add-tags'.encode('utf-8'))

                        # Wait for OK
                        ack = s.recv(1024).decode('utf-8')
                        if ack != f"{OK}": raise Exception("Negative ACK")

                        # Send query tags
                        s.sendall(tags_query.encode('utf-8'))

                        # Wait for OK
                        ack = s.recv(1024).decode('utf-8')
                        if ack != f"{OK}": raise Exception("Negative ACK")

                        # Send tags
                        s.sendall(tags.encode('utf-8'))

                        # Wait response
                        response = s.recv(1024).decode('utf-8')
                        response = json.loads(response)
                        s.close()
                        self.show_results(response)
                except:
                    self.display_error("The operation could not be completed successfully.")


            elif cmd == "delete-tags":
                if len(params) != 2: 
                    self.display_error(f"'delete-tags' command require 2 params but {len(params)} were given")
                    continue

                tags_query = params[0]
                tags = params[1]

                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((self.target_ip, self.target_port))

                        # Send operation
                        s.sendall('delete-tags'.encode('utf-8'))

                        # Wait for OK
                        ack = s.recv(1024).decode('utf-8')
                        if ack != f"{OK}": raise Exception("Negative ACK")

                        # Send query tags
                        s.sendall(tags_query.encode('utf-8'))

                        # Wait for OK
                        ack = s.recv(1024).decode('utf-8')
                        if ack != f"{OK}": raise Exception("Negative ACK")

                        # Send tags
                        s.sendall(tags.encode('utf-8'))

                        # Wait response
                        response = s.recv(1024).decode('utf-8')
                        response = json.loads(response)
                        s.close()
                        self.show_results(response)
                except:
                    self.display_error("The operation could not be completed successfully.")


            elif cmd == 'download':
                if len(params) != 1: 
                    self.display_error(f"'download' command require 1 param but {len(params)} were given")
                    continue

                tags_query = params[0]
                
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((self.target_ip, self.target_port))
                        print("Downloading...")

                        # Send operation
                        s.sendall('download'.encode('utf-8'))

                        # Wait for OK
                        ack = s.recv(1024).decode('utf-8')
                        if ack != f"{OK}": raise Exception("Negative ACK")

                        # Send query tags
                        s.sendall(tags_query.encode('utf-8'))

                        # Wait response
                        while True:
                            file_name = s.recv(1024).decode('utf-8')
                            if file_name == f"{END}":
                                break
                        
                            # Send file name received ACK
                            s.sendall(f"{OK}".encode('utf-8'))

                            file_content = b''
                            end_file = f"{END_FILE}".encode('utf-8')
                            while True:
                                fragment = s.recv(1024)
                                if end_file in fragment:
                                    file_content += fragment.split(end_file)[0]
                                    break
                                else:
                                    file_content += fragment
                        
                            # Send file bin received ACK
                            s.sendall(f"{OK}".encode('utf-8'))

                            #Guardar archivos en txt 
                            self.save_file(file_name, file_content)


                        print(f"{bcolors.OKGREEN}Download completed{bcolors.ENDC}")
                        s.sendall(f"{OK}".encode('utf-8'))
                        s.close()
                except:
                    self.display_error("The operation could not be completed")


            elif cmd == 'inspect-tag':
                if len(params) != 1: 
                    self.display_error(f"'inspect-tag' command require 1 param but {len(params)} were given")
                    continue

                tag = params[0]

                if len(tag) != 1: 
                    self.display_error(f"'inspect-tag' command is only valid for retrieving file names for a specific tag")
                    continue
                
                tag: str = tag[0]

                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((self.target_ip, self.target_port))

                        # Send operation
                        s.sendall('inspect-tag'.encode('utf-8'))

                        # Wait for OK
                        ack = s.recv(1024).decode('utf-8')
                        if ack != f"{OK}": raise Exception("Negative ACK")

                        # Send tag
                        s.sendall(tag.encode('utf-8'))

                        # Wait response
                        response = s.recv(1024).decode('utf-8')
                        response = json.loads(response)
                        s.close()
                        self.show_tag_file_relationship(response, 'files_by_tag')
                except:
                    self.display_error("The operation could not be completed successfully.")


            elif cmd == 'inspect-file':
                if len(params) != 1: 
                    self.display_error(f"'inspect-file' command require 1 param but {len(params)} were given")
                    continue

                file_name = params[0]

                if len(file_name) != 1: 
                    self.display_error(f"'inspect-file' command is only valid for retrieving tags for a specific file name")
                    continue
                
                file_name: str = file_name[0]

                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.connect((self.target_ip, self.target_port))

                        # Send operation
                        s.sendall('inspect-file'.encode('utf-8'))

                        # Wait for OK
                        ack = s.recv(1024).decode('utf-8')
                        if ack != f"{OK}": raise Exception("Negative ACK")

                        # Send tag
                        s.sendall(file_name.encode('utf-8'))

                        # Wait response
                        response = s.recv(1024).decode('utf-8')
                        response = json.loads(response)
                        s.close()
                        self.show_tag_file_relationship(response, 'tags_by_file')
                except:
                    self.display_error("The operation could not be completed successfully.")



            else:
                self.display_error("Command not found")
                continue

            print("")

    def check_no_comma(self):
        pass

    def display_error(self, msg: str):
        print(bcolors.FAIL + msg + bcolors.ENDC)

    def load_bins(self, names: list[str]) -> tuple[list[bytes], bool]:
        bins: list[bytes] = []

        # Check files existency
        for file_name in names:
            if not os.path.isfile(RESOURCES_PATH + file_name):
                self.display_error(f"{file_name} file not found")
                return [], False

        for file_name in names:
            content = []
            with open(RESOURCES_PATH + file_name, 'rb') as file:
                while True:
                    data = file.read(1024)
                    if not data:
                        break
                    content.append(data)
                bin_content = b''.join(content)
                bins.append(bin_content)

        return bins, True

    def show_list(self, data: dict):
        msg: str = data['msg']
        files_name: list = data['files_name']
        tags: list[list] = data['tags']
        
        print(msg)
        for i in range(len(files_name)):
            print(f"{bcolors.HEADER}{files_name[i]}{bcolors.ENDC} : {tags[i]}")

    def show_results(self, data: dict):
        msg: str = data['msg']
        failed: list[str] = data['failed']
        failed_msg: list[list] = data['failed_msg']
        succeded: list[str] = data['succeded']

        print(msg)

        for i in range(len(succeded)):
            print(f"{bcolors.OKGREEN}{succeded[i]}{bcolors.ENDC}")

        for i in range(len(failed)):
            print(f"{bcolors.FAIL}{failed[i]}{bcolors.ENDC} \n  Reason: {failed_msg[i]}")


    def save_file(self, file_name: str, content: bytes):
        downloads_folder = os.path.join(os.path.dirname(__file__), 'downloads')
        
        file_path = os.path.join(downloads_folder, file_name)
        
        with open(file_path, 'wb') as file:
            file.write(content)


    def show_tag_file_relationship(self, data: dict, mode: str):
        if mode == 'files_by_tag':
            file_names: list = data['file_names']
            tag: str = data['tag']

            if not file_names:
                print(f"{bcolors.WARNING}No files found for the tag '{tag}'.{bcolors.ENDC}")
                return

            print(f"{bcolors.HEADER}Files associated with tag '{tag}':{bcolors.ENDC}")
            for file_name in file_names:
                print(f"{bcolors.OKBLUE}{file_name}{bcolors.ENDC}")

        elif mode == 'tags_by_file':
            file_name: str = data['file_name']
            tags: list = data['tags']

            if not tags:
                print(f"{bcolors.WARNING}No tags found for the file '{file_name}'.{bcolors.ENDC}")
                return

            print(f"{bcolors.HEADER}Tags associated with file '{file_name}':{bcolors.ENDC}")
            for tag in tags:
                print(f"{bcolors.OKBLUE}{tag}{bcolors.ENDC}")

        else:
            print(f"{bcolors.FAIL}Invalid mode: {mode}{bcolors.ENDC}")


if __name__ == "__main__":

    ip = socket.gethostbyname(socket.gethostname())

    # Connect to any node
    if len(sys.argv) == 1:
        target_ip = SelfDiscovery(ip).find()
        Client(target_ip=target_ip)

    # Connect to specific IP addres
    elif len(sys.argv) == 2:
        target_ip = sys.argv[1]
        try:
            ipaddress.ip_address(target_ip)
        except:
            raise Exception(f"{target_ip} cannot be interpreted as an IP address")

        Client(target_ip=target_ip)
        