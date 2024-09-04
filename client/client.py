import json
import os
import socket
import threading

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






class Client:
    def __init__(self, target_ip=None, target_port=None):
        self.target_ip = target_ip if target_ip else '172.17.0.2'
        self.target_port = target_port if target_port else 8003

        self.start()

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


    def start(self):
        info = f"""=COMMANDS===================================
=> {bcolors.OKBLUE}add           {bcolors.ENDC}<file-list>  <tag-list>  <=
=> {bcolors.OKBLUE}delete        {bcolors.ENDC}<tag-query>              <=
=> {bcolors.OKBLUE}list          {bcolors.ENDC}<tag-query>              <=
=> {bcolors.OKBLUE}add-tags      {bcolors.ENDC}<tag-query>  <tag-list>  <=
=> {bcolors.OKBLUE}delete-tags   {bcolors.ENDC}<tag-query>  <tag-list>  <=
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

            user_input = user_input.split(" ")
            user_input = [x for x in user_input if x != ""]

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

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.target_ip, self.target_port))

                    # Send operation
                    s.sendall('add'.encode())

                    # Wait for OK
                    ack = s.recv(1024).decode()
                    if ack != f"{OK}": raise Exception("Negative ACK")

                    # Send each file
                    for i in range(len(files_name)):
                        # Send name
                        s.sendall(files_name[i].encode())

                        # Wait for OK
                        ack = s.recv(1024).decode()
                        if ack != f"{OK}": raise Exception("Negative ACK")

                        # Send bin and END_FILE
                        s.sendall(files_bin[i])
                        s.sendall(f"{END_FILE}".encode())

                        # Wait for OK
                        ack = s.recv(1024).decode()
                        if ack != f"{OK}": raise Exception("Negative ACK")

                    s.sendall(f"{END}".encode())

                    # Wait for OK
                    ack = s.recv(1024).decode()
                    if ack != f"{OK}": raise Exception("Negative ACK")

                    # Send tags
                    s.sendall(tags.encode())

                    # Wait response
                    response = s.recv(1024).decode()
                    response = json.loads(response)
                    s.close()
                    self.show_results(response)



            elif cmd == "delete":
                if len(params) != 1: 
                    self.display_error(f"'delete' command require 1 param but {len(params)} were given")
                    continue

                tags_query = params[0]

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.target_ip, self.target_port))

                    # Send operation
                    s.sendall('delete'.encode())

                    # Wait for OK
                    ack = s.recv(1024).decode()
                    if ack != f"{OK}": raise Exception("Negative ACK")

                    # Send query tags
                    s.sendall(tags_query.encode())

                    # Wait response
                    response = s.recv(1024).decode()
                    response = json.loads(response)
                    s.close()
                    self.show_results(response)

            
            elif cmd == "list":
                if len(params) != 1: 
                    self.display_error(f"'list' command require 1 param but {len(params)} were given")
                    continue

                tags_query = params[0]
                
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.target_ip, self.target_port))

                    # Send operation
                    s.sendall('list'.encode())

                    # Wait for OK
                    ack = s.recv(1024).decode()
                    if ack != f"{OK}": raise Exception("Negative ACK")

                    # Send query tags
                    s.sendall(tags_query.encode())

                    # Wait response
                    response = s.recv(1024).decode()
                    response = json.loads(response)
                    s.close()
                    self.show_list(response)


            elif cmd == "add-tags":
                if len(params) != 2: 
                    self.display_error(f"'add-tags' command require 2 params but {len(params)} were given")
                    continue

                tags_query = params[0]
                tags = params[1]

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.target_ip, self.target_port))

                    # Send operation
                    s.sendall('add-tags'.encode())

                    # Wait for OK
                    ack = s.recv(1024).decode()
                    if ack != f"{OK}": raise Exception("Negative ACK")

                    # Send query tags
                    s.sendall(tags_query.encode())

                    # Wait for OK
                    ack = s.recv(1024).decode()
                    if ack != f"{OK}": raise Exception("Negative ACK")

                    # Send tags
                    s.sendall(tags.encode())

                    # Wait response
                    response = s.recv(1024).decode()
                    response = json.loads(response)
                    s.close()
                    self.show_results(response)


            elif cmd == "delete-tags":
                if len(params) != 2: 
                    self.display_error(f"'delete-tags' command require 2 params but {len(params)} were given")
                    continue

                tags_query = params[0]
                tags = params[1]

                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((self.target_ip, self.target_port))

                    # Send operation
                    s.sendall('delete-tags'.encode())

                    # Wait for OK
                    ack = s.recv(1024).decode()
                    if ack != f"{OK}": raise Exception("Negative ACK")

                    # Send query tags
                    s.sendall(tags_query.encode())

                    # Wait for OK
                    ack = s.recv(1024).decode()
                    if ack != f"{OK}": raise Exception("Negative ACK")

                    # Send tags
                    s.sendall(tags.encode())

                    # Wait response
                    response = s.recv(1024).decode()
                    response = json.loads(response)
                    s.close()
                    self.show_results(response)

            else:
                self.display_error("Command not found")
                continue

            print("")


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




if __name__ == "__main__":
    Client()