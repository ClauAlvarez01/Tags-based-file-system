import hashlib
import socket
from const import OK, END, END_FILE
from typing import Dict, List

# Function to hash a string using SHA-1 and return its integer representation
def getShaRepr(data: str):
    return int(hashlib.sha1(data.encode('utf-8')).hexdigest(), 16)

# Function to check if n id is between two other id's in chord ring
def inbetween(k: int, start: int, end: int) -> bool:
    if start < end:
        return start < k <= end
    else:  # The interval wraps around 0
        return start < k or k <= end
        
# Function to send 2 messages using a socket and waiting OK confirmation
def send_2(first_msg: str, second_msg: str, target_ip: str, target_port: int):
        """Sends two messages to target ip, always waiting for OK ack"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((target_ip, target_port))
            s.sendall(first_msg.encode('utf-8'))
        
            ack = s.recv(1024).decode('utf-8')
            if ack != f"{OK}":
                raise Exception("ACK negativo")
            
            s.sendall(second_msg.encode('utf-8'))
            
            ack = s.recv(1024).decode('utf-8')
            if ack != f"{OK}":
                raise Exception("ACK negativo")

# Function to open a socket and send a binary file
def send_bin(op: str, file_name: str, bin: bytes, target_ip: str, target_port: int, end_msg: bool = False):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((target_ip, target_port))

        s.sendall(op.encode('utf-8'))

        ack = s.recv(1024).decode('utf-8')
        if ack != f"{OK}":
            raise Exception("ACK negativo")
        
        s.sendall(file_name.encode('utf-8'))

        ack = s.recv(1024).decode('utf-8')
        if ack != f"{OK}":
            raise Exception("ACK negativo")
        
        s.sendall(bin)

        if end_msg:
            s.sendall(f"{END_FILE}".encode('utf-8'))

        return s.recv(1024)
    
# Function to send multiple binary files using a specified socket
def send_bins(s: socket.socket, files_to_send: dict, path: str):
    for k, _ in files_to_send.items():
        file_path = f"{path}/{k}"

        s.sendall(k.encode('utf-8'))

        ack = s.recv(1024).decode('utf-8')
        if ack != f"{OK}":
            raise Exception("ACK negativo")

        with open(file_path, 'rb') as file:
            while True:
                data = file.read(1024)
                
                if not data:
                    break
                s.sendall(data)
            
                ack = s.recv(1024).decode('utf-8')
                if ack != f"{OK}":
                    raise Exception("ACK negativo")
        
            s.sendall(f"{END_FILE}".encode('utf-8'))

            ack = s.recv(1024).decode('utf-8')
            if ack != f"{OK}":
                raise Exception("ACK negativo")


    s.sendall(f"{END}".encode('utf-8'))

    ack = s.recv(1024).decode('utf-8')
    if ack != f"{OK}":
        raise Exception("ACK negativo")
    
# Function to receive multiple binary files using a specified socket
def recv_write_bins(s: socket.socket, dest_dir: str):
    while True:
        data = s.recv(1024)
        if data.decode('utf-8') == f"{END}":
            break

        file_name = data.decode('utf-8')
        s.sendall(f"{OK}".encode('utf-8'))

        file_content = []
        while True:
            data = s.recv(1024)
            if data.decode('utf-8') == f"{END_FILE}":
                file_bin = b''.join(file_content)

                # Save file binary
                with open(f"{dest_dir}/{file_name}", 'wb') as file:
                    file.write(file_bin)

                file_content = []
                break
            file_content.append(data)
            s.sendall(f"{OK}".encode('utf-8'))

        s.sendall(f"{OK}".encode('utf-8'))
        
    s.sendall(f"{OK}".encode('utf-8'))