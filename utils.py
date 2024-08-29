import hashlib
import socket
from const import OK

# Function to hash a string using SHA-1 and return its integer representation
def getShaRepr(data: str):
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)


def inbetween(k: int, start: int, end: int) -> bool:
        if start < end:
            return start < k <= end
        else:  # The interval wraps around 0
            return start < k or k <= end
        

def send_2(first_msg: str, second_msg: str, target_ip: str, target_port: int):
        """Sends two messages to target ip, always waiting for OK ack"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((target_ip, target_port))
            s.sendall(first_msg.encode())
        
            ack = s.recv(1024).decode()
            if ack != f"{OK}":
                raise Exception("ACK negativo")
            
            s.sendall(second_msg.encode())
            
            ack = s.recv(1024).decode()
            if ack != f"{OK}":
                raise Exception("ACK negativo")