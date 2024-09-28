import json
import socket
import threading
import time
from const import *



class Resources:
    def __init__(self, tags: list[str], files: list[str]) -> None:
        self.tags: set = set(tags)
        self.files: set = set(files)

    def use(self, other: 'Resources') -> bool:
        return len(self.tags.intersection(other.tags)) != 0 or len(self.files.intersection(other.files)) != 0
    
    def adopt(self, other: 'Resources') -> None:
        self.tags = self.tags.union(other.tags)
        self.files = self.files.union(other.files)

    def release(self, other: 'Resources') -> None:
        self.tags = self.tags.difference(other.tags)
        self.files = self.files.difference(other.files)
    




class RequestNode:
    def __init__(self, sock: socket.socket, tags: list[str], files: list[str], query_tags: list[str], qt_func, end_func) -> None:
        self.sock = sock
        self.green_light = False

        resource_tags = set(tags).union(set(query_tags))
        resource_files = set(qt_func(query_tags))
        self.resources = Resources(resource_tags, resource_files)

        self.end_func = end_func

    def set_green_light(self):
        self.green_light = True

    def start(self):
        # Wait green light
        while not self.green_light:
            time.sleep(0.5)

        # Green light now
        self.sock.sendall(f"{OK}".encode('utf-8'))

        ack = self.sock.recv(1024).decode('utf-8')
        if ack != f"{END}":
            print("No OK confirmation from operation")

        # Call end function to release resources
        self.end_func(self)

        



class Leader:
    def __init__(self, ip: str, query_tag_function, port: int = DEFAULT_LEADER_PORT):
        self.ip = ip
        self.port = port
        self.query_tag_func = query_tag_function

        self.blocked_resources: Resources = Resources([], [])
        self.waiting_queue: list[RequestNode] = []

        threading.Thread(target=self._start_leader_server, daemon=True).start()

    def block_resources(self, resources: Resources):
        self.blocked_resources.adopt(resources)

    def release_resources(self, resources: Resources):
        self.blocked_resources.release(resources)

    # For new requests
    def join(self, node: RequestNode):
        # Dont use blocked resources case
        if not node.resources.use(self.blocked_resources):
            self.block_resources(node.resources)
            node.set_green_light()

        # Use blocked resources case
        else:
            self.waiting_queue.append(node)


    def end_function(self, node: RequestNode):
        # Release node resources
        self.release_resources(node.resources)

        # Dequeue node in case it is in the queue
        if node in self.waiting_queue:
            self.waiting_queue.remove(node)

        # Check if a node in the queue can act now
        for n in self.waiting_queue:
            if not n.resources.use(self.blocked_resources):
                self.block_resources(n.resources)
                n.set_green_light()


    def _start_leader_server(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.ip, self.port))
            s.listen(10)

            while True:
                conn, _ = s.accept()
                json_str_data = conn.recv(1024).decode('utf-8')

                data = json.loads(json_str_data)

                threading.Thread(target=self.request_leader_handler, args=(conn, data['tags'], data['files'], data['query_tags'])).start()


    def request_leader_handler(self, sock: socket.socket, tags: list[str], files: list[str], query_tags: list[str]):
        request_node = RequestNode(sock, tags, files, query_tags, self.query_tag_func, self.end_function)
        self.join(request_node)
        request_node.start() 
