import threading
import socket
import time

# broadcast messages
DEFAULT = 0
ELECTION = 1
OK = 2
LEADER = 3

PORT = 8080

ELECTION_SYMBOL = "🔷"

class LeaderElection:

    def __init__(self) -> None:
        self.in_election = False
        self.work_done = True
        self.leader = None
        self.its_me = False
        self.id = str(socket.gethostbyname(socket.gethostname()))

    
    ######## Access functions ########
    def leader_lost(self):                  # to notify leader loss
        print("[🔷] Leader lost")
        self.leader = None

    def adopt_leader(self, leader: str):      # for recently joining nodes
        self.leader = leader
        if self.leader == self.id:
            self.its_me = True
    ##################################
    
        
    def loop(self):
        time.sleep(0.5)

        # start server
        t = threading.Thread(target=self._server).start()

        counter = 0
        while True:
            
            if not self.leader and not self.in_election:
                print(f"[{ELECTION_SYMBOL}] Starting leader election...")
                self.in_election = True
                self.work_done = False

                # this node broadcast
                threading.Thread(target=self._broadcast_msg, args=(f"{ELECTION}")).start()

            elif self.in_election and not self.work_done:
                counter += 1
                if counter == 10:
                    # after waiting 5 seconds, if there is no leader still, then i'm leader
                    if not self.leader:
                        print(f"[{ELECTION_SYMBOL}] I am the new leader")
                        threading.Thread(target=self._broadcast_msg, args=(f"{LEADER}")).start()

                    # end election
                    counter = 0

            time.sleep(0.5)


    def _broadcast_msg(self, msg: str, broadcast_ip='255.255.255.255'):
        bm = "DEFAULT"
        if msg == "1": bm = "ELECTION"
        if msg == "2": bm = "OK"
        if msg == "3": bm = "LEADER"
        print(f"[{ELECTION_SYMBOL}] Broadcasting {bm}")

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.sendto(msg.encode('utf-8'), (broadcast_ip, PORT))
        s.close()
    
    def _bully(self, id, otherId):
        return int(id.split('.')[-1]) > int(otherId.split('.')[-1])



    def _server(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(('', PORT))

        while True:
            try:
                msg, sender = s.recvfrom(1024)
                if not msg:
                    continue
                threading.Thread(target=self._handle_request, args=(msg, sender)).start()

            except Exception as e:
                print(f"[{ELECTION_SYMBOL}]Error in server_thread: {e}")



    def _handle_request(self, msg, sender):
        sender_id = sender[0]
        msg = msg.decode("utf-8")

        if msg.isdigit():
            msg = int(msg)

            if msg == ELECTION and not self.in_election:
                print(f"[{ELECTION_SYMBOL}] ELECTION message received from: {sender_id}")

                self.in_election = True
                self.leader = None
                
                # Someone greater than me call ELECTION
                if self._bully(sender_id, self.id):
                    self.work_done = True
                    return
                
                # Someone less than me call ELECTION
                if self._bully(self.id, sender_id):
                    self.work_done = False

                    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                    s.sendto(f'{OK}'.encode('utf-8'), (sender_id, PORT))

                    threading.Thread(target=self._broadcast_msg, args=(f"{ELECTION}")).start()
                        

            elif msg == OK:
                print(f"[{ELECTION_SYMBOL}] OK message received from: {sender_id}")

                if self._bully(sender_id, self.id):
                    self.work_done = True



            elif msg == LEADER:
                print(f"[{ELECTION_SYMBOL}] LEADER message received from: {sender_id}")

                if not self.leader:
                    self.leader = sender_id
                    self.its_me = True if sender_id == self.id else False
                    self.work_done = True
                    self.in_election = False
