import socket
import time
from broadcast import *

# broadcast messages
DEFAULT = 0
ELECTION = 1
OK = 2
LEADER = 3

PORT = 8255

ELECTION_SYMBOL = "🔷"

class LeaderElection:

    def __init__(self) -> None:
        self.in_election = False
        self.leader = None
        self.its_me = False
        self.id = str(socket.gethostbyname(socket.gethostname()))

    
    ######## Access functions ########
    def leader_lost(self):                  # to notify leader loss
        self.leader = None

    def get_leader(self):
        return self.leader
    
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

                # this node broadcast
                threading.Thread(target=self._broadcast_msg, args=(f"{ELECTION}")).start()

            elif self.in_election:
                counter += 1
                if counter == 4:
                    
                    # after waiting 2 seconds, if there is no leader still, then i'm leader
                    if not self.leader or self._bully(self.id, self.leader):
                        print(f"[{ELECTION_SYMBOL}] I am the new leader")
                        self.its_me = True
                        self.leader = self.id
                        self.in_election = False
                        threading.Thread(target=self._broadcast_msg, args=(f"{LEADER}")).start()

                    # end election
                    counter = 0
                    self.in_election = False

            # else:
            #     print(f'Leader: {self.leader}')

            time.sleep(0.5)


    def _broadcast_msg(self, msg: str, broadcast_ip='255.255.255.255'):
        bm = "DEFAULT"
        if msg == "1": bm = "ELECTION"
        if msg == "2": bm = "OK"
        if msg == "3": bm = "LEADER"
        print(f"[{ELECTION_SYMBOL}] Broadcasting {bm}")

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.sendto(msg.encode(), (broadcast_ip, PORT))
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

                sender_id = sender[0]
                msg = msg.decode("utf-8")


                if msg.isdigit():
                    msg = int(msg)

                    if msg == ELECTION and not self.in_election:
                        print(f"[{ELECTION_SYMBOL}] ELECTION message received from: {sender_id}")
                        
                        self.in_election = True

                        # say OK to sender
                        if self._bully(self.id, sender_id):
                            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                            s.sendto(f'{OK}'.encode(), (sender_id, PORT))

                        # broadcast ELECTION because i'm alive
                        threading.Thread(target=self._broadcast_msg, args=(f"{ELECTION}")).start()
                                

                    elif msg == OK:
                        print(f"[{ELECTION_SYMBOL}] OK message received from: {sender_id}")

                        # Update the leader constantly
                        if self.leader and self._bully(sender_id, self.leader):
                            self.leader = sender_id
                        self.its_me = False


                    elif msg == LEADER:
                        print(f"[{ELECTION_SYMBOL}] LEADER message received from: {sender_id}")

                        if not self._bully(self.id, sender_id) and (not self.leader or self._bully(sender_id, self.leader)):
                            self.leader = sender_id
                            self.its_me = True if self.leader == self.id else False
                            self.in_election = False


            except Exception as e:
                print(f"[{ELECTION_SYMBOL}]Error in server_thread: {e}")