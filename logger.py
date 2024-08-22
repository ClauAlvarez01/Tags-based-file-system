import os

class Logger():
    def __init__(self, node):
        self.node = node
        self.filename = f"logs/{self.node.id}.txt"

        # Check if the file exists; if not, create it
        if not os.path.exists(self.filename):
            with open(self.filename, 'w') as file:
                pass

    # Overwrite the file with the self.node.succ value
    def refresh(self):
        with open(self.filename, 'w') as file:
            id = str(self.node.id)
            succ = str(self.node.succ).split(',')[0]
            pred = str(self.node.pred).split(',')[0]
            log = f"""
IPv4     : {self.node.ip}
Id       : ({id[len(id)-3: len(id)]}) - {self.node.id}
Succ     : ({succ[len(id)-3: len(id)]}) - {succ}
Pred     : ({pred[len(id)-3: len(id)]}) - {pred}
"""
            file.write(log)
