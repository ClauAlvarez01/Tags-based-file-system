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
            succ = str(self.node.succ).split(',')
            succ_id = succ[0]
            succ_ip = succ[1]
            pred = str(self.node.pred).split(',')
            pred_id = pred[0] if str(self.node.pred) != "None" else ""
            pred_ip = pred[1] if str(self.node.pred) != "None" else ""
            lead = str(self.node.election.leader).split(',')[0] if str(self.node.election.leader) != "None" else ""
            log = f"""
IPv4     : {self.node.ip}
Id       : ({id[len(id)-3: len(id)]}) - {self.node.id} - {self.node.ip}
Succ     : ({succ_id[len(succ_id)-3: len(succ_id)]}) - {succ_id} - {succ_ip}
Pred     : ({pred_id[len(pred_id)-3: len(pred_id)]}) - {pred_id} - {pred_ip}
Leader   : {lead}
"""
            file.write(log)
