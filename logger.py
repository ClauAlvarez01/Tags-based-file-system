import os
import time
import threading

class Logger():
    def __init__(self, node):
        self.node = node
        self.filename = f"logs/{self.node.ip}.txt"

        # Check if the file exists; if not, create it
        if not os.path.exists(self.filename):
            with open(self.filename, 'w') as file:
                pass

        threading.Thread(target=self.refresh, daemon=True).start()

    def format_data(self, list: dict):
        result = ""
        for k, v in list.items():
            result += f"\t{k} - {v}\n"
            # result += f"\t\t{str(k)[:5]}...{str(k)[len(str(k))-3:]} - {v}\n"
        return result
    

    # Overwrite the file with the self.node.succ value
    def refresh(self):

        while True:
            time.sleep(2)
            with open(self.filename, 'w') as file:
                id = str(self.node.id)
                succ = str(self.node.succ).split(',')
                succ_id = succ[0]
                succ_ip = succ[1]
                pred = str(self.node.pred).split(',')
                predpred = str(self.node.predpred).split(',')
                pred_id = pred[0] if str(self.node.pred) != "None" else ""
                pred_ip = pred[1] if str(self.node.pred) != "None" else ""
                predpred_id = predpred[0] if str(self.node.predpred) != "None" else ""
                predpred_ip = predpred[1] if str(self.node.predpred) != "None" else ""
                lead = str(self.node.election.leader).split(',')[0] if str(self.node.election.leader) != "None" else ""
                log = f"""
IPv4     : {self.node.ip}
Id       : ({id[len(id)-3: len(id)]}) - {self.node.id} - {self.node.ip}
Succ     : ({succ_id[len(succ_id)-3: len(succ_id)]}) - {succ_id} - {succ_ip}
Pred     : ({pred_id[len(pred_id)-3: len(pred_id)]}) - {pred_id} - {pred_ip}
PredPred : ({predpred_id[len(predpred_id)-3: len(predpred_id)]}) - {predpred_id} - {predpred_ip}
Leader   : {lead}

------------------------ Owned -------------------------
🔖 Tags:
{self.format_data(self.node.database.tags)}
📁 Files:
{self.format_data(self.node.database.files)}

----------------- Replicated Predecesor -----------------
🔖 Tags:
{self.format_data(self.node.database.replicated_pred_tags)}
📁 Files:
{self.format_data(self.node.database.replicated_pred_files)}
"""
                file.write(log)
