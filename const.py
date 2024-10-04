# Constants
DEFAULT_NODE_PORT = 8001
DEFAULT_DATA_PORT = 8002
DEFAULT_QUERY_PORT = 8003
DEFAULT_BROADCAST_PORT = 8255
DEFAULT_DB_PORT = 8888
DEFAULT_LEADER_PORT = 8999

# Operation codes in ChordNode
FIND_SUCCESSOR = 1
FIND_PREDECESSOR = 2
GET_SUCCESSOR = 3
GET_PREDECESSOR = 4
NOTIFY = 5
CHECK_NODE = 6
CLOSEST_PRECEDING_FINGER = 7
LOOKUP = 15
STORE_KEY = 8
RETRIEVE_KEY = 9
NOT_ALONE_NOTIFY = 10
REVERSE_NOTIFY = 11
GET_LEADER = 12
DISCOVER = 13
ENTRY_POINT = 14

# Operation codes in DataNode
OK = 0
INSERT_TAG = 1
DELETE_TAG = 2
APPEND_FILE = 3
REMOVE_FILE = 4
RETRIEVE_TAG = 5
# OWNS_TAG = 6

INSERT_FILE = 7
DELETE_FILE = 8
APPEND_TAG = 9
REMOVE_TAG = 10
RETRIEVE_FILE = 11
OWNS_FILE = 12

INSERT_BIN = 13
DELETE_BIN = 14
RETRIEVE_BIN = 15
END = 100
END_FILE = 200

FALSE = 0
TRUE = 1

# Operation codes in Database
PULL_REPLICATION = 2
PUSH_DATA = 4
FETCH_REPLICA = 8

# Replicated predecesor
REPLICATE_PRED_STORE_TAG = 11
REPLICATE_PRED_APPEND_FILE = 12
REPLICATE_PRED_DELETE_TAG = 13
REPLICATE_PRED_REMOVE_FILE = 14

REPLICATE_PRED_STORE_FILE = 21
REPLICATE_PRED_APPEND_TAG = 22
REPLICATE_PRED_DELETE_FILE = 23
REPLICATE_PRED_REMOVE_TAG = 24

REPLICATE_PRED_STORE_BIN = 30
REPLICATE_PRED_DELETE_BIN = 31

# Replicated succesor
REPLICATE_SUCC_STORE_TAG = 111
REPLICATE_SUCC_APPEND_FILE = 121
REPLICATE_SUCC_DELETE_TAG = 131
REPLICATE_SUCC_REMOVE_FILE = 141

REPLICATE_SUCC_STORE_FILE = 211
REPLICATE_SUCC_APPEND_TAG = 221
REPLICATE_SUCC_DELETE_FILE = 231
REPLICATE_SUCC_REMOVE_TAG = 241

REPLICATE_SUCC_STORE_BIN = 301
REPLICATE_SUCC_DELETE_BIN = 311
