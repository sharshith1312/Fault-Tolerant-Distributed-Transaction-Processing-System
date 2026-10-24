"""
Author: sharshith1312
Course: CSE535 - Distributed Systems
Multi-Cluster Sharded Paxos Configuration
"""

# ==================== CLUSTER CONFIGURATION ====================
NUM_CLUSTERS = 3
NODES_PER_CLUSTER = 3
TOTAL_NODES = NUM_CLUSTERS * NODES_PER_CLUSTER

# Calculate quorum for each cluster
F = (NODES_PER_CLUSTER - 1) // 2
QUORUM_SIZE = F + 1

CLIENT_COUNT = 9000

# ==================== CLUSTER TOPOLOGY ====================
# Assign nodes to clusters sequentially
# e.g., NUM_CLUSTERS=3, NODES_PER_CLUSTER=3:
#   Cluster 1: Nodes 1, 2, 3
#   Cluster 2: Nodes 4, 5, 6
#   Cluster 3: Nodes 7, 8, 9

def get_cluster_id(node_id):
    return ((node_id - 1) // NODES_PER_CLUSTER) + 1

# Generate cluster assignments dynamically
NODE_TO_CLUSTER = {
    node_id: get_cluster_id(node_id) 
    for node_id in range(1, TOTAL_NODES + 1)
}

# Reverse mapping: {cluster_id: [node_ids]}
CLUSTER_TO_NODES = {}
for node_id, cluster_id in NODE_TO_CLUSTER.items():
    if cluster_id not in CLUSTER_TO_NODES:
        CLUSTER_TO_NODES[cluster_id] = []
    CLUSTER_TO_NODES[cluster_id].append(node_id)

# Sort node lists for consistency
for cluster_id in CLUSTER_TO_NODES:
    CLUSTER_TO_NODES[cluster_id].sort()

# ==================== SHARD MAPPING (Client ID Ranges) ====================
CLIENTS_PER_CLUSTER = CLIENT_COUNT // NUM_CLUSTERS
SHARD_RANGES = {}
for cluster_id in range(1, NUM_CLUSTERS + 1):
    start_id = (cluster_id - 1) * CLIENTS_PER_CLUSTER + 1
    end_id = cluster_id * CLIENTS_PER_CLUSTER
    SHARD_RANGES[cluster_id] = (start_id, end_id)


CLIENT_TO_CLUSTER_OVERRIDE = {}

def get_cluster_for_client(client_id):
    if client_id in CLIENT_TO_CLUSTER_OVERRIDE:
        return CLIENT_TO_CLUSTER_OVERRIDE[client_id]
    for cluster_id, (start, end) in SHARD_RANGES.items():
        if start <= client_id <= end:
            return cluster_id
    raise ValueError(f"Client ID {client_id} out of range")

def get_original_cluster_for_client(client_id):
    for cluster_id, (start, end) in SHARD_RANGES.items():
        if start <= client_id <= end:
            return cluster_id
    raise ValueError(f"Client ID {client_id} out of range")

def set_client_cluster(client_id, cluster_id):
    CLIENT_TO_CLUSTER_OVERRIDE[client_id] = cluster_id

def reset_client_mappings():
    CLIENT_TO_CLUSTER_OVERRIDE.clear()

def get_nodes_in_cluster(cluster_id):
    return CLUSTER_TO_NODES.get(cluster_id, [])

# ==================== NETWORK ADDRESSES ====================
# Node addresses: {node_id: "host:port"}
NODE_ADDRESSES = {i: f"localhost:{50050 + i}" for i in range(1, TOTAL_NODES + 1)}

# ==================== TIMING PARAMETERS ====================
HEARTBEAT_INTERVAL = 0.1  
TP_WAIT_DURATION = 1.0
CLIENT_RETRY = 0.5 

BASE_TIMEOUT = 0.2

def _generate_timer_timeouts():
    timeouts = {}
    for node_id in range(1, TOTAL_NODES + 1):
        # Position within cluster (0-indexed): 0, 1, 2, ...
        position_in_cluster = (node_id - 1) % NODES_PER_CLUSTER
        # Timeout increases with position: 0.2, 0.4, 0.6, ...
        timeouts[node_id] = BASE_TIMEOUT * (position_in_cluster + 1)
    return timeouts

timer_timeouts = _generate_timer_timeouts()

def get_randomized_timeout(node_id):
    return timer_timeouts.get(node_id, 0.5)

# ==================== PERSISTENCE ====================
DB_DIR = "db"
CHECKPOINT_INTERVAL = 100
RESHARD_FILE = "db/resharded_clients.json"

# SQLite database path per node
def get_db_path(node_id):
    return f"{DB_DIR}/node_{node_id}.db"

USE_SQLITE_WAL_MODE = True

# ==================== TWO-PHASE COMMIT CONFIGURATION ====================
LOCK_TIMEOUT = 5.0      
TWOPC_TIMEOUT = 3.0       
TWOPC_RETRY_COUNT = 5
KEEP_WAL_AFTER_COMMIT = False

# ==================== CLIENT CONFIGURATION ====================
INITIAL_AMOUNT = 10

# ==================== CONSISTENCY LEVELS ====================
CONSISTENCY_LINEARIZABLE = "linearizable"
CONSISTENCY_SEQUENTIAL = "sequential"  
CONSISTENCY_PRAM = "pram"

# Default consistency level
DEFAULT_CONSISTENCY = CONSISTENCY_LINEARIZABLE