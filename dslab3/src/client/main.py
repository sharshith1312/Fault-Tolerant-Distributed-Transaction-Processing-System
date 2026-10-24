"""
Author: sharshith1312
Course: CSE535 - Distributed Systems
"""

import grpc
import time
import subprocess
import sys
import os
import csv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List
import paxos_pb2
import paxos_pb2_grpc
from shared import config

class Client:
    def __init__(self, client_id):
        self.client_id = client_id
        self.timestamp = 0
        self.stubs = {}
        self.cluster_leaders = {
            cluster_id: config.CLUSTER_TO_NODES[cluster_id][0] 
            for cluster_id in range(1, config.NUM_CLUSTERS + 1)
        }
        self._update_stubs()
        self.is_request_waiting = False

    def _update_stubs(self):
        self.stubs = {}
        for nid, addr in config.NODE_ADDRESSES.items():
            try:
                channel = grpc.insecure_channel(addr)
                self.stubs[nid] = paxos_pb2_grpc.PaxosServiceStub(channel)
            except Exception as e:
                print(f"Warning: Could not create stub for node {nid}: {e}")
    

    def send_balance_request(self, client_id: int, consistency_level: str = None):
        if consistency_level is None:
            consistency_level = config.DEFAULT_CONSISTENCY
            
        cluster_id = config.get_cluster_for_client(client_id)
        nodes_in_cluster = config.get_nodes_in_cluster(cluster_id)
        
        request = paxos_pb2.BalanceRequest(client_id=client_id)
        
        if consistency_level == config.CONSISTENCY_LINEARIZABLE:
            for _ in range(3):
                for node_id in nodes_in_cluster:
                    if node_id not in self.stubs:
                        continue
                    try:
                        response = self.stubs[node_id].HandleBalanceRequest(request, timeout=1)
                        if not response.success and "idle" in response.message.lower():
                            continue
                        if not response.success and response.message.lower() in ["not leader"]:
                            time.sleep(config.CLIENT_RETRY)
                            continue
                        return True, response.balance
                    except grpc.RpcError:
                        continue
            return False, "No Leader Available"
        
        elif consistency_level == config.CONSISTENCY_SEQUENTIAL:
            print(f"  [Sequential] Reading from any replica - operations ordered consistently")
            for node_id in nodes_in_cluster:
                if node_id not in self.stubs:
                    continue
                try:
                    response = self.stubs[node_id].GetDatabase(paxos_pb2.Empty(), timeout=1)
                    if response.balances:
                        for balance_entry in response.balances:
                            if balance_entry.client_id == client_id:
                                print(f"  [Sequential] Read from Node {node_id}")
                                return True, balance_entry.balance
                except grpc.RpcError:
                    continue
            return False, "No nodes available"
        
        elif consistency_level == config.CONSISTENCY_PRAM:
            print(f"  [PRAM] Reading from any replica - may see stale data")
            for node_id in nodes_in_cluster:
                if node_id not in self.stubs:
                    continue
                try:
                    response = self.stubs[node_id].GetDatabase(paxos_pb2.Empty(), timeout=1)
                    if response.balances:
                        for balance_entry in response.balances:
                            if balance_entry.client_id == client_id:
                                print(f"  [PRAM] Read from Node {node_id} (may be stale)")
                                return True, balance_entry.balance
                except grpc.RpcError:
                    continue
            return False, "No nodes available"
        
        else:
            print(f"  [Unknown consistency level: {consistency_level}, using linearizable]")
            return self.send_balance_request(client_id, config.CONSISTENCY_LINEARIZABLE)

    def send_request(self, transaction_str, max_retries=3, retry_delay=0.2):
        self.timestamp += 1
        self.is_request_waiting = True
        
        request = paxos_pb2.ClientRequest(
            client_id=self.client_id,
            timestamp=self.timestamp,
            transaction=transaction_str
        )
        
        parts = transaction_str.strip("()").split(',')
        if len(parts) != 3:
            print(f"Invalid transaction format: {transaction_str}")
            self.is_request_waiting = False
            return (False, "Invalid format")
        
        try:
            sender = int(parts[0].strip())
        except ValueError:
            print(f"Invalid transaction format: {transaction_str}")
            self.is_request_waiting = False
            return (False, "Invalid format")
        
        sender_cluster = config.get_cluster_for_client(sender)
        nodes_in_cluster = config.get_nodes_in_cluster(sender_cluster)
        believed_leader = self.cluster_leaders.get(sender_cluster)
        
        # Step 1: Try believed leader first (no retries for IDLE/no leader)
        if believed_leader and believed_leader in self.stubs:
            try:
                reply = self.stubs[believed_leader].HandleClientRequest(request, timeout=2.0)
                if reply and reply.success:
                    self.is_request_waiting = False
                    return (True, reply.message)
                elif reply:
                    msg_lower = reply.message.lower()
                    # IDLE or no leader → immediately go to broadcast
                    if "idle" in msg_lower or "no leader" in msg_lower:
                        self.cluster_leaders.pop(sender_cluster, None)
                        # Fall through to broadcast
                    else:
                        # Other failure (insufficient funds, etc.)
                        self.is_request_waiting = False
                        return (False, reply.message)
            except grpc.RpcError:
                self.cluster_leaders.pop(sender_cluster, None)
                # Fall through to broadcast
        
        # Step 2: Broadcast to all nodes in cluster
        for node_id in nodes_in_cluster:
            if node_id == believed_leader:
                continue
            if node_id not in self.stubs:
                continue
            
            for attempt in range(max_retries):
                try:
                    time.sleep(config.CLIENT_RETRY)
                    reply = self.stubs[node_id].HandleClientRequest(request, timeout=2.0)
                    
                    if reply and reply.success:
                        self.cluster_leaders[sender_cluster] = reply.leader_id
                        self.is_request_waiting = False
                        return (True, reply.message)
                    elif reply:
                        msg_lower = reply.message.lower()
                        # Node is IDLE → move to next node immediately
                        if "idle" in msg_lower:
                            break
                        
                        # Leader syncing or no leader → retry
                        if "syncing" in msg_lower or "no leader" in msg_lower:
                            if attempt < max_retries - 1:
                                time.sleep(retry_delay)
                                continue
                            break
                        
                        # Other failure (insufficient funds, aborted, etc.)
                        self.is_request_waiting = False
                        return (False, reply.message)
                except grpc.RpcError:
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    break
        
        self.is_request_waiting = False
        print("Transaction failed: No leaders available")
        return (False, "No leaders available")

class PaxosMultiClientController:
    def __init__(self):
        self.server_processes: Dict[int, subprocess.Popen] = {}
        self.clients: Dict[int, Client] = {}  # {client_id: Client}
        
        self.default_client = Client(1)
        self.stubs = self.default_client.stubs 
        self.cluster_leaders = {}  # {cluster_id: leader_node_id}
        
        # Performance tracking
        self.perf_start_time = 0
        self.perf_end_time = 0
        self.perf_transaction_count = 0
        self.perf_success_count = 0
        self.perf_latencies = []
        
        self.perf_readonly_count = 0
        self.perf_readonly_success = 0
        
        self.transaction_history = []
        
        print(f"[PaxosMultiClientManager] Initialized controller for 9-node cluster system")


    def start_servers_in_terminals(self, server_ids: List[int] = None):
        if server_ids is None:
            server_ids = list(range(1, config.TOTAL_NODES + 1))  # All 9 nodes
        
        for server_id in server_ids:
            try:
                src_path = f"{os.getcwd()}/src"
                python_cmd = f"PYTHONPATH={src_path} {sys.executable} -c 'from server.main import serve; serve({server_id})'"
                terminal_script = f'tell app "Terminal" to do script "cd {os.getcwd()} && {python_cmd}"'
                terminal_cmd = ['osascript', '-e', terminal_script]
                proc = subprocess.Popen(terminal_cmd, stdout=subprocess.PIPE)
                self.server_processes[server_id] = proc
                print(f"Started server S{server_id} (Cluster {config.get_cluster_id(server_id)}) in new terminal")
                time.sleep(0.3)
            except Exception as e:
                print(f"Failed to start server S{server_id} in terminal: {e}")



    def set_node_idle(self, server_id: int):
        if server_id in self.stubs:
            try:
                request = paxos_pb2.IdleRequest(is_idle=True)
                response = self.stubs[server_id].SetIdle(request, timeout=5.0)
                if response.success:
                    pass
                else:
                    print(f"Failed to set node S{server_id} to idle: {response.message}")
            except grpc.RpcError as e:
                print(f"Failed to communicate with S{server_id}: {e.details()}")
        else:
            print(f"Server S{server_id} is not available")
    
    def set_node_active(self, server_id: int):
        if server_id in self.stubs:
            try:
                request = paxos_pb2.IdleRequest(is_idle=False)
                response = self.stubs[server_id].SetIdle(request, timeout=5.0)
                if response.success:
                    pass
                else:
                    print(f"Failed to set node S{server_id} to active: {response.message}")
            except grpc.RpcError as e:
                print(f"Failed to communicate with S{server_id}: {e.details()}")
        else:
            print(f"Server S{server_id} is not available")
    
    def print_server_status(self):
        print(f"\n{'='*90}")
        print(f"SERVER STATUS (9-Node Multi-Cluster System)")
        print(f"{'='*90}")
        print(f"{'Node':<8} {'Cluster':<10} {'State':<12} {'Role':<12} {'Leader':<10} {'Ballot':<12}")
        print(f"{'-'*90}")
        
        for node_id in range(1, config.TOTAL_NODES + 1):
            cluster_id = config.get_cluster_id(node_id)
            
            if node_id in self.stubs:
                try:
                    info = self.stubs[node_id].GetNodeInfo(paxos_pb2.Empty(), timeout=2.0)
                    
                    state = "IDLE" if info.is_idle else "ACTIVE"
                    
                    role = info.role
                    leader_info = f"S{info.leader_id}" if info.leader_id > 0 else "None"
                    ballot_info = f"({info.ballot_number},{info.ballot_node_id})"
                    
                    print(f"S{node_id:<7} C{cluster_id:<9} {state:<12} {role:<12} {leader_info:<10} {ballot_info:<12}")
                    
                except grpc.RpcError as e:
                    state = "DOWN"
                    role = "?"
                    leader_info = "?"
                    ballot_info = "?"
                    print(f"S{node_id:<7} C{cluster_id:<9} {state:<12} {role:<12} {leader_info:<10} {ballot_info:<12}")
            else:
                state = "NO STUB"
                role = "?"
                leader_info = "?"
                ballot_info = "?"
                print(f"S{node_id:<7} C{cluster_id:<9} {state:<12} {role:<12} {leader_info:<10} {ballot_info:<12}")
        
        print(f"{'='*90}\n")
    


    def submit_transaction(self, sender: int, receiver: int, amount: float):
        """Submit a transfer transaction (sender and receiver are integer client IDs)"""
        transaction = f"({sender},{receiver},{amount})"
        
        if sender not in self.clients:
            self.clients[sender] = Client(sender)
        
        if self.clients[sender].is_request_waiting:
            print(f"Client {sender} is already processing a request. Please wait.")
            return (False, "Client busy")
        
        # Track latency
        tx_start = time.time()
        result = self.clients[sender].send_request(transaction)
        tx_end = time.time()
        
        # Record for performance stats
        self.perf_latencies.append(tx_end - tx_start)
        self.perf_transaction_count += 1
        if result[0]:
            self.perf_success_count += 1
            self.transaction_history.append((sender, receiver))
        
        return result
    
    def send_balance_request(self, client_id: int, consistency_level: str = None):
        if client_id not in self.clients:
            self.clients[client_id] = Client(client_id)
        
        if consistency_level is None:
            consistency_level = config.DEFAULT_CONSISTENCY
            
        self.perf_readonly_count += 1
        result = self.clients[client_id].send_balance_request(client_id, consistency_level)
        if result[0]:
            self.perf_readonly_success += 1
        return result
    
    def reset_performance(self):
        self.perf_start_time = time.time()
        self.perf_end_time = 0
        self.perf_transaction_count = 0
        self.perf_success_count = 0
        self.perf_latencies = []
        self.perf_readonly_count = 0
        self.perf_readonly_success = 0
    
    def Performance(self):
        if self.perf_end_time == 0:
            self.perf_end_time = time.time()
        
        total_time = self.perf_end_time - self.perf_start_time if self.perf_start_time > 0 else 0
        num_transactions = self.perf_transaction_count
        num_success = self.perf_success_count
        
        if self.perf_latencies:
            avg_latency = sum(self.perf_latencies) / len(self.perf_latencies)
            min_latency = min(self.perf_latencies)
            max_latency = max(self.perf_latencies)
        else:
            avg_latency = 0
            min_latency = 0
            max_latency = 0
        
        total = num_transactions + self.perf_readonly_count
        success = num_success + self.perf_readonly_success
        tps = total / total_time if total_time > 0 else 0
        
        print(f"\n{'='*50}")
        print(f"PERFORMANCE SUMMARY")
        print(f"{'='*50}")
        print(f"Total Transactions: {total}")
        print(f"Successful: {success}")
        print(f"Failed: {total - success}")
        print(f"Total Time: {total_time:.2f}s")
        print(f"TPS: {tps:.2f}")
        print(f"Avg Latency: {avg_latency*1000:.2f}ms")
        print(f"Min Latency: {min_latency*1000:.2f}ms")
        print(f"Max Latency: {max_latency*1000:.2f}ms")
        print(f"{'='*50}\n")
    
    # ==================== SHARD REDISTRIBUTION ====================
    
    def _build_hypergraph(self):
        vertices = set()
        edges = []
        adjacency = {}
        
        for sender, receiver in self.transaction_history:
            vertices.add(sender)
            vertices.add(receiver)
            edges.append((sender, receiver))
            
            if sender not in adjacency:
                adjacency[sender] = set()
            if receiver not in adjacency:
                adjacency[receiver] = set()
            adjacency[sender].add(receiver)
            adjacency[receiver].add(sender)
        
        return vertices, edges, adjacency
    
    def _hypergraph_partition(self, vertices, edges, adjacency, num_partitions=3):
        if not vertices:
            return {1: set(), 2: set(), 3: set()}
        
        partitions = {1: set(), 2: set(), 3: set()}
        vertex_to_partition = {}
        
        for v in vertices:
            current_cluster = config.get_cluster_for_client(v)
            partitions[current_cluster].add(v)
            vertex_to_partition[v] = current_cluster
        
        edge_weights = {}
        for sender, receiver in edges:
            key = tuple(sorted([sender, receiver]))
            edge_weights[key] = edge_weights.get(key, 0) + 1
        
        max_iterations = 100
        improved = True
        iteration = 0
        
        while improved and iteration < max_iterations:
            improved = False
            iteration += 1
            
            for v in vertices:
                current_part = vertex_to_partition[v]
                
                best_part = current_part
                best_gain = -1
                
                connections = {1: 0, 2: 0, 3: 0}
                for neighbor in adjacency.get(v, set()):
                    if neighbor in vertex_to_partition:
                        neighbor_part = vertex_to_partition[neighbor]
                        key = tuple(sorted([v, neighbor]))
                        weight = edge_weights.get(key, 1)
                        connections[neighbor_part] += weight
                
                cross_shard_connections = sum(connections[p] for p in connections if p != current_part)
                if cross_shard_connections == 0:
                    continue
                
                current_cost = cross_shard_connections
                
                for target_part in range(1, config.NUM_CLUSTERS + 1):
                    if target_part == current_part:
                        continue
                    
                    max_size = len(vertices) // num_partitions + len(vertices) // config.NUM_CLUSTERS
                    if len(partitions[target_part]) >= max_size:
                        continue
                    

                    new_cost = sum(connections[p] for p in connections if p != target_part)
                    gain = current_cost - new_cost
                    
                    if gain > best_gain or (gain == best_gain and connections[target_part] > connections[current_part]):
                        best_gain = gain
                        best_part = target_part
                
                if best_part != current_part and best_gain >= 0:
                    partitions[current_part].remove(v)
                    partitions[best_part].add(v)
                    vertex_to_partition[v] = best_part
                    improved = True
        
        return partitions
    
    def _move_data_between_clusters(self, client_id, source_cluster, dest_cluster):
        config.set_client_cluster(client_id, dest_cluster)
        return True
    
    def PrintReshard(self):
        print(f"\n{'='*60}")
        print("SHARD REDISTRIBUTION")
        print(f"{'='*60}")
        print(f"Analyzing {len(self.transaction_history)} transactions...")
        
        if len(self.transaction_history) < 3:
            print("Not enough transaction history for resharding (need at least 5)")
            print(f"{'='*60}\n")
            return []
        
        vertices, edges, adjacency = self._build_hypergraph()
        print(f"Built hypergraph: {len(vertices)} vertices, {len(edges)} edges")
        
        cross_shard_before = 0
        for sender, receiver in edges:
            sender_cluster = config.get_cluster_for_client(sender)
            receiver_cluster = config.get_cluster_for_client(receiver)
            if sender_cluster != receiver_cluster:
                cross_shard_before += 1
        
        print(f"Current cross-shard transactions: {cross_shard_before}/{len(edges)}")
        
        partitions = self._hypergraph_partition(vertices, edges, adjacency)
        
        moves = []
        
        for new_cluster, clients in partitions.items():
            for client_id in clients:
                original_cluster = config.get_original_cluster_for_client(client_id)
                current_cluster = config.get_cluster_for_client(client_id)
                
                if new_cluster != current_cluster:
                    moves.append((client_id, current_cluster, new_cluster))
        
        if not moves:
            print("No resharding needed - current partitioning is optimal")
            print(f"{'='*60}\n")
            return []
        
        print(f"\nMoving {len(moves)} data items:")
        print("-" * 40)
        
        reshard_mapping = {}
        for client_id, source_cluster, dest_cluster in moves:
            print(f"  ({client_id}, c{source_cluster}, c{dest_cluster})")
            self._move_data_between_clusters(client_id, source_cluster, dest_cluster)
            reshard_mapping[client_id] = dest_cluster
        
        import json
        import os
        os.makedirs(os.path.dirname(config.RESHARD_FILE), exist_ok=True)
        with open(config.RESHARD_FILE, 'w') as f:
            json.dump({"moves": reshard_mapping}, f)
        print(f"\nResharding data saved to {config.RESHARD_FILE}")
        
        cross_shard_after = 0
        for sender, receiver in edges:
            sender_cluster = config.get_cluster_for_client(sender)
            receiver_cluster = config.get_cluster_for_client(receiver)
            if sender_cluster != receiver_cluster:
                cross_shard_after += 1
        
        print("-" * 40)
        print(f"Cross-shard transactions: {cross_shard_before} -> {cross_shard_after}")
        print(f"Reduction: {cross_shard_before - cross_shard_after} ({100*(cross_shard_before - cross_shard_after)/max(1,cross_shard_before):.1f}%)")
        print(f"{'='*60}\n")
        
        return moves
    
    def print_balance(self, client_id: int):
        cluster_id = config.get_cluster_for_client(client_id)
        node_ids = config.CLUSTER_TO_NODES[cluster_id]
        
        print(f"\nPrintBalance({client_id}):")
        
        balances = {}
        for node_id in node_ids:
            if node_id in self.stubs:
                try:
                    response = self.stubs[node_id].GetDatabase(paxos_pb2.Empty(), timeout=2.0)
                    if client_id in response.balances:
                        balances[node_id] = response.balances[client_id]
                    else:
                        balances[node_id] = "N/A"
                except grpc.RpcError:
                    balances[node_id] = "N/A"
        
        output_parts = [f"n{node_id} : {balances.get(node_id, 'N/A')}" for node_id in sorted(node_ids)]
        print(", ".join(output_parts))
    
    def print_db(self, clients_to_print=None):
        if clients_to_print is None:
            clients_to_print = self.last_test_set_clients if hasattr(self, 'last_test_set_clients') else set()
        
        if not clients_to_print:
            print(f"\nPrintDB() - No clients to display (no test set executed yet)")
            return
        
        print(f"\nPrintDB() - Balances for clients in last test set:")
        print(f"{'='*80}")
        print(f"Clients: {sorted(clients_to_print)}")
        print(f"{'='*80}")
        
        clients_by_cluster = {1: [], 2: [], 3: []}
        for client_id in clients_to_print:
            cluster_id = config.get_cluster_for_client(client_id)
            clients_by_cluster[cluster_id].append(client_id)
        
        for cluster_id in range(1, config.NUM_CLUSTERS + 1):
            if not clients_by_cluster[cluster_id]:
                continue
                
            node_ids = config.CLUSTER_TO_NODES[cluster_id]
            print(f"\nCluster {cluster_id} (Nodes {node_ids}):")
            
            for node_id in sorted(node_ids):
                if node_id in self.stubs:
                    try:    
                        response = self.stubs[node_id].GetDatabase(paxos_pb2.Empty(), timeout=2.0)
                        balance_strs = []
                        for client_id in sorted(clients_by_cluster[cluster_id]):
                            if client_id in response.balances:
                                balance_strs.append(f"{client_id}:${response.balances[client_id]:.1f}")
                            else:
                                balance_strs.append(f"{client_id}:N/A")
                        
                        if balance_strs:
                            print(f"  Node {node_id}: {', '.join(balance_strs)}")
                    except grpc.RpcError as e:
                        print(f"  Node {node_id}: Error - {e}")
        
        print(f"{'='*80}")
    
    
    
    def _collect_transaction_history(self, num_transactions):
        all_transactions = []
        
        for node_id in range(1, config.TOTAL_NODES + 1):
            if node_id in self.stubs:
                try:
                    log_response = self.stubs[node_id].GetLog(paxos_pb2.Empty(), timeout=5.0)
                    
                    for entry in log_response.entries:
                        if entry.transaction_type in ["INTRA", "CROSS"]:
                            tx_str = entry.request.transaction
                            parts = tx_str.strip('()').split(',')
                            if len(parts) == 3:
                                try:
                                    sender = int(parts[0].strip())
                                    receiver = int(parts[1].strip())
                                    amount = float(parts[2].strip())
                                    all_transactions.append({
                                        'sender': sender,
                                        'receiver': receiver,
                                        'amount': amount,
                                        'seq': entry.sequence_num
                                    })
                                except (ValueError, IndexError):
                                    pass
                except grpc.RpcError:
                    pass
        
        seen = set()
        unique = []
        for tx in all_transactions:
            key = (tx['sender'], tx['receiver'], tx['amount'])
            if key not in seen:
                seen.add(key)
                unique.append(tx)
        
        return unique[-num_transactions:]
    
    def _partition_hypergraph(self, clients, hyperedges, current_assignment):
        
        assignment = current_assignment.copy()
        
        improved = True
        iterations = 0
        
        while improved and iterations < 10:
            improved = False
            iterations += 1
            
            for client in clients:
                current_cluster = assignment[client]
                best_cluster = current_cluster
                best_score = sum(1 for s, r in hyperedges 
                               if s in assignment and r in assignment 
                               and assignment[s] != assignment[r])
                
                for new_cluster in range(1, config.NUM_CLUSTERS + 1):
                    if new_cluster != current_cluster:
                        assignment[client] = new_cluster
                        score = sum(1 for s, r in hyperedges 
                                  if s in assignment and r in assignment 
                                  and assignment[s] != assignment[r])
                        
                        if score < best_score:
                            best_score = score
                            best_cluster = new_cluster
                            improved = True
                        
                        assignment[client] = current_cluster
                
                assignment[client] = best_cluster
        
        return assignment
    
    def submit_concurrent_transactions(self, transactions: List[tuple], max_workers=100, batch_size=50, batch_delay=0.02):
        print(f"\n{'='*80}")
        print(f"CONCURRENT TRANSACTION EXECUTION")
        print(f"Total transactions: {len(transactions)}")
        print(f"Thread pool size: {max_workers}")
        print(f"Batch size: {batch_size}, Batch delay: {batch_delay}s")
        print(f"{'='*80}\n")
        
        results = {
            'total': len(transactions),
            'success': 0,
            'failed': 0,
            'failed_txns': []
        }
        
        results_lock = threading.Lock()
        
        def execute_transaction(idx, sender, receiver, amount):
            thread_id = threading.current_thread().name
            print(f"[Thread-{thread_id}] Transaction {idx+1}: ({sender},{receiver},{amount})")
            
            success, msg = self.submit_transaction(sender, receiver, amount)
            
            if success:
                print(f"[Thread-{thread_id}] Transaction {sender} -> {receiver}: ${amount} - {msg}")
                return True, (sender, receiver, amount)
            else:
                print(f"[Thread-{thread_id}] Transaction {sender} -> {receiver}: ${amount} - {msg}")
                return False, (sender, receiver, amount)
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            all_futures = []
            
            for batch_start in range(0, len(transactions), batch_size):
                batch_end = min(batch_start + batch_size, len(transactions))
                batch = transactions[batch_start:batch_end]
                
                for idx, (sender, receiver, amount) in enumerate(batch, start=batch_start):
                    future = executor.submit(execute_transaction, idx, sender, receiver, amount)
                    all_futures.append(future)
                
                if batch_end < len(transactions):
                    time.sleep(batch_delay)
            
            for future in as_completed(all_futures):
                try:
                    success, txn = future.result()
                    with results_lock:
                        if success:
                            results['success'] += 1
                        else:
                            results['failed'] += 1
                            results['failed_txns'].append(txn)
                except Exception as e:
                    with results_lock:
                        results['failed'] += 1
                    print(f"Exception in transaction: {e}")
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        print(f"\n{'='*80}")
        print(f"CONCURRENT EXECUTION COMPLETED")
        print(f"{'='*80}")
        print(f"Total transactions: {results['total']}")
        print(f"Successful: {results['success']}")
        print(f"Failed: {results['failed']}")
        
        if results['failed_txns']:
            print(f"\nFailed transactions:")
            for sender, receiver, amount in results['failed_txns']:
                print(f"  ({sender},{receiver},{amount})")
        
        print(f"{'='*80}\n")
        
        return results

    def print_f_log(self, server_id: int):
        try:
            if server_id in self.stubs:
                print(f"Requesting log from server S{server_id}")
                response = self.stubs[server_id].GetLog(paxos_pb2.Empty(), timeout=5.0)

                print(f"\n--- LOG [Server S{server_id}] ---")
                if not response.entries:
                    print("  No log entries")
                else:
                    for entry in response.entries:
                        if entry.is_noop:
                            req_str = "NO-OP"
                        else:
                            req_str = f"Client: {entry.request.client_id}, Tx: {entry.request.transaction}"
                        print(f"  Seq {entry.sequence_num}: Ballot({entry.ballot_num.number},{entry.ballot_num.node_id}), Status: {entry.status}, Req: {req_str}")
                print("------------------------\n")
            else:
                print(f"Server S{server_id} is not available")
        except grpc.RpcError as e:
            print(f"Failed to get log from S{server_id}: {e.details()}")
        except Exception as e:
            print(f"Failed to request log from S{server_id}: {e}")

    def print_status(self, sequence_num: int):
        try:
            print(f"Requesting transaction status for sequence {sequence_num}")
            print(f"\n--- TRANSACTION STATUS [Seq {sequence_num}] ---")
            
            for node_id, stub in self.stubs.items():
                try:
                    request = paxos_pb2.StatusRequest(sequence_num=sequence_num)
                    response = stub.GetStatus(request, timeout=5.0)
                    if node_id in response.node_status:
                        status = response.node_status[node_id]
                        message = response.node_status_message.get(node_id, "")
                        if message:
                            print(f"  Node {node_id}: {status} ({message})")
                        else:
                            print(f"  Node {node_id}: {status}")
                    else:
                        print(f"  Node {node_id}: X")
                except grpc.RpcError:
                    print(f"  Node {node_id}: UNREACHABLE")
            print("----------------------------------------\n")
        except Exception as e:
            print(f"Failed to get transaction status: {e}")

    def print_view(self):
        print(f"\n{'='*80}")
        print("NEW-VIEW HISTORY (All Nodes)")
        print(f"{'='*80}")
        
        for node_id in range(1, config.TOTAL_NODES + 1):
            if node_id not in self.stubs:
                continue
            try:
                response = self.stubs[node_id].GetView(paxos_pb2.Empty(), timeout=2.0)
                print(f"\n--- Node {node_id} ---")
                if not response.new_views:
                    print("  No NEW-VIEW messages recorded.")
                else:
                    for i, view in enumerate(response.new_views, 1):
                        ballot = (view.ballot_num.number, view.ballot_num.node_id)
                        print(f"  {i}. NEW-VIEW")
                        print(f"     Ballot: ({ballot[0]}, {ballot[1]})")
                        print(f"     AcceptLog entries: {len(view.accept_log)}")
                        if view.latest_checkpoint and view.latest_checkpoint.sequence_num > 0:
                            print(f"     Checkpoint: seq {view.latest_checkpoint.sequence_num} (transactions 1-{view.latest_checkpoint.sequence_num} checkpointed)")
                        else:
                            print("     Checkpoint: None")
                        print("     Log entries:")
                        for j, entry in enumerate(view.accept_log):
                            if entry.is_noop:
                                print(f"       {j+1}. Seq {entry.sequence_num}: NO-OP")
                            else:
                                print(f"       {j+1}. Seq {entry.sequence_num}: {entry.request.transaction}")
            except grpc.RpcError:
                print(f"\n--- Node {node_id} ---")
                print("  Unavailable")
        
        print(f"\n{'='*80}")


    def crash_current_leader(self):
        """Crash one of the leaders (randomly pick a cluster)"""
        leader_id = None
        
        for node_id in range(1, config.TOTAL_NODES + 1):
            if node_id in self.stubs:
                try:
                    info = self.stubs[node_id].GetNodeInfo(paxos_pb2.Empty(), timeout=2.0)
                    if info.role == 'LEADER' and not info.is_idle:
                        leader_id = node_id
                        break
                except grpc.RpcError:
                    continue
        
        if leader_id:
            cluster_id = config.get_cluster_id(leader_id)
            print(f"[LEADER FAILURE] Setting leader S{leader_id} (Cluster {cluster_id}) to IDLE")
            self.set_node_idle(leader_id)
            return True
        else:
            print(f"[LEADER FAILURE] Could not identify active leader")
            return False
    
    def parse_live_nodes(self, live_nodes_str):
        if not live_nodes_str or live_nodes_str.strip() == '':
            return None
        
        live_nodes_str = live_nodes_str.strip().replace('[', '').replace(']', '')
        nodes = []
        for node in live_nodes_str.split(','):
            node = node.strip()
            if node.startswith('n'):
                try:
                    nodes.append(int(node[1:]))
                except ValueError:
                    pass
        return nodes if nodes else None
    
    def parse_transaction(self, tx_str):
        if not tx_str or tx_str.strip() == '' or tx_str.upper() == 'LF':
            return None
        
        tx_str = tx_str.strip()
        
        if tx_str.upper().startswith('F(') and tx_str.endswith(')'):
            node_id_str = tx_str[2:-1].strip()
            if node_id_str.startswith('n'):
                node_id = int(node_id_str[1:])
            else:
                node_id = int(node_id_str)
            return ('FAILURE', node_id, 0)
        
        if tx_str.upper().startswith('R(') and tx_str.endswith(')'):
            node_id_str = tx_str[2:-1].strip()
            if node_id_str.startswith('n'):
                node_id = int(node_id_str[1:])
            else:
                node_id = int(node_id_str)
            return ('RECOVERY', node_id, 0)
        
        tx_str = tx_str.replace('(', '').replace(')', '')
        parts = [p.strip() for p in tx_str.split(',')]
        
        if len(parts) == 1:
            try:
                client_id = int(parts[0])
                return ('BALANCE', client_id, config.DEFAULT_CONSISTENCY)
            except ValueError:
                return None
        
        if len(parts) == 2:
            try:
                client_id = int(parts[0])
                consistency = parts[1].lower()
                if consistency in [config.CONSISTENCY_LINEARIZABLE, config.CONSISTENCY_SEQUENTIAL, config.CONSISTENCY_PRAM]:
                    return ('BALANCE', client_id, consistency)
            except ValueError:
                pass
        
        if len(parts) == 3:
            try:
                sender = int(parts[0])
                receiver = int(parts[1])
                amount = float(parts[2])
                return (sender, receiver, amount, config.DEFAULT_CONSISTENCY)
            except ValueError:
                return None
        
        if len(parts) == 4:
            try:
                sender = int(parts[0])
                receiver = int(parts[1])
                amount = float(parts[2])
                consistency = parts[3].lower()
                if consistency in [config.CONSISTENCY_LINEARIZABLE, config.CONSISTENCY_SEQUENTIAL, config.CONSISTENCY_PRAM]:
                    return (sender, receiver, amount, consistency)
                else:
                    return (sender, receiver, amount, config.DEFAULT_CONSISTENCY)
            except ValueError:
                return None
        
        return None
    
    def print_log(self, server_id: int):
        try:
            import os
            log_file = os.path.join("logs", f"server_{server_id}.log")
            
            if not os.path.exists(log_file):
                print(f"Log file not found: {log_file}")
                print(f"Server S{server_id} may not have been started yet or logs directory doesn't exist.")
                return
            
            print(f"\n{'='*80}")
            print(f"SERVER LOG [Server S{server_id}]")
            print(f"{'='*80}\n")
            
            with open(log_file, 'r') as f:
                lines = f.readlines()
                
                if not lines:
                    print("(log file is empty)")
                else:
                    for line in lines:
                        print(line.rstrip())
            
            print(f"\n{'='*80}")
            print(f"Total lines: {len(lines)}")
            print(f"{'='*80}\n")
            
        except Exception as e:
            print(f"Error reading log file for S{server_id}: {e}")
    
    def run_test_cases(self, csv_filename: str, specific_sets: List[int] = None):
        try:
            start_time = time.time()
            with open(csv_filename, 'r') as file:
                reader = csv.DictReader(file)
                rows = list(reader)
            
            current_set = None
            set_transactions = []
            set_live_nodes = None
            executed_count = 0
            
            for i, row in enumerate(rows):
                set_num = row.get('Set Number', '').strip()
                transaction = row.get('Transactions', '').strip()
                live_nodes = row.get('Live Nodes', '').strip()
                
                if set_num != '':
                    if current_set is not None:
                        if specific_sets is None or int(current_set) in specific_sets:
                            self._execute_test_set_sequential(current_set, set_transactions, set_live_nodes)
                            executed_count += 1
                        else:
                            pass
                    
                    current_set = set_num
                    set_transactions = []
                    set_live_nodes = live_nodes
                    
                    if transaction != '':
                        set_transactions.append(transaction)
                else:
                    if transaction != '':
                        set_transactions.append(transaction)
            
            if current_set is not None:
                if specific_sets is None or int(current_set) in specific_sets:
                    self._execute_test_set_sequential(current_set, set_transactions, set_live_nodes)
                    executed_count += 1
                else:
                    
                    pass
            
        except FileNotFoundError:
            print(f"Error: File '{csv_filename}' not found")
        except Exception as e:
            print(f"Error running test cases: {e}")
            import traceback
            traceback.print_exc()
    
    def run_test_cases_concurrent(self, csv_filename: str, specific_sets: List[int] = None, max_workers: int = 100):
        if specific_sets:
            print(f"\n{'='*80}")
            print(f"STARTING AUTOMATED TEST SUITE: {csv_filename}")
            print(f"Running specific sets: {specific_sets}")
            print(f"Concurrent execution with {max_workers} worker threads")
            print(f"{'='*80}\n")
        else:
            print(f"\n{'='*80}")
            print(f"STARTING AUTOMATED TEST SUITE: {csv_filename}")
            print(f"Running all test sets")
            print(f"Concurrent execution with {max_workers} worker threads")
            print(f"{'='*80}\n")
        
        try:
            start_time = time.time()
            with open(csv_filename, 'r') as file:
                reader = csv.DictReader(file)
                rows = list(reader)
            
            current_set = None
            set_transactions = []
            set_live_nodes = None
            executed_count = 0
            
            for i, row in enumerate(rows):
                set_num = row.get('Set Number', '').strip()
                transaction = row.get('Transactions', '').strip()
                live_nodes = row.get('Live Nodes', '').strip()
                
                if set_num != '':
                    if current_set is not None:
                        if specific_sets is None or int(current_set) in specific_sets:
                            self._execute_test_set_concurrent(current_set, set_transactions, set_live_nodes, max_workers)
                            executed_count += 1
                        else:
                            pass
                    
                    current_set = set_num
                    set_transactions = []
                    set_live_nodes = live_nodes
                    
                    if transaction != '':
                        set_transactions.append(transaction)
                else:
                    if transaction != '':
                        set_transactions.append(transaction)
            
            if current_set is not None:
                if specific_sets is None or int(current_set) in specific_sets:
                    self._execute_test_set_concurrent(current_set, set_transactions, set_live_nodes, max_workers)
                    executed_count += 1
                else:
                    pass
            
            print(f"\n{'='*80}")
            print(f"AUTOMATED TEST SUITE COMPLETED")
            print(f"Executed {executed_count} test set(s)")
            print(f"{'='*80}\n")
            end_time = time.time()
            # print(f"Time taken: {end_time - start_time:.2f} seconds")
        except FileNotFoundError:
            print(f"Error: File '{csv_filename}' not found")
        except Exception as e:
            print(f"Error running test cases: {e}")
            import traceback
            traceback.print_exc()
    
    def _execute_test_set_sequential(self, set_num, transactions, live_nodes_str):
        print(f"\n{'='*80}")
        print(f"EXECUTING TEST SET {set_num}")
        print(f"Total Transactions: {len(transactions)}")
        print(f"{'='*80}")
        
        self.reset_performance()
        
        self._reset_all_nodes()
        
        live_nodes = self.parse_live_nodes(live_nodes_str)
        if live_nodes:
            self._ensure_only_nodes_active(live_nodes)
        
        print(f"starting servers for live nodes: {live_nodes}")
        self._trigger_leader_elections(live_nodes)
        
        self.last_test_set_clients = set()
        
        success_count = 0
        total_count = 0
        start_time = time.time()
        self.perf_start_time = start_time
        
        for tx_idx, tx_str in enumerate(transactions, 1):
            parsed = self.parse_transaction(tx_str)
            if parsed:
                if parsed[0] == 'FAILURE':
                    node_id = parsed[1]
                    print(f"\nTransaction {tx_idx}: F(n{node_id}) - Setting node {node_id} to IDLE")
                    self.set_node_idle(node_id)
                elif parsed[0] == 'RECOVERY':
                    node_id = parsed[1]
                    print(f"\nTransaction {tx_idx}: R(n{node_id}) - Recovering node {node_id}")
                    self.set_node_active(node_id)
                elif parsed[0] == 'BALANCE':
                    client_id = parsed[1]
                    consistency_level = parsed[2] if len(parsed) > 2 else config.DEFAULT_CONSISTENCY
                    total_count += 1
                    print(f"\nTransaction {tx_idx}: Balance query for client {client_id}")
                    success, balance = self.send_balance_request(client_id, consistency_level)
                    if success:
                        success_count += 1
                        print(f"Balance: ${balance}")
                    else:
                        print(f"Balance query failed: {balance}")
                else:
                    sender, receiver, amount, consistency = parsed
                    self.last_test_set_clients.add(sender)
                    self.last_test_set_clients.add(receiver)
                    
                    total_count += 1
                    print(f"\nTransaction {tx_idx}: {sender} -> {receiver}: ${amount}")
                    success, msg = self.submit_transaction(sender, receiver, amount)
                    if success:
                        success_count += 1
                        print(f"{msg}")
                    else:
                        print(f"{msg}\n")
            else:
                print(f"\nTransaction {tx_idx}: Invalid transaction format: {tx_str}")
        
        end_time = time.time()
        self.perf_end_time = end_time
        elapsed = end_time - start_time
        
        print(f"\nSet {set_num} Results: {success_count}/{total_count} successful")
        if total_count > 0 and elapsed > 0:
            throughput = total_count / elapsed
        print(f"\n{'='*80}")
        print(f"TEST SET {set_num} COMPLETED")
        print(f"{'='*80}\n")
    
    def _execute_test_set_concurrent(self, set_num, transactions, live_nodes_str, max_workers=100):
        print(f"\n{'='*80}")
        print(f"EXECUTING TEST SET {set_num}")
        print(f"Total Transactions: {len(transactions)}")
        print(f"Running transactions CONCURRENTLY with {max_workers} worker threads")
        print(f"{'='*80}")
        
        self.reset_performance()
        
        self._reset_all_nodes()
        
        live_nodes = self.parse_live_nodes(live_nodes_str)
        if live_nodes:
            self._ensure_only_nodes_active(live_nodes)
        
        self._trigger_leader_elections(live_nodes)
        
        self.last_test_set_clients = set()
        start_time = time.time()
        self.perf_start_time = start_time
        
        # Prepare concurrent transactions
        concurrent_transactions = []
        balance_total = 0
        balance_success = 0
        
        for tx_idx, tx_str in enumerate(transactions, 1):
            if tx_str.upper() == 'LF':
                pass
            else:
                parsed = self.parse_transaction(tx_str)
                if parsed:
                    if parsed[0] == 'FAILURE':
                        node_id = parsed[1]
                        print(f"\nTransaction {tx_idx}: F(n{node_id}) - Setting node {node_id} to IDLE")
                        self.set_node_idle(node_id)
                    elif parsed[0] == 'RECOVERY':
                        node_id = parsed[1]
                        print(f"\nTransaction {tx_idx}: R(n{node_id}) - Recovering node {node_id}")
                        self.set_node_active(node_id)
                    elif parsed[0] == 'BALANCE':
                        client_id = parsed[1]
                        consistency_level = parsed[2] if len(parsed) > 2 else config.DEFAULT_CONSISTENCY
                        balance_total += 1
                        print(f"\nTransaction {tx_idx}: Balance query for client {client_id}")
                        success, balance = self.send_balance_request(client_id, consistency_level)
                        if success:
                            balance_success += 1
                            print(f"Balance: ${balance}")
                        else:
                            print(f"Balance query failed: {balance}")
                    else:
                        sender, receiver, amount, consistency = parsed
                        self.last_test_set_clients.add(sender)
                        self.last_test_set_clients.add(receiver)
                        concurrent_transactions.append((sender, receiver, amount, consistency))
                else:
                    print(f"\nTransaction {tx_idx}: Invalid transaction format: {tx_str}")
        
        rw_success = 0
        rw_total = 0
        if concurrent_transactions:
            results = self.submit_concurrent_transactions(concurrent_transactions, max_workers=max_workers)
            rw_success = results['success']
            rw_total = results['total']
        
        end_time = time.time()
        self.perf_end_time = end_time
        elapsed = end_time - start_time
        total = rw_total + balance_total
        success = rw_success + balance_success
        print(f"\nSet {set_num} Results: {success}/{total} successful")
        if total > 0 and elapsed > 0:
            throughput = total / elapsed

        print(f"\n{'='*80}")
        print(f"TEST SET {set_num} COMPLETED")
        print(f"{'='*80}\n")
    
    def _reset_all_nodes(self):
        
        for node_id in range(1, config.TOTAL_NODES + 1):
            if node_id in self.stubs:
                try:
                    self.stubs[node_id].ResetSystemState(paxos_pb2.Empty(), timeout=5.0)
                except grpc.RpcError as e:
                    print(f"Failed to reset Node {node_id}: {e}")
        
        self.cluster_leaders = {}
    
    def _trigger_leader_elections(self, live_nodes: List[int] = None):
        live_set = set(live_nodes) if live_nodes else set(range(1, config.TOTAL_NODES + 1))
        
        for cluster_id in range(1, config.NUM_CLUSTERS + 1):
            nodes = config.get_nodes_in_cluster(cluster_id)
            for node_id in nodes:
                if node_id not in live_set:
                    continue
                if node_id not in self.stubs:
                    continue
                try:
                    response = self.stubs[node_id].TriggerElection(paxos_pb2.Empty(), timeout=1.0)
                    if response.success:
                        self.cluster_leaders[cluster_id] = response.leader_id
                        break
                except grpc.RpcError:
                    continue
            
            if cluster_id not in self.cluster_leaders:
                print(f"  Cluster {cluster_id}: No leader elected")
        
    
    def _ensure_only_nodes_active(self, live_nodes: List[int]):
        all_nodes = set(range(1, config.TOTAL_NODES + 1))
        nodes_should_be_idle = all_nodes - set(live_nodes)
        
        print(f"Required state: Live nodes = {live_nodes}")
        current_states = {}
        for node_id in range(1, config.TOTAL_NODES + 1):
            if node_id in self.stubs:
                try:
                    info = self.stubs[node_id].GetNodeInfo(paxos_pb2.Empty(), timeout=2.0)
                    current_states[node_id] = "IDLE" if info.is_idle else "ACTIVE"
                except grpc.RpcError:
                    current_states[node_id] = "UNKNOWN"
            else:
                current_states[node_id] = "NO_STUB"
        
        for node_id in live_nodes:
            if current_states.get(node_id) != "ACTIVE":
                self.set_node_active(node_id)
            else:
                pass
        
        for node_id in nodes_should_be_idle:
            if current_states.get(node_id) != "IDLE":
                self.set_node_idle(node_id)
            else:
                pass
    

def parse_server_ids(args: List[str]) -> List[int]:
    server_ids = []
    for arg in args:
        if arg.upper().startswith('S'):
            try:
                server_ids.append(int(arg[1:]))
            except ValueError:
                print(f"Invalid server ID: {arg}")
        else:
            try:
                server_ids.append(int(arg))
            except ValueError:
                print(f"Invalid server ID: {arg}")
    return server_ids

def remove_db_files():
    if os.path.exists(config.DB_DIR):
        for file in os.listdir(config.DB_DIR):
            os.remove(os.path.join(config.DB_DIR, file))

def main():
    """Interactive CLI for Multi-Paxos system Distributed Systems Lab 1"""
    controller = PaxosMultiClientController()
    remove_db_files()
    help_text = """
Available Commands:
=== Server Management ===
  terminals [server_id...]  - Start servers in separate terminal windows
  elect                     - Trigger leader election on all clusters
  crash server_id           - Crash server or make it idle (doesn't participate)
  recover server_id         - Recover crashed server or make it active (participates normally)

=== Manual Transactions (CLI) ===
  submit sender receiver amount - Submit single transaction via CLI
  concurrent num_txns [workers] - Run concurrent transactions (default: 40 threads)
  runtest filename.csv      - Run all test cases from CSV file automatically
  runset filename.csv set_nums - Run test sets SEQUENTIALLY (e.g., runset file.csv 1,3,5)
  runset_concurrent filename.csv set_nums [workers] - Run test sets CONCURRENTLY with thread pool

=== Balance & Resharding ===
  printbalance client_id    - Print balance of client from all nodes in its cluster
  printdb                   - Print balances of clients in last test set across all nodes
  printreshard              - Perform hypergraph partitioning and show data movements
  showmappings              - Show current resharded client mappings
  resetmappings             - Reset client mappings to default

=== Consistency Levels ===
  consistency               - Show available consistency levels
  Transaction formats:
    (sender, receiver, amount)                    - Default (linearizable)
    (sender, receiver, amount, sequential)        - Sequential consistency
    (sender, receiver, amount, pram)              - PRAM consistency
  Balance query formats:
    (client_id)                                   - Default (linearizable)
    (client_id, sequential)                       - Sequential consistency
    (client_id, pram)                             - PRAM consistency

=== Performance ===
  performance               - Show performance stats (TPS, latency, success/fail counts)

=== System Information ===
  print_f_log server_id     - Show final executed log from specific server
  printstatus seq_num       - Show transaction status across all nodes
  printview                 - Show NEW-VIEW message history from all nodes
  printlog server_id        - Show all Paxos protocollog messages
  serverstatus              - Show status of all servers (running, idle, role)

=== General ===
  help                      - Show this help message
  exit/quit                 - Exit the program

Examples:
  terminals                 - Start all servers in separate terminals
  terminals S1 S3           - Start S1 and S3 in separate terminals
  submit 1 2 100            - Submit $100 transaction from client 1 to client 2
  printbalance 1            - Show balance of client 1 from all nodes in its cluster
  printdb                   - Show balances of all clients in last test set
  printreshard              - Analyze and suggest data movements
  print_f_log S1            - Show final executed log from S1
  printstatus 5             - Show status of transaction at sequence 5
  printview                 - Show NEW-VIEW history from all nodes
  printlog S1               - Show all Paxos log messages from S1
  serverstatus              - Show status of all servers
  crash S2                  - Crash S2 or make it idle
  recover S2                - Recover crashed S2 or make it active
  runtest CSE535-F25-Project-1-Testcases.csv - Run all test cases automatically
  runset CSE535-F25-Project-1-Testcases.csv 1,3,5 - Run sets 1, 3, 5 SEQUENTIALLY
  runset CSE535-F25-Project-1-Testcases.csv 2 - Run set 2 SEQUENTIALLY
  runset_concurrent test.csv 1,2 30 - Run sets 1,2 CONCURRENTLY with 30 threads
  runset_concurrent test.csv 5 50 - Run set 5 CONCURRENTLY with 50 threads
    """

    print("Multi-Paxos Distributed System Controller")
    print("Type 'help' for available commands\n")
    
    while True:
        try:
            command = input("116228817_PAXOS> ").strip().split()
            if not command:
                continue
            
            cmd = command[0].lower()
            args = command[1:]
            
            if cmd in ['exit', 'quit']:
                print("Shutting down...")
                controller.stop_servers()
                break
            
            elif cmd == 'help':
                print(help_text)
            

            
            elif cmd == 'terminals':
                server_ids = parse_server_ids(args) if args else None
                controller.start_servers_in_terminals(server_ids)
            
            elif cmd == 'elect':
                print("Triggering leader elections on all clusters...")
                controller._trigger_leader_elections()
                print("Leader election complete.")
            
            elif cmd == 'performance':
                controller.Performance()
            
            elif cmd == 'printreshard':
                controller.PrintReshard()
            
            elif cmd == 'showmappings':
                # Show current client-to-cluster mappings that differ from default
                if config.CLIENT_TO_CLUSTER_OVERRIDE:
                    print(f"\nResharded client mappings ({len(config.CLIENT_TO_CLUSTER_OVERRIDE)} clients):")
                    for client_id, cluster_id in sorted(config.CLIENT_TO_CLUSTER_OVERRIDE.items()):
                        orig = config.get_original_cluster_for_client(client_id)
                        print(f"  Client {client_id}: c{orig} -> c{cluster_id}")
                else:
                    print("No resharded clients. Using default range-based mapping.")
            
            
            elif cmd == 'resetmappings':
                config.reset_client_mappings()
                # Also delete the reshard file
                import os
                if os.path.exists(config.RESHARD_FILE):
                    os.remove(config.RESHARD_FILE)
                print("Client mappings reset to default range-based partitioning.")
            
            elif cmd == 'submit':
                if len(args) < 3:
                    print("Usage: submit sender receiver amount")
                    continue
                try:
                    sender = int(args[0])
                    receiver = int(args[1])
                    amount = float(args[2])
                    success, message = controller.submit_transaction(sender, receiver, amount)
                    if success:
                        print(f"{message}")
                    else:
                        print(f"Transaction failed: {message}")
                except ValueError:
                    print("Invalid arguments. sender and receiver must be integers, amount must be a number")
            
            elif cmd == 'printbalance':
                if len(args) != 1:
                    print("Usage: printbalance client_id")
                    print("Example: printbalance 4005")
                    continue
                try:
                    client_id = int(args[0])
                    controller.print_balance(client_id)
                except ValueError:
                    print("Invalid client_id. Must be an integer")
            
            elif cmd == 'printdb':
                if len(args) != 0:
                    print("Usage: printdb")
                    continue
                controller.print_db()
            
            
            elif cmd == 'print_f_log':
                if len(args) != 1:
                    print("Usage: print_f_log server_id")
                    continue
                try:
                    server_id = int(args[0][1:]) if args[0].upper().startswith('S') else int(args[0])
                    controller.print_f_log(server_id)
                except ValueError:
                    print("Invalid server ID")
            
            elif cmd == 'printstatus':
                if len(args) != 1:
                    print("Usage: printstatus sequence_number")
                    continue
                try:
                    seq_num = int(args[0])
                    controller.print_status(seq_num)
                except ValueError:
                    print("Invalid sequence number")
            
            elif cmd == 'printview':
                controller.print_view()
            
            elif cmd == 'printlog':
                if len(args) != 1:
                    print("Usage: printlog server_id")
                    continue
                try:
                    server_id = int(args[0][1:]) if args[0].upper().startswith('S') else int(args[0])
                    controller.print_log(server_id)
                except ValueError:
                    print("Invalid server ID")
            
            elif cmd == 'serverstatus':
                controller.print_server_status()
        
            elif cmd == 'crash':
                if len(args) != 1:
                    print("Usage: crash server_id")
                    continue
                try:
                    server_id = int(args[0][1:]) if args[0].upper().startswith('S') else int(args[0])
                    controller.set_node_idle(server_id)
                except ValueError:
                    print("Invalid server ID")
            
            elif cmd == 'recover':
                if len(args) != 1:
                    print("Usage: recover server_id")
                    continue
                try:
                    server_id = int(args[0][1:]) if args[0].upper().startswith('S') else int(args[0])
                    controller.set_node_active(server_id)
                except ValueError:
                    print("Invalid server ID")
            
            
            elif cmd == 'runtest':
                if len(args) != 1:
                    print("Usage: runtest filename.csv")
                    continue
                controller.run_test_cases(args[0])
            
            elif cmd == 'runset':
                if len(args) < 1 or len(args) > 2:
                    print("Usage: runset filename.csv [set_numbers]")
                    print("Examples:")
                    print("  runset test.csv        - Run all sets SEQUENTIALLY")
                    print("  runset test.csv 1      - Run only set 1 SEQUENTIALLY")
                    print("  runset test.csv 1,3,5  - Run sets 1, 3, and 5 SEQUENTIALLY")
                    continue
                
                filename = args[0]
                if len(args) == 2:
                    try:
                        set_nums_str = args[1]
                        set_nums = [int(s.strip()) for s in set_nums_str.split(',')]
                        controller.run_test_cases(filename, set_nums)
                    except ValueError:
                        print("Invalid set numbers. Use comma-separated integers (e.g., 1,3,5)")
                else:
                    controller.run_test_cases(filename)
            
            elif cmd == 'runset_concurrent':
                if len(args) < 1 or len(args) > 3:
                    print("Usage: runset_concurrent filename.csv [set_numbers] [workers]")
                    print("Examples:")
                    print("  runset_concurrent test.csv                - Run all sets CONCURRENTLY with 40 threads (default)")
                    print("  runset_concurrent test.csv 1              - Run set 1 CONCURRENTLY with 40 threads")
                    print("  runset_concurrent test.csv 1,3,5 30       - Run sets 1,3,5 CONCURRENTLY with 30 threads")
                    print("  runset_concurrent test.csv 2 50           - Run set 2 CONCURRENTLY with 50 threads")
                    continue
                
                filename = args[0]
                set_nums = None
                max_workers = 100
                
                if len(args) >= 2:
                    try:
                        set_nums_str = args[1]
                        set_nums = [int(s.strip()) for s in set_nums_str.split(',')]
                    except ValueError:
                        print("Invalid set numbers. Use comma-separated integers (e.g., 1,3,5)")
                        continue
                
                if len(args) == 3:
                    try:
                        max_workers = int(args[2])
                        if max_workers < 1 or max_workers > 100:
                            print("Worker count must be between 1 and 100")
                            continue
                    except ValueError:
                        print("Invalid worker count. Must be an integer.")
                        continue
                
                controller.run_test_cases_concurrent(filename, set_nums, max_workers)
            
            else:
                print(f"Unknown command: {cmd}. Type 'help' for available commands.")
        
        except KeyboardInterrupt:
            print("Use 'exit' or 'quit' to shutdown gracefully.")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == '__main__':
    main()
