"""
Author: sharshith1312
Course: CSE535 - Distributed Systems
"""

import grpc
import threading
import time
import json
import hashlib
import concurrent.futures
from shared import config
from shared.persistence import ServerState
from shared.logger import setup_logger
import paxos_pb2
import paxos_pb2_grpc
from server.lock_manager import LockManager
from server.wal_manager import WALManager
from server import transaction_utils


def ballot_greater_than(b1, b2):
    if b1.number > b2.number:
        return True
    if b1.number == b2.number and b1.node_id > b2.node_id:
        return True
    return False

class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.lock = threading.Lock()
        
        # Determine cluster membership
        self.cluster_id = config.NODE_TO_CLUSTER.get(node_id)
        if not self.cluster_id:
            raise ValueError(f"Node {node_id} not assigned to any cluster")
        
        self.cluster_nodes = config.get_nodes_in_cluster(self.cluster_id)
        
        # Set up logger for this node
        self.logger = setup_logger(node_id)
        self.logger.info(f"Node {node_id} belongs to Cluster {self.cluster_id} with nodes {self.cluster_nodes}")

        self.persistence = ServerState(node_id)
        
        # Initialize 2PC managers
        self.lock_manager = LockManager(self.persistence)
        self.wal_manager = WALManager(self.persistence)
        
        self._load_persistent_state()
        
        # gRPC stubs for cross-cluster communication (will be populated on demand)
        self.stubs = {}  # {node_id: stub}

        # Initialize client replies only for this cluster's shard
        start_id, end_id = config.SHARD_RANGES[self.cluster_id]
        self.client_replies = {i: None for i in range(start_id, end_id + 1)}

        self.promise_counts = {}
        self.promise_logs = {}
        
        self.checkpoint_interval = config.CHECKPOINT_INTERVAL
        
        # Leader election state
        self.queued_prepare_messages = []
        self.last_prepare_received_time = 0 
        self.last_heartbeat_time = 0 
        self.timer_start_time = 0
        self.is_idle = False
        self.new_leader_syncing = False

        self.peers = {}
        for nid in self.cluster_nodes:
            if nid != self.node_id:
                addr = config.NODE_ADDRESSES[nid]
                channel = grpc.insecure_channel(addr)
                self.peers[nid] = paxos_pb2_grpc.PaxosServiceStub(channel)
                self.logger.info(f"Connected to cluster peer: Node {nid} at {addr}")

        self.leader_timer = None
        
        self._pending_execution_event = threading.Event()
        self._execution_thread_running = True
        self._execution_thread = threading.Thread(target=self._background_execution_loop, daemon=True)
        self._execution_thread.start()
        
        self.logger.info(f"Initialized as {self.role} with persistent state loaded.")
    
    def _load_persistent_state(self):
        try:
            state = self.persistence.load_state()
            self.role = 'BACKUP'
            self.leader_id = state['leader_id']
            self.sequence_num = state['sequence_num']
            self.last_executed_seq = state['last_executed_seq']

            self.highest_promised_ballot = self.persistence.deserialize_ballot_num(state['highest_promised_ballot'])

            self.ballot_num = paxos_pb2.BallotNumber(number=0, node_id=self.node_id)
            
            self.log = self.persistence.deserialize_log()
            
            try:
                self.datastore = {int(k): v for k, v in state['datastore'].items()}
            except (ValueError, TypeError) as e:
                self.logger.warning(f"Error converting datastore keys, reinitializing: {e}")
                self.datastore = {}
            
            try:
                self.last_reply_timestamps = {int(k): v for k, v in state['last_reply_timestamps'].items()}
            except (ValueError, TypeError) as e:
                self.logger.warning(f"Error converting last_reply_timestamps keys, reinitializing: {e}")
                self.last_reply_timestamps = {}
            
            self.transaction_status = {int(k): v for k, v in state['transaction_status'].items()}
            self.transaction_status_message = {int(k): v for k, v in state.get('transaction_status_message', {}).items()}
            
            self.new_view_history = state['new_view_history'].copy()
            
            self.last_checkpoint_seq = state['last_checkpoint_seq']
            self.latest_checkpoint_state = self.persistence.deserialize_checkpoint_state()
            
            self.logger.debug("Checking for stale locks from crash recovery...")
            expired_count = self.lock_manager.release_expired_locks(timeout=0.1)
            if expired_count > 0:
                self.logger.warning(f"Released {expired_count} stale locks from crash recovery")
            
            self.logger.debug("Checking for uncommitted 2PC transactions...")
            wal_entries = self.persistence.get_all_wal_entries()
            
            if wal_entries:
                for txid in list(wal_entries.keys()):
                    self.logger.warning(f"Found uncommitted 2PC transaction: {txid}")
                    twopc_state = self.persistence.get_2pc_state(txid)
                    
                    if twopc_state:
                        self.logger.warning(f"Aborting uncommitted transaction {txid} (state: {twopc_state.get('status', 'UNKNOWN')})")
                        self.wal_manager.rollback_transaction(txid, self.datastore)
                        
                        self.lock_manager.release_all_locks_for_transaction(txid)
                        
                        self.persistence.delete_2pc_state(txid)
                    else:
                        self.logger.error(f"Orphaned WAL entry for {txid} - cleaning up")
                        self.persistence.clear_wal(txid)
            
            if self.last_checkpoint_seq > self.last_executed_seq:
                self.logger.info("INCONSISTENT STATE DETECTED!")
                self.logger.info(f"  - Checkpoint at seq {self.last_checkpoint_seq} but only executed up to {self.last_executed_seq}")
                self.logger.info(f"  - This means we missed transactions while offline")
                self.logger.info(f"  - Requesting checkpoint recovery...")
                
                if self._recover_from_checkpoint():
                    self.logger.info("Successfully recovered from checkpoint")
                else:
                    self.logger.info("WARNING: Could not recover checkpoint - may have execution gaps")
            
            self.logger.debug(f"Loaded persistent state: {len(self.log)} log entries, seq_num={self.sequence_num}")
            
        except Exception as e:
            self.logger.info(f"Error loading persistent state: {e}")
            self.role = 'BACKUP'
            self.leader_id = None
            self.ballot_num = paxos_pb2.BallotNumber(number=0, node_id=self.node_id)
            self.highest_promised_ballot = paxos_pb2.BallotNumber(number=0, node_id=0)
            self.log = {}
            self.sequence_num = 0
            start_id, end_id = config.SHARD_RANGES[self.cluster_id]
            self.datastore = {i: config.INITIAL_AMOUNT for i in range(start_id, end_id + 1)}
            self.last_reply_timestamps = {i: -1 for i in range(start_id, end_id + 1)}
            self.transaction_status = {}
            self.transaction_status_message = {}
            self.new_view_history = []
            self.last_checkpoint_seq = 0
            self.latest_checkpoint_state = None
            self.last_executed_seq = 0

    
    def is_leader_timer_expired(self):
        if self.leader_timer is None:
            return False, 0
        
        if self.timer_start_time <= 0:
            return False, 0
        
        current_time = time.time()
        timeout_duration = config.get_randomized_timeout(self.node_id)
        elapsed_time = current_time - self.timer_start_time
        is_expired = elapsed_time >= timeout_duration
        remaining_time = timeout_duration - elapsed_time if not is_expired else 0
        return is_expired, remaining_time

    def start_leader_failure_timer(self):
        if self.leader_timer:
            self.leader_timer.cancel()
        timeout = config.get_randomized_timeout(self.node_id)
        self.timer_start_time = time.time()
        self.leader_timer = threading.Timer(timeout, self.initiate_leader_election)
        self.leader_timer.start()

    def reset_leader_failure_timer(self):
        if self.leader_timer:
            self.leader_timer.cancel()
        self.start_leader_failure_timer()


    def initiate_leader_election(self):
        
        if self.is_idle:
            self.logger.info("[Election] Node is IDLE, not starting election")
            return
        
        with self.lock:
            if self.role == 'PROPOSER':
                self.logger.info("[Election] Already PROPOSER, election in progress")
                return
            if self.role == 'LEADER':
                self.logger.info("[Election] Already LEADER, no election needed")
                return
        
        current_time = time.time()
        if self.last_prepare_received_time > 0:
            time_since_last_prepare = current_time - self.last_prepare_received_time
            if time_since_last_prepare < config.get_randomized_timeout(self.node_id) + config.TP_WAIT_DURATION:
                sleep_time = config.get_randomized_timeout(self.node_id) + config.TP_WAIT_DURATION - time_since_last_prepare
                self.logger.info(f"[Election] Received PREPARE {time_since_last_prepare:.2f}s ago, sleeping {sleep_time:.2f}s before starting election")
                
                sleep_start_time = time.time()
                time.sleep(sleep_time)
                
                if self.last_heartbeat_time > sleep_start_time:
                    self.logger.info(f"[Election] Received heartbeat during tp wait (leader {self.leader_id}), aborting election")
                    return
        
        self.logger.info(f"[Election] Starting leader election")
        
        with self.lock:
            self.role = 'PROPOSER'
            self.leader_id = None
        
        self.ballot_num.number += 1
        self.ballot_num.node_id = self.node_id
        
        self.persistence.update_leader_id(self.leader_id)
        
        self.logger.info(f"[Election] Sending PREPARE with ballot ({self.ballot_num.number},{self.ballot_num.node_id})")
        
        prepare_msg = paxos_pb2.PrepareMsg(ballot_num=self.ballot_num)
        
        ballot_key = (self.ballot_num.number, self.ballot_num.node_id)
        self.promise_counts[ballot_key] = 1
        self.promise_logs[ballot_key] = [paxos_pb2.PromiseMsg(
            acknowledged=True, 
            ballot_num=self.ballot_num, 
            accept_log=list(self.log.values())
        )]
        
        self.send_prepare_messages_parallel(prepare_msg)


    def send_prepare_messages_parallel(self, prepare_msg):
        
        def send_prepare_to_peer(peer_id, stub):
            try:
                promise_msg = stub.SendPrepare(prepare_msg, timeout=2.0)
                return promise_msg
            except grpc.RpcError as e:
                self.logger.debug(f"Failed to get PROMISE from Node {peer_id}: {e.code()}")
            except Exception as e:
                self.logger.debug(f"Error sending PREPARE to Node {peer_id}: {e}")
            return None
        
        ballot_key = (self.ballot_num.number, self.ballot_num.node_id)
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.peers)) as executor:
            future_to_peer = {
                executor.submit(send_prepare_to_peer, peer_id, stub): peer_id 
                for peer_id, stub in self.peers.items()
            }
            
            try:
                for future in concurrent.futures.as_completed(future_to_peer, timeout=3.0):
                    peer_id = future_to_peer[future]
                    
                    try:
                        promise_msg = future.result()
                        
                        if promise_msg and promise_msg.acknowledged:
                            self.logger.info(f"Received PROMISE from Node {peer_id}")
                            self.promise_counts[ballot_key] += 1
                            self.promise_logs[ballot_key].append(promise_msg)
                            
                            # Early exit if we have quorum
                            # if self.promise_counts[ballot_key] >= config.QUORUM_SIZE:
                            #     self.logger.info(f"Quorum achieved ({self.promise_counts[ballot_key]}/{config.QUORUM_SIZE})")
                            #     break
                        else:
                            self.logger.debug(f"Node {peer_id} rejected PREPARE")
                            
                    except Exception as e:
                        self.logger.debug(f"Error processing response from Node {peer_id}: {e}")
            
            except concurrent.futures.TimeoutError:
                self.logger.info(f"Timeout waiting for PREPARE responses")
        
        final_promises = self.promise_counts.get(ballot_key, 0)
        
        if final_promises < config.QUORUM_SIZE:
            self.logger.info(f"Failed to achieve quorum ({final_promises}/{config.QUORUM_SIZE}). Becoming BACKUP.")
            self.role = 'BACKUP'
            self.leader_id = None
            self.start_leader_failure_timer()
        else:
            self.logger.info(f"Achieved quorum ({final_promises}/{config.QUORUM_SIZE}). Becoming LEADER.")
            self.become_leader_and_consolidate_logs(ballot_key)


    def become_leader_and_consolidate_logs(self, ballot_key):

        with self.lock:
            if self.role != 'PROPOSER':
                self.logger.info(f"Not a PROPOSER anymore (role={self.role}), skipping leader election")
                return
            
            self.logger.info(f"Quorum reached. Becoming LEADER.")
            self.role = 'LEADER'
            self.leader_id = self.node_id
            self.new_leader_syncing = True
        
        self.logger.info(f"Node {self.node_id} is leader. Starting heartbeat thread.")
        threading.Thread(target=self.send_heartbeats, daemon=True).start()

        self.highest_promised_ballot = self.ballot_num
        self.persistence.update_highest_promised_ballot(self.ballot_num)
        self.logger.info(f"Updated highest_promised_ballot to ({self.highest_promised_ballot.number}, {self.highest_promised_ballot.node_id})")
        

        if self.leader_timer:
            self.leader_timer.cancel()
            self.leader_timer = None
            self.logger.info(f"Canceled leader failure timer (now LEADER)")
        
        all_logs = self.promise_logs[ballot_key]
        
        highest_checkpoint = 0
        for prom_msg in all_logs:
            if prom_msg.latest_checkpoint and prom_msg.latest_checkpoint.sequence_num > highest_checkpoint:
                highest_checkpoint = prom_msg.latest_checkpoint.sequence_num
        
        server_logs = {}
        server_logs[self.node_id] = self.log
        
        for prom_msg in all_logs:
            peer_log = {entry.sequence_num: entry for entry in prom_msg.accept_log}
            server_logs[f"peer_{len(server_logs)}"] = peer_log
        
        max_seq = 0
        for log in server_logs.values():
            if log:
                max_seq = max(max_seq, max(log.keys()) if log else 0)
        
        self.logger.info(f"Consolidating logs from {len(all_logs)} PROMISE messages, max_seq={max_seq}, checkpoint={highest_checkpoint}")
        
        consolidated_log = {}
        start_seq = max(highest_checkpoint + 1, 1)
        for seq in range(start_seq, max_seq + 1):
            best_entry = None
            best_ballot = None
            
            for server_id, log in server_logs.items():
                if seq in log:
                    entry = log[seq]
                    if best_entry is None or ballot_greater_than(entry.ballot_num, best_ballot):
                        best_entry = entry
                        best_ballot = entry.ballot_num
            
            if best_entry:
                if best_entry.is_noop:
                    consolidated_log[seq] = paxos_pb2.LogEntry(
                        ballot_num=best_entry.ballot_num,
                        sequence_num=best_entry.sequence_num,
                        is_noop=True,
                        status="X"
                    )
                    self.logger.debug(f"Seq {seq}: Using NO-OP entry with status X")
                else:
                    consolidated_log[seq] = best_entry
                    self.logger.debug(f"Seq {seq}: Using entry with ballot ({best_entry.ballot_num.number},{best_entry.ballot_num.node_id})")
            else:
                self.logger.debug(f"Seq {seq}: Creating NO-OP (gap)")
                consolidated_log[seq] = paxos_pb2.LogEntry(
                    ballot_num=self.ballot_num,
                    sequence_num=seq,
                    is_noop=True,
                    status="X"
                )
        
        seq_nums = sorted(consolidated_log.keys())
        self.logger.debug(f"Consolidated log sequence numbers: {seq_nums}")
        
        if highest_checkpoint > self.last_checkpoint_seq:
            self.last_checkpoint_seq = highest_checkpoint
        
        
        self.logger.debug(f"Starting NewView prepare requests from consolidated log")
        for seq in consolidated_log.keys():
            self.logger.info(f"Seq {seq}: Sending prepare request with ballot {self.ballot_num.number},{self.ballot_num.node_id}")
            self.logger.info(f"Seq {seq}: Received Majority Quorum Committing")

        
        next_seq = self.last_executed_seq + 1
        while next_seq in consolidated_log and next_seq <= max_seq:
            entry = consolidated_log[next_seq]
            
            if entry.is_noop:
                self.logger.debug(f"Seq {next_seq}: NO-OP")
                self.update_transaction_status(next_seq, "X", "NO-OP")
                self.last_executed_seq = next_seq
                self.persistence.update_last_executed_seq(next_seq)
                
                entry_to_store = paxos_pb2.LogEntry(
                    ballot_num=entry.ballot_num,
                    sequence_num=entry.sequence_num,
                    is_noop=True,
                    status="X"
                )
            else:
                self.logger.debug(f"Seq {next_seq}: Executing {entry.request.transaction}")
                
                tx_parts = entry.request.transaction.strip("()").split(',')
                sender = int(tx_parts[0].strip())
                receiver = int(tx_parts[1].strip())
                amount = float(tx_parts[2].strip())
                
                if self.datastore[sender] >= amount:
                    self.datastore[sender] -= amount
                    self.datastore[receiver] += amount
                    self.logger.debug(f"Executed: {entry.request.transaction} at seq {next_seq}")
                    self.update_transaction_status(next_seq, "E", "Success")
                    status_for_entry = "E"
                    self.persistence.update_datastore(self.datastore)
                else:
                    self.logger.debug(f"Failed (insufficient balance): {entry.request.transaction} at seq {next_seq}")
                    self.update_transaction_status(next_seq, "E", "Insufficient Balance")
                    status_for_entry = "E"
                
                self.last_executed_seq = next_seq
                self.persistence.update_last_executed_seq(next_seq)
                
                self.last_reply_timestamps[entry.request.client_id] = entry.request.timestamp
                self.persistence.update_last_reply_timestamp(entry.request.client_id, entry.request.timestamp)
                
                entry_to_store = paxos_pb2.LogEntry(
                    ballot_num=entry.ballot_num,
                    sequence_num=entry.sequence_num,
                    request=entry.request,
                    is_noop=False,
                    status=status_for_entry
                )
            
            self.log[next_seq] = entry_to_store
            self.persistence.add_log_entry(next_seq, entry_to_store)
            
            next_seq += 1
        
        self.sequence_num = max_seq
        self.persistence.update_sequence_num(self.sequence_num)
        
        if highest_checkpoint > 0:
            self.persistence.update_checkpoint_state(highest_checkpoint, self.latest_checkpoint_state)
        
        self.logger.info(f"Finished executing and updating log. last_executed_seq={self.last_executed_seq}")
                    
        sorted_log_entries = [self.log[seq] for seq in sorted(self.log.keys())]
        new_view_msg = paxos_pb2.NewViewMsg(
            ballot_num=self.ballot_num, 
            accept_log=sorted_log_entries,
            latest_checkpoint=self.latest_checkpoint_state
        )
        self.add_new_view_to_history(new_view_msg)
        for peer_id, stub in self.peers.items():
            try:
                stub.SendNewView(new_view_msg)
            except grpc.RpcError as e:
                pass
        

        self.new_leader_syncing = False
        self.logger.debug(f"NEW-VIEW sync complete. Ready to accept client requests.")

    def send_heartbeats(self):
        while self.role == 'LEADER' and self.leader_id == self.node_id:
            heartbeat_msg = paxos_pb2.HeartbeatMsg(leader_id=self.node_id, ballot_num=self.ballot_num)
            for nid, stub in self.peers.items():
                try:
                    stub.SendHeartbeat(heartbeat_msg,timeout=config.HEARTBEAT_INTERVAL)
                    self.logger.debug(f"[h_thread] Sent heartbeat to Node {nid}")
                except grpc.RpcError:
                    pass
            time.sleep(config.HEARTBEAT_INTERVAL)

    def update_transaction_status(self, seq_num, status, message=""):
        """Update transaction status: A (Accepted), C (Committed), E (Executed), X (No Status)"""
        ranks = {"A": 1, "C": 2, "E": 3, "X": 0}
        current_rank = ranks.get(self.transaction_status.get(seq_num, "X"), 0)
        new_rank = ranks[status]
        
        if status == "X" or new_rank > current_rank:
            self.transaction_status[seq_num] = status
        
        if message and new_rank >= current_rank:
            self.transaction_status_message[seq_num] = message
            if seq_num in self.log:
                self.log[seq_num].status = status
                self.persistence.add_log_entry(seq_num, self.log[seq_num])
            self.persistence.update_transaction_status(seq_num, status, message)

    def get_transaction_status(self, seq_num):
        return self.transaction_status.get(seq_num, "X")
    
    def get_transaction_status_message(self, seq_num):
        return self.transaction_status_message.get(seq_num, "")

    def store_client_reply(self, client_id, reply):
        self.client_replies[client_id] = reply
        self.logger.info(f"Stored reply for client {client_id}: success={reply.success}")

    def get_stored_client_reply(self, client_id, timestamp):
        stored_reply = self.client_replies.get(client_id)
        if stored_reply and stored_reply.request_timestamp == timestamp:
            self.logger.info(f"Found stored reply for client {client_id}, timestamp {timestamp}")
            return stored_reply
        return None

    def add_new_view_to_history(self, new_view_msg):
        view_info = {
            'timestamp': time.time(),
            'ballot_num': (new_view_msg.ballot_num.number, new_view_msg.ballot_num.node_id),
            'accept_log_size': len(new_view_msg.accept_log),
            'accept_log': [self.persistence._serialize_log_entry(entry) for entry in new_view_msg.accept_log],
            'checkpoint_seq': new_view_msg.latest_checkpoint.sequence_num if new_view_msg.latest_checkpoint else 0
        }
        self.new_view_history.append(view_info)
        
        self.persistence.add_new_view(view_info)

    def should_checkpoint(self):
        return (self.last_executed_seq - self.last_checkpoint_seq) >= self.checkpoint_interval

    def create_checkpoint(self):
        if not self.should_checkpoint():
            return None
        
        state_json = json.dumps(self.datastore, sort_keys=True)
        
        state_digest = hashlib.sha256(state_json.encode('utf-8')).digest()
        
        checkpoint_msg = paxos_pb2.CheckpointMsg(
            sequence_num=self.last_executed_seq,
            state_datastore=state_json 
        )
        
        self.latest_checkpoint_state = paxos_pb2.CheckpointState(
            sequence_num=self.last_executed_seq,
            state_digest=state_digest
        )
        
        self.last_checkpoint_seq = self.last_executed_seq
        
        self.persistence.update_checkpoint_state(self.last_executed_seq, self.latest_checkpoint_state)
        
        self.logger.info(f"Created checkpoint at sequence {self.last_executed_seq} with digest: {state_digest.hex()[:16]}...")
        return checkpoint_msg

    def can_execute_transaction(self, seq_num):
        if seq_num == 1:
            return True
        
        prev_seq = seq_num - 1
        
        if prev_seq not in self.log:
            self.logger.info(f"Cannot execute seq {seq_num}: seq {prev_seq} missing")
            return False
        
        prev_status = self.transaction_status.get(prev_seq)
        if prev_status in ["E", "X"]:
            return True
        
        self.logger.info(f"Cannot execute seq {seq_num}: seq {prev_seq} not executed (status: {prev_status})")
        return False

    def execute_transaction_if_ready(self, seq_num, request):
        """Execute a transaction if it's ready (all previous transactions executed)"""
        if not self.can_execute_transaction(seq_num):
            return False, "Not ready"
        
        with self.lock:
            current_status = self.get_transaction_status(seq_num)
            if current_status in ["E", "X"]:
                self.logger.debug(f"Seq {seq_num} already executed (status={current_status}), skipping")
                msg = self.get_transaction_status_message(seq_num) or "Already executed"
                return True, msg
            self.transaction_status[seq_num] = "E"
        
        log_entry = self.log.get(seq_num)
        if not log_entry:
            self.logger.error(f"Cannot execute seq {seq_num}: not in log")
            return False, "Not in log"
        
        phase = log_entry.phase if hasattr(log_entry, 'phase') else ''
        transaction_id = log_entry.transaction_id if hasattr(log_entry, 'transaction_id') else ''
        
        if phase == 'P':
            if self.wal_manager.has_wal_entries(transaction_id):
                self.logger.debug(f"[2PC-PREPARE-EXECUTE] WAL exists for {transaction_id}, skipping DB update")
            else:
                parsed = transaction_utils.parse_transaction(request.transaction)
                if parsed and parsed['type'] == 'TRANSFER':
                    sender, receiver, amount = parsed['sender'], parsed['receiver'], parsed['amount']
                    sender_cluster = config.get_cluster_for_client(sender)
                    receiver_cluster = config.get_cluster_for_client(receiver)
                    
                    if self.cluster_id == sender_cluster:
                        old_bal = self.datastore.get(sender, config.INITIAL_AMOUNT)
                        new_bal = old_bal - amount
                        self.logger.info(f"[2PC-PREPARE-EXECUTE] Debit: {sender} ${old_bal} -> ${new_bal}")
                        self.wal_manager.log_operation(transaction_id, seq_num, sender, old_bal, new_bal, "debit", "P")
                        self.datastore[sender] = new_bal
                        self.persistence.save_datastore_entry(sender, new_bal)
                        
                    elif self.cluster_id == receiver_cluster:
                        old_bal = self.datastore.get(receiver, config.INITIAL_AMOUNT)
                        new_bal = old_bal + amount
                        self.logger.info(f"[2PC-PREPARE-EXECUTE] Credit: {receiver} ${old_bal} -> ${new_bal}")
                        self.wal_manager.log_operation(transaction_id, seq_num, receiver, old_bal, new_bal, "credit", "P")
                        self.datastore[receiver] = new_bal
                        self.persistence.save_datastore_entry(receiver, new_bal)
            
            self.update_transaction_status(seq_num, "E", "2PC-PREPARE")
            self.last_executed_seq = seq_num
            self.persistence.update_last_executed_seq(seq_num)
            return True, "2PC-PREPARE"
        
        elif phase == 'C':
            self.logger.info(f"[2PC-COMMIT-EXECUTE] Finalizing {transaction_id}")
            self.wal_manager.finalize_commit(transaction_id)
            self.lock_manager.release_all_locks_for_transaction(transaction_id)
            
            self.update_transaction_status(seq_num, "E", "Transaction Success")
            self.last_executed_seq = seq_num
            self.persistence.update_last_executed_seq(seq_num)
            return True, "Transaction Success"
        
        elif phase == 'A':
            self.logger.info(f"[2PC-ABORT] Seq {seq_num}, txid={transaction_id}")
            
            if self.wal_manager.has_wal_entries(transaction_id):
                self.logger.info(f"[2PC-ABORT] Rolling back from WAL for txid={transaction_id}")
                self.wal_manager.rollback_transaction(transaction_id, self.datastore)
            
            self.lock_manager.release_all_locks_for_transaction(transaction_id)
            
            existing_msg = self.get_transaction_status_message(seq_num)
            msg = existing_msg if existing_msg and not existing_msg.startswith("2PC-") else "Transaction aborted"
            
            self.update_transaction_status(seq_num, "E", msg)
            self.last_executed_seq = seq_num
            self.persistence.update_last_executed_seq(seq_num)
            return True, msg
        
        success = None
        msg = ""
        
        if not hasattr(request, 'transaction') or not request.transaction:
            self.logger.info(f"Executing NO-OP at seq {seq_num}")
            msg = "NO-OP"
            self.update_transaction_status(seq_num, "X", msg)
            success = True
        else:
            parsed = transaction_utils.parse_transaction(request.transaction)
            
            if not parsed or parsed['type'] != 'TRANSFER':
                self.logger.error(f"Invalid transaction at seq {seq_num}: {request.transaction}")
                self.update_transaction_status(seq_num, "E", "Invalid transaction")
                success = False
                msg = "Invalid transaction"
            else:
                sender = parsed['sender']
                receiver = parsed['receiver']
                amount = parsed['amount']
                

                if self.datastore.get(sender, 0) >= amount:
                    self.datastore[sender] = self.datastore.get(sender, config.INITIAL_AMOUNT) - amount
                    self.datastore[receiver] = self.datastore.get(receiver, config.INITIAL_AMOUNT) + amount
                    
                    self.logger.info(f"Executed transaction: {request.transaction} at seq {seq_num}")
                    msg = "Transaction Success"
                    self.update_transaction_status(seq_num, "E", msg)
                    success = True
                    
                    self.persistence.update_datastore(self.datastore)
                else:
                    self.logger.info(f"Transaction failed (insufficient balance): {request.transaction} at seq {seq_num}")
                    msg = "Insufficient Balance"
                    self.update_transaction_status(seq_num, "E", msg)
                    success = False
            
        
        self.last_executed_seq = seq_num
        self.persistence.update_last_executed_seq(seq_num)
        
        if self.role == 'LEADER' and self.should_checkpoint():
            self.logger.info(f"Triggering checkpoint after executing seq {seq_num}")
            checkpoint_msg = self.create_checkpoint()
            if checkpoint_msg:
                for stub in self.peers.values():
                    try:
                        stub.SendCheckpoint(checkpoint_msg)
                        self.logger.info(f"Sent checkpoint to peer")
                    except grpc.RpcError as e:
                        self.logger.info(f"Failed to send checkpoint: {e}")
        
        return success, msg

    def try_execute_pending_transactions(self):
        executed_any = True
        total_executed = 0
        
        while executed_any:
            executed_any = False
            
            with self.lock:
                next_seq = self.last_executed_seq + 1
                sequences_to_execute = []
                while next_seq in self.log and self.get_transaction_status(next_seq) == "C":
                    sequences_to_execute.append((next_seq, self.log[next_seq].request))
                    next_seq += 1
            
            for seq_num, request in sequences_to_execute:
                success, _ = self.execute_transaction_if_ready(seq_num, request)
                if success:
                    executed_any = True
                    total_executed += 1
                else:
                    break
        
        if total_executed > 0:
            self.logger.debug(f"Successfully executed {total_executed} pending transactions")

    def signal_pending_execution(self):
        self._pending_execution_event.set()
    
    def _background_execution_loop(self):
        while self._execution_thread_running:
            self._pending_execution_event.wait(timeout=0.1)
            self._pending_execution_event.clear()
            
            if not self._execution_thread_running:
                break
            
            try:
                self.try_execute_pending_transactions()
            except Exception as e:
                self.logger.error(f"Background execution error: {e}")

    def _recover_from_checkpoint(self):
        self.logger.info(f"Attempting checkpoint recovery for seq {self.last_checkpoint_seq}")
        
        for peer_id, stub in self.peers.items():
            try:
                request = paxos_pb2.CheckpointRequest(sequence_num=self.last_checkpoint_seq)
                response = stub.RequestCheckpoint(request, timeout=5.0)
                
                if response.has_checkpoint:
                    self.logger.info(f"Received recovery checkpoint from Node {peer_id}")
                    success = self.apply_received_checkpoint(response.checkpoint)
                    if success:
                        return True
            except grpc.RpcError as e:
                self.logger.info(f"Failed to get recovery checkpoint from Node {peer_id}: {e}")
                continue
        
        self.logger.info("Could not obtain recovery checkpoint from any node")
        return False

    def request_missing_checkpoint(self, target_seq_num):
        self.logger.info(f"Requesting checkpoint for sequence {target_seq_num}")
        
        for peer_id, stub in self.peers.items():
            try:
                request = paxos_pb2.CheckpointRequest(sequence_num=target_seq_num)
                response = stub.RequestCheckpoint(request, timeout=2.0)
                
                if response.has_checkpoint:
                    self.logger.info(f"Received checkpoint from Node {peer_id}")
                    self.apply_received_checkpoint(response.checkpoint)
                    return True
            except grpc.RpcError as e:
                self.logger.info(f"Failed to get checkpoint from Node {peer_id}: {e}")
                continue
        
        self.logger.info("Could not obtain checkpoint from any node")
        return False

    def apply_received_checkpoint(self, checkpoint_msg):
        try:
            with self.lock:
                self.datastore = json.loads(checkpoint_msg.state_datastore)
                self.last_checkpoint_seq = checkpoint_msg.sequence_num
                
                state_digest = hashlib.sha256(checkpoint_msg.state_datastore.encode('utf-8')).digest()
                self.latest_checkpoint_state = paxos_pb2.CheckpointState(
                    sequence_num=checkpoint_msg.sequence_num,
                    state_digest=state_digest
                )
                
                to_remove = [seq for seq in self.log.keys() if seq <= checkpoint_msg.sequence_num]
                for seq in to_remove:
                    del self.log[seq]
                    if seq in self.transaction_status:
                        del self.transaction_status[seq]
                
                self.last_executed_seq = max(self.last_executed_seq, checkpoint_msg.sequence_num)
            
            self.persistence.update_datastore(self.datastore)
            self.persistence.update_checkpoint_state(checkpoint_msg.sequence_num, self.latest_checkpoint_state)
            self.persistence.update_last_executed_seq(self.last_executed_seq)
            self.persistence.remove_log_entries(to_remove)
            self.persistence.remove_transaction_status(to_remove)
            
            self.logger.info(f"Applied received checkpoint at sequence {checkpoint_msg.sequence_num}")
            return True
        except Exception as e:
            self.logger.info(f"Error applying received checkpoint: {e}")
            return False
    
    def run_paxos_accept_phase(self, transaction_str: str, original_request, 
                                phase: str = None, transaction_id: str = None,
                                transaction_type: str = None) -> int:
        if self.role != 'LEADER':
            self.logger.error("Only LEADER can run Paxos accept phase")
            return None
        
        with self.lock:
            self.sequence_num += 1
            seq_num = self.sequence_num
            
            log_entry = paxos_pb2.LogEntry(
                ballot_num=self.ballot_num,
                sequence_num=seq_num,
                request=original_request,
                is_noop=False,
                status='A',
                phase=phase or '',
                transaction_type=transaction_type or 'INTRA',
                transaction_id=transaction_id or ''
            )
            
            self.log[seq_num] = log_entry
            self.transaction_status[seq_num] = 'A'
            
            self.persistence.add_log_entry(seq_num, log_entry)
            self.persistence.update_transaction_status(seq_num, 'A', '')
            self.persistence.update_sequence_num(seq_num)
            
            self.logger.debug(
                f"Created log entry seq={seq_num}, phase={phase}, txid={transaction_id}"
            )
        
        accept_msg = paxos_pb2.AcceptMsg(
            ballot_num=self.ballot_num,
            sequence_num=seq_num,
            request=original_request,
            phase=phase or '',
            transaction_id=transaction_id or ''
        )
        
        accepted_count = 1
        peer_responses = []
        
        self.logger.info(f"[PAXOS-ACCEPT] Broadcasting ACCEPT for seq={seq_num} to {len(self.peers)} peers (need quorum={config.QUORUM_SIZE})")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.peers)) as executor:
            futures = {
                executor.submit(peer.SendAccept, accept_msg, timeout=2.0): peer_id
                for peer_id, peer in self.peers.items()
            }
            
            try:
                for future in concurrent.futures.as_completed(futures, timeout=1.0):
                    peer_id = futures[future]
                    try:
                        response = future.result()
                        if response.sequence_num == seq_num:
                            accepted_count += 1
                            peer_responses.append(f"Node {peer_id}: ACCEPTED")
                            self.logger.info(f"[PAXOS-ACCEPT] Node {peer_id} accepted seq={seq_num}")
                        else:
                            peer_responses.append(f"Node {peer_id}: REJECTED")
                            self.logger.warning(f"[PAXOS-ACCEPT] Node {peer_id} rejected seq={seq_num}")
                    except Exception as e:
                        peer_responses.append(f"Node {peer_id}: ERROR")
                        self.logger.debug(f"[PAXOS-ACCEPT] Node {peer_id} failed: {e}")
            except concurrent.futures.TimeoutError:
                self.logger.warning(f"[PAXOS-ACCEPT] Timeout waiting for ACCEPT responses for seq={seq_num}")
        
 
        if accepted_count >= config.QUORUM_SIZE:
            self.logger.info(f"[PAXOS-ACCEPT] QUORUM ACHIEVED for seq={seq_num}, {accepted_count}/{len(self.peers)+1} nodes accepted")
            return seq_num
        else:
            self.logger.error(f"[PAXOS-ACCEPT] QUORUM NOT ACHIEVED for seq={seq_num}, {accepted_count}/{len(self.peers)+1} nodes accepted")
            return None
    
    def run_paxos_commit_phase(self, seq_num: int, phase: str = None, transaction_id: str = None) -> bool:

        if self.role != 'LEADER':
            self.logger.error("Only LEADER can run Paxos commit phase")
            return False
        
        with self.lock:
            if seq_num not in self.log:
                self.logger.error(f"Cannot commit seq={seq_num}: not in log")
                return False
            
            log_entry = self.log[seq_num]
            
            self.transaction_status[seq_num] = 'C'
            self.persistence.update_transaction_status(seq_num, 'C', '')
            
            self.logger.debug(f"Committed seq={seq_num}, phase={phase}, txid={transaction_id}")
        
        commit_msg = paxos_pb2.CommitMsg(
            ballot_num=self.ballot_num,
            sequence_num=seq_num,
            request=log_entry.request,
            phase=phase or ''
        )
        
        self.logger.debug(f"[PAXOS-COMMIT] Broadcasting COMMIT for seq={seq_num} to {len(self.peers)} peers")
        
        def send_commit_async(peer, peer_id):
            try:
                peer.SendCommit(commit_msg, timeout=1.0)
            except Exception:
                pass
        
        for peer_id, peer in self.peers.items():
            threading.Thread(target=send_commit_async, args=(peer, peer_id), daemon=True).start()
        
        self.try_execute_pending_transactions()
        
        return True
    
    def is_cross_shard(self, sender: int, receiver: int) -> bool:
        return transaction_utils.is_cross_shard(sender, receiver)
    
    def handle_balance_request(self, client_id: int) -> tuple:
        if self.is_idle:
            return False, -1, "Node is IDLE"
        
        if self.role != 'LEADER':
            return False, -1, "Not leader"
        
        with self.lock:
            if client_id not in self.datastore:
                return False, 0.0, f"Client {client_id} not found"
            
            balance = self.datastore[client_id]
        
        self.logger.debug(f"Balance query: client={client_id}, balance={balance}")
        return True, balance, "Success"
    
    def get_stub_for_node(self, node_id: int):
        if node_id not in self.stubs:
            address = config.NODE_ADDRESSES[node_id]
            channel = grpc.insecure_channel(address)
            self.stubs[node_id] = paxos_pb2_grpc.PaxosServiceStub(channel)
        
        return self.stubs[node_id]
    
    def handle_cross_shard_transaction(self, sender: int, receiver: int, 
                                       amount: float, original_request):
        txid = transaction_utils.generate_transaction_id(sender)
        
        sender_cluster = config.get_cluster_for_client(sender)
        receiver_cluster = config.get_cluster_for_client(receiver)
        
        self.logger.info(f"2PC: txid={txid}, sender={sender}@C{sender_cluster}, receiver={receiver}@C{receiver_cluster}, amount={amount}")
        
        # Create 2PC state
        self.persistence.create_2pc_state(txid, sender, receiver, amount, sender_cluster, receiver_cluster)
        
        # Phase 1: PREPARE
        seq_num = self._2pc_phase1_coordinator(txid, sender, receiver, amount, original_request, receiver_cluster)
        if seq_num is None:
            return False, "Transaction aborted in Phase 1"
        
        # Phase 2: COMMIT
        success = self._2pc_phase2_coordinator(txid, seq_num, receiver_cluster)
        if not success:
            return False, "Transaction aborted in Phase 2"
        
        # Cleanup
        self.lock_manager.release_all_locks_for_transaction(txid)
        self.wal_manager.finalize_commit(txid)
        self.persistence.delete_2pc_state(txid)
        
        return True, "Transaction Success"
    
    def _2pc_phase1_coordinator(self, txid, sender, receiver, amount, original_request, receiver_cluster):
        lock_acquired = False
        seq_num = None
        
        try:
            self.logger.info(f"[2PC-COORDINATOR-PREPARE] Starting Phase 1 for txid={txid}")
            
            # Step 1: Lock sender
            self.logger.info(f"[2PC-COORDINATOR-PREPARE] Attempting to lock sender {sender}")
            if not self.lock_manager.try_acquire_lock(sender, txid):
                lock_owner = self.lock_manager.get_lock_owner(sender)
                self.logger.warning(f"[2PC-COORDINATOR-PREPARE] Sender {sender} locked by {lock_owner}")
                return None
            
            lock_acquired = True
            self.logger.info(f"[2PC-COORDINATOR-PREPARE] Lock acquired on sender {sender}")
            
            # Step 2: Check funds BEFORE running Paxos (avoid wasting Paxos round)
            old_balance = self.datastore.get(sender, config.INITIAL_AMOUNT)
            self.logger.info(f"[2PC-COORDINATOR-PREPARE] Checking funds (sender={sender}, balance=${old_balance}, amount=${amount})")
            if old_balance < amount:
                self.logger.warning(f"[2PC-COORDINATOR-PREPARE] Insufficient funds")
                self.lock_manager.release_lock(sender, txid)
                return None
            
            self.logger.info(f"[2PC-COORDINATOR-PREPARE] Sufficient funds available")
            
            # Step 3: Run Paxos ACCEPT phase with phase='P'
            self.logger.info(f"[2PC-COORDINATOR-PREPARE] Running Paxos ACCEPT phase for phase='P'")
            transaction_str = transaction_utils.format_transaction(sender, receiver, amount)
            seq_num = self.run_paxos_accept_phase(transaction_str, original_request, phase='P', 
                                                   transaction_id=txid, transaction_type='CROSS')
            
            if seq_num is None:
                self.logger.error(f"[2PC-COORDINATOR-PREPARE] Paxos ACCEPT failed (no quorum)")
                self.lock_manager.release_lock(sender, txid)
                return None
            
            self.logger.info(f"[2PC-COORDINATOR-PREPARE] Paxos ACCEPT successful for seq={seq_num}")
            
            # Step 4: Run Paxos COMMIT phase (DECIDE) - this triggers WAL + tentative debit on all nodes
            self.logger.info(f"[2PC-COORDINATOR-PREPARE] Running Paxos COMMIT phase (DECIDE)")
            self.run_paxos_commit_phase(seq_num, phase='P', transaction_id=txid)
            
            # Step 5: Execute on LEADER (followers execute in their SendCommit handler)
            if self.can_execute_transaction(seq_num):
                with self.lock:
                    log_entry = self.log.get(seq_num)
                if log_entry:
                    self.execute_transaction_if_ready(seq_num, log_entry.request)
            
     
            # Step 6: Send PREPARE to participant cluster (try all nodes if needed)
            self.logger.info(f"[2PC-COORDINATOR-PREPARE] Sending 2PC PREPARE message to participant cluster {receiver_cluster}")
            prepare_msg = paxos_pb2.Prepare2PCMsg(
                transaction_id=txid, sender=sender, receiver=receiver, amount=amount,
                coordinator_cluster=self.cluster_id, participant_cluster=receiver_cluster,
                original_request=original_request, timestamp=int(time.time() * 1000)
            )
            
            # Try sending to nodes in participant cluster (no retries - client will retry)
            participant_nodes = config.get_nodes_in_cluster(receiver_cluster)
            self.logger.info(f"[2PC-COORDINATOR-PREPARE] Participant nodes: {participant_nodes}")
            response = None
            
            for node_id in participant_nodes:
                try:
                    stub = self.get_stub_for_node(node_id)
                    response = stub.SendPrepare2PC(prepare_msg, timeout=config.TWOPC_TIMEOUT)
                    
                    if response and response.prepared:
                        # SUCCESS
                        self.logger.info(f"[2PC-COORDINATOR-PREPARE] 2PC PREPARED received from participant cluster {receiver_cluster} on Node {node_id}")
                        break
                    elif response and ("no leader" in response.message.lower() or "No leader" in response.message or "Not leader" in response.message or "IDLE" in response.message):
                        # Node is not leader, try next node
                        self.logger.debug(f"Node {node_id} is not leader, trying next node")
                        continue
                    elif response and ("Quorum not achieved" in response.message or "Paxos failed" in response.message):
                        # Participant cluster doesn't have quorum - fail fast, client will retry
                        self.logger.warning(f"Participant cluster has no quorum: {response.message}")
                        self._2pc_abort(txid, seq_num, f"Participant cluster unavailable: {response.message}",
                                      participant_cluster=receiver_cluster, participant_prepared=False)
                        return None
                    elif response and ("locked" in response.message.lower()):
                        # Lock conflict - fail fast, client will retry
                        self._2pc_abort(txid, seq_num, f"Participant rejected: {response.message}",
                                      participant_cluster=receiver_cluster, participant_prepared=False)
                        return None
                    elif response:
                        # Other rejection (insufficient funds, etc.)
                        self._2pc_abort(txid, seq_num, f"Participant rejected: {response.message}",
                                      participant_cluster=receiver_cluster, participant_prepared=False)
                        return None
                except grpc.RpcError as e:
                    self.logger.debug(f"Failed to contact Node {node_id}: {e}")
                    continue
            
            if not response or not response.prepared:
                self.logger.warning(f"[2PC-COORDINATOR-PREPARE] No response from participant - client will retry")
                self._2pc_abort(txid, seq_num, "Participant cluster unavailable",
                              participant_cluster=receiver_cluster, participant_prepared=False)
                return None
            
            self.logger.info(f"[2PC-COORDINATOR-PREPARE] Phase 1 COMPLETE: Lock held, WAL written, tentative debit applied, PREPARED received")
            return seq_num  # Lock still held (will be released in phase 2)
            
        except Exception as e:
            self.logger.error(f"Exception in 2PC Phase 1: {e}")
            if lock_acquired and seq_num:
                self._2pc_abort(txid, seq_num, f"Exception: {e}",
                              participant_cluster=receiver_cluster, participant_prepared=False)
            elif lock_acquired:
                # Lock was acquired but Paxos failed with exception - just release lock
                self.lock_manager.release_lock(sender, txid)
                self.logger.info(f"Released lock on sender {sender} after exception")
            return None
    
    def _2pc_phase2_coordinator(self, txid, seq_num, receiver_cluster):
        self.logger.info(f"[2PC-COORDINATOR-COMMIT] Starting Phase 2 for txid={txid}, seq={seq_num}")
        
        # Step 1: Update log entry from phase='P' to phase='C' and run full Paxos
        self.logger.info(f"[2PC-COORDINATOR-COMMIT] Updating log entry seq={seq_num} to phase='C'")
        
        with self.lock:
            if seq_num not in self.log:
                self.logger.error(f"Cannot run COMMIT: seq={seq_num} not in log")
                return False
            
            # Update the log entry in self.log
            self.log[seq_num].phase = 'C'
            self.log[seq_num].status = 'A'
            
            self.transaction_status[seq_num] = 'A'
            self.transaction_status_message[seq_num] = ''
            
            # Persist the updated log entry
            self.persistence.add_log_entry(seq_num, self.log[seq_num])
            self.persistence.update_transaction_status(seq_num, 'A', '')
            
            # Get log entry for ACCEPT message
            log_entry = self.log[seq_num]
        
        # Step 2: Run full Paxos ACCEPT phase for phase='C'
        self.logger.info(f"[2PC-COORDINATOR-COMMIT] Running Paxos ACCEPT phase for phase='C'")
        
        # Manually broadcast ACCEPT with phase='C' (reusing same seq)
        accept_msg = paxos_pb2.AcceptMsg(
            ballot_num=self.ballot_num,
            sequence_num=seq_num,
            request=log_entry.request,
            phase='C',
            transaction_id=txid
        )
        
        accepted_count = 1
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.peers)) as executor:
            futures = {
                executor.submit(peer.SendAccept, accept_msg, timeout=2.0): peer_id
                for peer_id, peer in self.peers.items()
            }
            
            try:
                for future in concurrent.futures.as_completed(futures, timeout=1.0):
                    peer_id = futures[future]
                    try:
                        response = future.result()
                        if response.sequence_num == seq_num:
                            accepted_count += 1
                    except Exception as e:
                        self.logger.debug(f"[2PC-COORDINATOR-COMMIT] Node {peer_id} failed ACCEPT: {e}")
            except concurrent.futures.TimeoutError:
                self.logger.warning(f"[2PC-COORDINATOR-COMMIT] Timeout waiting for ACCEPT responses")
        
        if accepted_count < config.QUORUM_SIZE:
            self.logger.error(f"[2PC-COORDINATOR-COMMIT] QUORUM NOT ACHIEVED for phase='C'")
            return False
        
        self.logger.info(f"[2PC-COORDINATOR-COMMIT] Paxos ACCEPT quorum achieved ({accepted_count}/{len(self.peers)+1})")
        
        # Step 3: Run Paxos COMMIT phase (DECIDE)
        self.logger.info(f"[2PC-COORDINATOR-COMMIT] Running Paxos COMMIT phase (DECIDE)")
        self.run_paxos_commit_phase(seq_num, phase='C', transaction_id=txid)
        
        # Step 3.5: Execute on LEADER and try all pending transactions
        if self.can_execute_transaction(seq_num):
            with self.lock:
                log_entry = self.log.get(seq_num)
            if log_entry:
                self.execute_transaction_if_ready(seq_num, log_entry.request)
        
        # Ensure any pending transactions are executed (handles out-of-order commits)
        self.try_execute_pending_transactions()
        
        self.logger.info(f"[2PC-COORDINATOR-COMMIT] Paxos COMMIT complete - all nodes will clear WAL + release locks")
        
        # Step 4: Send COMMIT to participant cluster (try all nodes if needed)
        self.logger.info(f"[2PC-COORDINATOR-COMMIT] Sending COMMIT to participant cluster {receiver_cluster}")
        commit_msg = paxos_pb2.Commit2PCMsg(transaction_id=txid, coordinator_cluster=self.cluster_id)
        participant_nodes = config.get_nodes_in_cluster(receiver_cluster)
        commit_success = False
        
        for retry in range(config.TWOPC_RETRY_COUNT):
            if retry > 0:
                self.logger.info(f"[2PC-COORDINATOR-COMMIT] Retry {retry}/{config.TWOPC_RETRY_COUNT} sending 2PC COMMIT message to participant cluster {receiver_cluster}")
            
            for node_id in participant_nodes:
                try:
                    stub = self.get_stub_for_node(node_id)
                    response = stub.SendCommit2PC(commit_msg, timeout=config.TWOPC_TIMEOUT)
                    if response and response.success:
                        self.logger.info(f"[2PC-COORDINATOR-COMMIT] ACK received from cluster {receiver_cluster} : Node {node_id}")
                        commit_success = True
                        break
                except grpc.RpcError as e:
                    self.logger.debug(f"[2PC-COORDINATOR-COMMIT] Failed to contact cluster {receiver_cluster} : Node {node_id}: {e}")
                    continue
            
            if commit_success:
                break
            
            if retry < config.TWOPC_RETRY_COUNT - 1:
                time.sleep(0.5)
        
        if not commit_success:
            self.logger.error(f"[2PC-COORDINATOR-COMMIT] Failed to confirm COMMIT with participant after retries")
        else:
            self.logger.info(f"[2PC-COORDINATOR-COMMIT] Phase 2 COMPLETE: Transaction finalized on both clusters")
        
        return True
    
    def _2pc_abort(self, txid, seq_num, reason, participant_cluster=None, participant_prepared=False):
        self.logger.warning(f"[2PC-COORDINATOR-ABORT] Aborting txid={txid}: {reason}")
        self.logger.info(f"[2PC-COORDINATOR-ABORT] Participant prepared: {participant_prepared}")
        
        # Broadcast ABORT to all peers in this cluster (no consensus, just cleanup)
        abort_cleanup_msg = paxos_pb2.Abort2PCMsg(
            transaction_id=txid,
            coordinator_cluster=self.cluster_id
        )
        
        self.logger.info(f"[2PC-COORDINATOR-ABORT] Broadcasting ABORT to all peers in cluster (fire-and-forget)")
        def send_abort_async(peer_id, peer):
            try:
                peer.SendAbort2PC(abort_cleanup_msg, timeout=0.5)
                self.logger.debug(f"[2PC-COORDINATOR-ABORT] Sent ABORT cleanup to Node {peer_id}")
            except grpc.RpcError as e:
                self.logger.debug(f"[2PC-COORDINATOR-ABORT] Failed to send ABORT to Node {peer_id}: {e}")
        
        for peer_id, peer in self.peers.items():
            thread = threading.Thread(target=send_abort_async, args=(peer_id, peer), daemon=True)
            thread.start()
        
        self._local_2pc_abort_cleanup(txid, seq_num, abort_reason=reason)
        self.logger.info(f"[2PC-COORDINATOR-ABORT] cleanup complete for coordinator cluster")
        
        if participant_prepared and participant_cluster:
            self.logger.info(f"[2PC-COORDINATOR-ABORT] Sending ABORT to ALL nodes in participant cluster {participant_cluster} (fire-and-forget)")
            abort_msg = paxos_pb2.Abort2PCMsg(transaction_id=txid, coordinator_cluster=self.cluster_id)
            participant_nodes = config.get_nodes_in_cluster(participant_cluster)
            
            def send_participant_abort_async(node_id):
                try:
                    stub = self.get_stub_for_node(node_id)
                    response = stub.SendAbort2PC(abort_msg, timeout=0.5)
                    self.logger.debug(f"[2PC-COORDINATOR-ABORT] Sent ABORT to participant Node {node_id}")
                except grpc.RpcError as e:
                    self.logger.debug(f"[2PC-COORDINATOR-ABORT] Failed to send ABORT to Node {node_id}: {e}")
            
            for node_id in participant_nodes:
                thread = threading.Thread(target=send_participant_abort_async, args=(node_id,), daemon=True)
                thread.start()

        elif participant_cluster:
            self.logger.info(f"[2PC-COORDINATOR-ABORT] Skipping ABORT to participant (it never prepared)")
        
        self.logger.info(f"[2PC-COORDINATOR-ABORT] Abort complete for txid={txid}")
    
    # ==================== 2PC PARTICIPANT METHODS ====================
    
    def handle_2pc_prepare(self, prepare_msg):
        txid = prepare_msg.transaction_id
        receiver = prepare_msg.receiver
        amount = prepare_msg.amount
        lock_acquired = False
        seq_num = None
        
        self.logger.info(f"[2PC-PARTICIPANT-PREPARE] Starting Phase 1 for txid={txid}, receiver={receiver}, amount={amount}")
        
        try:
            # Step 1: Lock receiver
            self.logger.info(f"[2PC-PARTICIPANT-PREPARE] Attempting to lock receiver {receiver}")
            if not self.lock_manager.try_acquire_lock(receiver, txid):
                lock_owner = self.lock_manager.get_lock_owner(receiver)
                self.logger.warning(f"[2PC-PARTICIPANT-PREPARE] FAILED: Receiver {receiver} locked by {lock_owner}")
                return False, f"Receiver {receiver} is locked"
            
            lock_acquired = True
            self.logger.info(f"[2PC-PARTICIPANT-PREPARE] Lock acquired on receiver {receiver}")
            
            # Step 2: Run Paxos ACCEPT phase with phase='P'
            self.logger.info(f"[2PC-PARTICIPANT-PREPARE] Running Paxos ACCEPT phase (phase='P')")
            transaction_str = transaction_utils.format_transaction(prepare_msg.sender, receiver, amount)
            seq_num = self.run_paxos_accept_phase(transaction_str, prepare_msg.original_request,
                                                   phase='P', transaction_id=txid, transaction_type='CROSS')
            
            if seq_num is None:
                # Paxos ACCEPT failed (likely no quorum) - release lock and return failure
                self.lock_manager.release_lock(receiver, txid)
                self.logger.warning(f"[2PC-PARTICIPANT-PREPARE] FAILED: Paxos ACCEPT quorum not achieved for txid={txid}")
                return False, "Quorum not achieved"
            
            self.logger.info(f"[2PC-PARTICIPANT-PREPARE] Paxos ACCEPT successful for seq={seq_num}")
            
            # Step 3: Run Paxos COMMIT phase (DECIDE) - this triggers WAL + tentative credit on all nodes
            self.logger.info(f"[2PC-PARTICIPANT-PREPARE] Running Paxos COMMIT phase (DECIDE)")
            self.run_paxos_commit_phase(seq_num, phase='P', transaction_id=txid)
            
            # Step 3.5: Execute on LEADER (followers execute in their SendCommit handler)
            if self.can_execute_transaction(seq_num):
                with self.lock:
                    log_entry = self.log.get(seq_num)
                if log_entry:
                    self.execute_transaction_if_ready(seq_num, log_entry.request)
            
            self.try_execute_pending_transactions()
            
            self.logger.info(f"[2PC-PARTICIPANT-PREPARE] Paxos COMMIT complete - all nodes applied WAL + tentative credit")
            
            # Step 4: Store 2PC state
            self.logger.info(f"[2PC-PARTICIPANT-PREPARE] Storing 2PC state in DB")
            self.persistence.create_2pc_state(txid, prepare_msg.sender, receiver, amount,
                                              prepare_msg.coordinator_cluster, self.cluster_id)
            self.persistence.update_2pc_state(txid, 'PREPARED', seq_num)
            
            self.logger.info(f"[2PC-PARTICIPANT-PREPARE] SUCCESS: Phase 1 complete for txid={txid}, seq={seq_num}")
            return True, "PREPARED"
            
        except Exception as e:
            self.logger.error(f"Exception in 2PC PREPARE handler: {e}")
            if lock_acquired and seq_num:
                self.logger.warning(f"Rolling back transaction {txid} due to exception")
                self.wal_manager.rollback_transaction(txid, self.datastore)
                self.lock_manager.release_lock(receiver, txid)
            elif lock_acquired:
                self.lock_manager.release_lock(receiver, txid)
                self.logger.info(f"Released lock on receiver {receiver} after exception")
            return False, f"Exception: {str(e)}"
    
    def handle_2pc_commit(self, commit_msg):
        txid = commit_msg.transaction_id
        self.logger.info(f"[2PC-PARTICIPANT-COMMIT] Received COMMIT for txid={txid}")
        
        state = self.persistence.get_2pc_state(txid)
        
        if not state:
            self.logger.warning(f"[2PC-PARTICIPANT-COMMIT] Transaction {txid} not found in 2PC state")
            return False, "Transaction not found"
        
        seq_num = state['sequence_num']
        
        # Step 1: Update log entry from phase='P' to phase='C' and run full Paxos
        self.logger.info(f"[2PC-PARTICIPANT-COMMIT] Updating log entry seq={seq_num} to phase='C'")
        
        with self.lock:
            if seq_num not in self.log:
                self.logger.error(f"Cannot run COMMIT: seq={seq_num} not in log")
                return False, "Sequence not found"
            
            # Update the log entry in self.log (not a copy)
            self.log[seq_num].phase = 'C'
            self.log[seq_num].status = 'A'
            

            self.transaction_status[seq_num] = 'A'
            self.transaction_status_message[seq_num] = ''
            
            # Persist the updated log entry
            self.persistence.add_log_entry(seq_num, self.log[seq_num])
            self.persistence.update_transaction_status(seq_num, 'A', '')
            
            # Get log entry for ACCEPT message
            log_entry = self.log[seq_num]
        
        # Step 2: Run full Paxos ACCEPT phase for phase='C'
        self.logger.info(f"[2PC-PARTICIPANT-COMMIT] Running Paxos ACCEPT phase for phase='C'")
        
        accept_msg = paxos_pb2.AcceptMsg(
            ballot_num=self.ballot_num,
            sequence_num=seq_num,
            request=log_entry.request,
            phase='C',
            transaction_id=txid
        )
        
        accepted_count = 1
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.peers)) as executor:
            futures = {
                executor.submit(peer.SendAccept, accept_msg, timeout=2.0): peer_id
                for peer_id, peer in self.peers.items()
            }
            
            try:
                for future in concurrent.futures.as_completed(futures, timeout=1.0):
                    peer_id = futures[future]
                    try:
                        response = future.result()
                        if response.sequence_num == seq_num:
                            accepted_count += 1
                    except Exception as e:
                        self.logger.debug(f"[2PC-PARTICIPANT-COMMIT] Node {peer_id} failed ACCEPT: {e}")
            except concurrent.futures.TimeoutError:
                self.logger.warning(f"[2PC-PARTICIPANT-COMMIT] Timeout waiting for ACCEPT responses")
        
        if accepted_count < config.QUORUM_SIZE:
            self.logger.error(f"[2PC-PARTICIPANT-COMMIT] QUORUM NOT ACHIEVED for phase='C'")
            return False, "Quorum not achieved"
        
        self.logger.info(f"[2PC-PARTICIPANT-COMMIT] Paxos ACCEPT quorum achieved ({accepted_count}/{len(self.peers)+1})")
        
        # Step 3: Run Paxos COMMIT phase (DECIDE)
        self.logger.info(f"[2PC-PARTICIPANT-COMMIT] Running Paxos COMMIT phase (DECIDE)")
        self.run_paxos_commit_phase(seq_num, phase='C', transaction_id=txid)
        
        if self.can_execute_transaction(seq_num):
            with self.lock:
                log_entry = self.log.get(seq_num)
            if log_entry:
                self.execute_transaction_if_ready(seq_num, log_entry.request)
        
        # Ensure any pending transactions are executed
        self.try_execute_pending_transactions()
        
        self.logger.info(f"[2PC-PARTICIPANT-COMMIT] Paxos COMMIT complete - all nodes will clear WAL + release locks")
        
        # Step 4: Delete 2PC state
        self.persistence.delete_2pc_state(txid)
        
        self.logger.info(f"[2PC-PARTICIPANT-COMMIT] Phase 2 COMPLETE: Transaction finalized")
        return True, "Committed"
    
    def handle_2pc_abort(self, abort_msg):
        txid = abort_msg.transaction_id
        self.logger.info(f"[2PC-PARTICIPANT-ABORT] Received ABORT for txid={txid}")
        
        state = self.persistence.get_2pc_state(txid)
        seq_num = state['sequence_num'] if state else None
        
        abort_reason = abort_msg.reason if hasattr(abort_msg, 'reason') and abort_msg.reason else "Transaction aborted"
        self._local_2pc_abort_cleanup(txid, seq_num, abort_reason=abort_reason)
        
        self.logger.info(f"[2PC-PARTICIPANT-ABORT] Abort complete for txid={txid}")
        return True, "Aborted"
    
    def _local_2pc_abort_cleanup(self, txid, seq_num, abort_reason="Transaction aborted"):
  
        self.logger.info(f"[2PC-ABORT-CLEANUP] Starting cleanup for txid={txid}, seq={seq_num}")
        
        # Check if this node has state for this transaction
        if seq_num and seq_num not in self.log:
            self.logger.info(f"[2PC-ABORT-CLEANUP] Seq {seq_num} not in log, nothing to cleanup")
            return
        
        # Rollback from WAL if entries exist
        if self.wal_manager.has_wal_entries(txid):
            self.logger.info(f"[2PC-ABORT-CLEANUP] Rolling back from WAL for txid={txid}")
            self.wal_manager.rollback_transaction(txid, self.datastore)
            self.logger.info(f"[2PC-ABORT-CLEANUP] WAL rollback complete")
        else:
            self.logger.info(f"[2PC-ABORT-CLEANUP] No WAL entries to rollback for txid={txid}")
        
        # Release locks
        self.logger.info(f"[2PC-ABORT-CLEANUP] Releasing locks for txid={txid}")
        self.lock_manager.release_all_locks_for_transaction(txid)
        
        # Update log entry phase to 'A' and mark as executed
        if seq_num:
            with self.lock:
                if seq_num in self.log:
                    self.log[seq_num].phase = 'A'
                    self.persistence.add_log_entry(seq_num, self.log[seq_num])
                    self.logger.info(f"[2PC-ABORT-CLEANUP] Updated seq={seq_num} phase to 'A'")
                
                # Update status with business reason for abort (not "2PC-ABORT")
                self.update_transaction_status(seq_num, "E", abort_reason)
                self.last_executed_seq = seq_num
                self.persistence.update_last_executed_seq(seq_num)
            
            self.logger.info(f"[2PC-ABORT-CLEANUP] Transaction seq={seq_num} marked as executed (aborted)")
            
            # Trigger execution of pending transactions
            self.try_execute_pending_transactions()
        
        # Delete 2PC state
        self.persistence.delete_2pc_state(txid)
        
        self.logger.info(f"[2PC-ABORT-CLEANUP] Cleanup complete for txid={txid}")
    
    def _get_cluster_leader(self, cluster_id):
        nodes = config.get_nodes_in_cluster(cluster_id)
        best_leader = None
        best_ballot = (-1, -1)
        
        for node_id in nodes:
            try:
                stub = self.get_stub_for_node(node_id)
                info = stub.GetNodeInfo(paxos_pb2.Empty(), timeout=0.5)
                if info.role == 'LEADER':
                    ballot = (info.ballot_number, info.ballot_node_id)
                    if ballot > best_ballot:
                        best_leader = node_id
                        best_ballot = ballot
                        self.logger.debug(f"Found leader candidate: Node {node_id} with ballot {ballot}")
            except:
                continue
        
        if best_leader:
            self.logger.debug(f"Selected leader for cluster {cluster_id}: Node {best_leader}")
        return best_leader
    
    # ==================== SYSTEM RESET ====================
    
    def reset_system_state(self):
        from shared.logger import clear_log
        
        with self.lock:
            self.role = 'BACKUP'
            self.leader_id = None
            self.sequence_num = 0
            self.last_executed_seq = 0
            self.ballot_num = paxos_pb2.BallotNumber(number=0, node_id=self.node_id)
            self.highest_promised_ballot = paxos_pb2.BallotNumber(number=0, node_id=0)
            
            self.log = {}
            self.transaction_status = {}
            self.transaction_status_message = {}
            
            start_id, end_id = config.SHARD_RANGES[self.cluster_id]
            self.datastore = {i: config.INITIAL_AMOUNT for i in range(start_id, end_id + 1)}
            self.last_reply_timestamps = {i: -1 for i in range(start_id, end_id + 1)}
            
            try:
                import json
                import os
                config.reset_client_mappings()
                if os.path.exists(config.RESHARD_FILE):
                    with open(config.RESHARD_FILE, 'r') as f:
                        reshard_data = json.load(f)
                    
                    for client_id_str, dest_cluster in reshard_data.get("moves", {}).items():
                        client_id = int(client_id_str)
                        config.set_client_cluster(client_id, dest_cluster)
                        
                        if dest_cluster == self.cluster_id:
                            self.datastore[client_id] = config.INITIAL_AMOUNT
                            self.last_reply_timestamps[client_id] = -1
                            self.logger.info(f"Added resharded client {client_id} to cluster {self.cluster_id}")
                        else:
                            orig_cluster = config.get_original_cluster_for_client(client_id)
                            if orig_cluster == self.cluster_id and client_id in self.datastore:
                                del self.datastore[client_id]
                                if client_id in self.last_reply_timestamps:
                                    del self.last_reply_timestamps[client_id]
                                self.logger.info(f"Removed resharded client {client_id} from cluster {self.cluster_id}")
            except Exception as e:
                self.logger.debug(f"No resharding data or error: {e}")
            
            self.new_view_history = []
            self.last_checkpoint_seq = 0
            self.latest_checkpoint_state = None
            
            self.promise_counts = {}
            self.promise_logs = {}
            
            self.queued_prepare_messages = []
            self.last_prepare_received_time = 0
            self.last_heartbeat_time = 0
            
            if self.leader_timer:
                self.leader_timer.cancel()
                self.leader_timer = None
            
            try:
                conn = self.persistence._get_connection()
                cursor = conn.cursor()
                cursor.execute('DELETE FROM locks')
                cursor.execute('DELETE FROM wal')
                cursor.execute('DELETE FROM twopc_state')
                conn.commit()
                self.persistence._return_connection(conn)
                self.logger.info("Cleared all locks, WAL, and 2PC state")
            except Exception as e:
                self.logger.error(f"Error clearing locks: {e}")
            
            self.persistence.reset_state(self.datastore)
        
        clear_log(self.logger)
        
        # self.logger.info("System state reset complete")

    