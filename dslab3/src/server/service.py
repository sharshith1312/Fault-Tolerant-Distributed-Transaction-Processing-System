"""
Author: sharshith1312
Course: CSE535 - Distributed Systems
"""

import grpc
import time
import json
import hashlib
import paxos_pb2
import paxos_pb2_grpc
from shared import config
from server.node import ballot_greater_than
from server import transaction_utils

class PaxosService(paxos_pb2_grpc.PaxosServiceServicer):
    def __init__(self, node_instance):
        self.node = node_instance

    def SendPrepare(self, request, context):
        if self.node.is_idle:
            return paxos_pb2.PromiseMsg(acknowledged=False)
        
        self.node.logger.info(f"Received PREPARE from Node {request.ballot_num.node_id} with ballot ({request.ballot_num.number},{request.ballot_num.node_id})")
        
        if ballot_greater_than(request.ballot_num, self.node.ballot_num):
            self.node.last_prepare_received_time = time.time()
        
        if self.node.role == 'LEADER':
            if ballot_greater_than(request.ballot_num, self.node.ballot_num):
                self.node.logger.info(f"Stepping down from LEADER - higher ballot received")
                self.node.role = 'BACKUP'
                self.node.leader_id = None
                self.node.reset_leader_failure_timer()
            else:
                self.node.logger.info(f"Rejecting PREPARE - I'm LEADER with equal/higher ballot")
                return paxos_pb2.PromiseMsg(acknowledged=False)
        
        if self.node.role == 'BACKUP' and self.node.leader_timer is None:
            self.node.logger.info(f"First PREPARE received, starting timer ({config.get_randomized_timeout(self.node.node_id)}s)")
            self.node.start_leader_failure_timer()
        
        with self.node.lock:
            self.node.queued_prepare_messages.append((request, time.time()))
        
        timer_expired, remaining_time = self.node.is_leader_timer_expired()
        
        if not timer_expired and remaining_time > 0:
            self.node.logger.info(f"Timer not expired yet ({remaining_time:.2f}s remaining), waiting...")
            time.sleep(remaining_time)
        
        self.node.logger.info(f"Timer expired, processing {len(self.node.queued_prepare_messages)} queued PREPAREs")
        
        with self.node.lock:
            if not self.node.queued_prepare_messages:
                return paxos_pb2.PromiseMsg(acknowledged=False)
            
            queued_messages = list(self.node.queued_prepare_messages)
            self.node.queued_prepare_messages.clear()
        
        highest_prepare = max(
            queued_messages,
            key=lambda item: (item[0].ballot_num.number, item[0].ballot_num.node_id)
        )[0]
        
        with self.node.lock:
            if ballot_greater_than(highest_prepare.ballot_num, self.node.highest_promised_ballot):
                if self.node.role == 'PROPOSER':
                    if ballot_greater_than(highest_prepare.ballot_num, self.node.ballot_num):
                        self.node.logger.info(f"PROPOSER stepping down to higher ballot ({highest_prepare.ballot_num.number},{highest_prepare.ballot_num.node_id}) > our ballot ({self.node.ballot_num.number},{self.node.ballot_num.node_id})")
                        self.node.highest_promised_ballot = highest_prepare.ballot_num
                        self.node.role = 'BACKUP'
                        self.node.leader_id = None
                    else:
                        self.node.logger.info(f"Rejecting - we are PROPOSER with ballot ({self.node.ballot_num.number},{self.node.ballot_num.node_id}) >= incoming ({highest_prepare.ballot_num.number},{highest_prepare.ballot_num.node_id})")
                        return paxos_pb2.PromiseMsg(acknowledged=False)
                else:
                    self.node.highest_promised_ballot = highest_prepare.ballot_num
                    self.node.role = 'BACKUP'
                    self.node.leader_id = None
                
                self.node.persistence.update_highest_promised_ballot(self.node.highest_promised_ballot)
                self.node.persistence.update_leader_id(self.node.leader_id)
                
                if (request.ballot_num.number == highest_prepare.ballot_num.number and 
                    request.ballot_num.node_id == highest_prepare.ballot_num.node_id):
                    
                    self.node.logger.info(f"Promising to Node {request.ballot_num.node_id} with ballot ({highest_prepare.ballot_num.number},{highest_prepare.ballot_num.node_id})")
                    
                    return paxos_pb2.PromiseMsg(
                        acknowledged=True,
                        ballot_num=highest_prepare.ballot_num,
                        accept_log=list(self.node.log.values()),
                        latest_checkpoint=self.node.latest_checkpoint_state
                    )
                else:
                    self.node.logger.info(f"Rejecting - promised to higher ballot for different node")
                    return paxos_pb2.PromiseMsg(acknowledged=False)
            else:
                self.node.logger.info(f"Rejecting - already promised to higher ballot ({self.node.highest_promised_ballot.number},{self.node.highest_promised_ballot.node_id})")
                return paxos_pb2.PromiseMsg(acknowledged=False)

        

    
    def SendNewView(self, request, context):
        if self.node.is_idle:
            return paxos_pb2.Empty()
        
        if not ballot_greater_than(self.node.highest_promised_ballot, request.ballot_num):
            self.node.logger.info(f"Accepting NEW-VIEW Request from leader with ballot ({request.ballot_num.number},{request.ballot_num.node_id})")
            self.node.logger.info(f"Our highest_promised_ballot: ({self.node.highest_promised_ballot.number},{self.node.highest_promised_ballot.node_id})")
            self.node.role = 'BACKUP'
            self.node.leader_id = request.ballot_num.node_id
            self.node.ballot_num = request.ballot_num
        
            new_view_log = {entry.sequence_num: entry for entry in request.accept_log}
            new_sequence_num = max(new_view_log.keys()) if new_view_log else 0
            
            if request.latest_checkpoint:
                checkpoint_seq = request.latest_checkpoint.sequence_num
                
                if checkpoint_seq > self.node.last_executed_seq:
                    self.node.logger.info(f"Behind checkpoint (executed: {self.node.last_executed_seq}, checkpoint: {checkpoint_seq})")
                    self.node.logger.info("Requesting checkpoint to catch up...")
                    
                    try:
                        leader_stub = self.node.peers.get(request.ballot_num.node_id)
                        if leader_stub:
                            checkpoint_request = paxos_pb2.CheckpointRequest(sequence_num=checkpoint_seq)
                            checkpoint_response = leader_stub.RequestCheckpoint(checkpoint_request, timeout=5.0)
                            
                            if checkpoint_response.has_checkpoint:
                                self.node.logger.info(f"Received checkpoint from new leader")
                                self.node.apply_received_checkpoint(checkpoint_response.checkpoint)
                            else:
                                self.node.logger.info(f"Leader doesn't have checkpoint - using NEW-VIEW checkpoint info")
                                self.node.last_checkpoint_seq = checkpoint_seq
                                self.node.latest_checkpoint_state = request.latest_checkpoint
                                self.node.persistence.update_checkpoint_state(checkpoint_seq, request.latest_checkpoint)
                    except grpc.RpcError as e:
                        self.node.logger.info(f"Failed to get checkpoint from leader: {e}")
                        self.node.last_checkpoint_seq = checkpoint_seq
                        self.node.latest_checkpoint_state = request.latest_checkpoint
                        self.node.persistence.update_checkpoint_state(checkpoint_seq, request.latest_checkpoint)
            
            self.node.logger.debug(f"Processing NEW-VIEW with {len(new_view_log)} entries")
            
            
            self.node.persistence.update_leader_id(self.node.leader_id)
            self.node.add_new_view_to_history(request)
            
            if new_view_log:
                max_seq_in_view = max(new_view_log.keys())
                start_seq = self.node.last_executed_seq + 1
                
                self.node.logger.debug(f"Executing sequences {start_seq} to {max_seq_in_view}")
                
                for seq in range(start_seq, max_seq_in_view + 1):
                    if seq <= self.node.last_checkpoint_seq:
                        continue
                    
                    if seq not in new_view_log:
                        self.node.logger.info(f"ERROR: Seq {seq} missing in NEW-VIEW! This should not happen!")
                        break
                    
                    entry = new_view_log[seq]
                    
                    if entry.is_noop:
                        self.node.logger.debug(f"Seq {seq}: NO-OP")
                        self.node.update_transaction_status(seq, "X", "NO-OP")
                        self.node.last_executed_seq = seq
                        self.node.persistence.update_last_executed_seq(seq)
                        
                        entry_to_store = paxos_pb2.LogEntry(
                            ballot_num=entry.ballot_num,
                            sequence_num=entry.sequence_num,
                            is_noop=True,
                            status="X"
                        )
                    else:
                        self.node.logger.debug(f"Seq {seq}: Executing {entry.request.transaction}")
                        tx_parts = entry.request.transaction.strip("()").split(',')
                        sender = int(tx_parts[0].strip())
                        receiver = int(tx_parts[1].strip())
                        amount = float(tx_parts[2].strip())

                        if self.node.datastore[sender] >= amount:
                            self.node.datastore[sender] -= amount
                            self.node.datastore[receiver] += amount
                            self.node.logger.debug(f"Executed: {entry.request.transaction} at seq {seq}")
                            self.node.update_transaction_status(seq, "E", "Success")
                            status_for_entry = "E"
                            self.node.persistence.update_datastore(self.node.datastore)
                        else:
                            self.node.logger.debug(f"Failed (insufficient balance): {entry.request.transaction} at seq {seq}")
                            self.node.update_transaction_status(seq, "E", "Insufficient Balance")
                            status_for_entry = "E"
                        
                        self.node.last_executed_seq = seq
                        self.node.persistence.update_last_executed_seq(seq)
                        
                        self.node.last_reply_timestamps[entry.request.client_id] = entry.request.timestamp
                        self.node.persistence.update_last_reply_timestamp(entry.request.client_id, entry.request.timestamp)
                        
                        entry_to_store = paxos_pb2.LogEntry(
                            ballot_num=entry.ballot_num,
                            sequence_num=entry.sequence_num,
                            request=entry.request,
                            is_noop=False,
                            status=status_for_entry
                        )
                    
                    self.node.log[seq] = entry_to_store
                    self.node.persistence.add_log_entry(seq, entry_to_store)
            
            self.node.sequence_num = new_sequence_num
            self.node.persistence.update_sequence_num(self.node.sequence_num)
        else:
            self.node.logger.debug(f"Rejecting NEW-VIEW Request with ballot ({request.ballot_num.number},{request.ballot_num.node_id})")
            self.node.logger.debug(f"Our highest_promised_ballot: ({self.node.highest_promised_ballot.number},{self.node.highest_promised_ballot.node_id})")
        return paxos_pb2.Empty()

    def SendAccept(self, request, context):
        if self.node.is_idle:
            return paxos_pb2.AcceptedMsg()
        
        self.node.logger.debug(f"Received ACCEPT Request with ballot ({request.ballot_num.number},{request.ballot_num.node_id}) for seq={request.sequence_num}")
        if not ballot_greater_than(self.node.highest_promised_ballot, request.ballot_num):
            log_entry = paxos_pb2.LogEntry(
                ballot_num=request.ballot_num,
                sequence_num=request.sequence_num,
                request=request.request,
                status="A",
                phase=request.phase if hasattr(request, 'phase') else '',
                transaction_type='INTRA',
                transaction_id=request.transaction_id if hasattr(request, 'transaction_id') else ''
            )
            self.node.log[request.sequence_num] = log_entry
            self.node.sequence_num = max(self.node.sequence_num, request.sequence_num)
            self.node.persistence.add_log_entry(request.sequence_num, log_entry)
            self.node.persistence.update_sequence_num(self.node.sequence_num)
            self.node.update_transaction_status(request.sequence_num, "A")
            return paxos_pb2.AcceptedMsg(ballot_num=request.ballot_num, sequence_num=request.sequence_num, node_id=self.node.node_id)
        return paxos_pb2.AcceptedMsg()

    def SendCommit(self, request, context):
        if self.node.is_idle:
            return paxos_pb2.Empty()

        if request.sequence_num not in self.node.log:
            self.node.logger.info(f"Ignoring COMMIT for seq {request.sequence_num} - not in log")
            return paxos_pb2.Empty()
        
        self.node.update_transaction_status(request.sequence_num, "C")
        
        self.node.signal_pending_execution()
        self.node.try_execute_pending_transactions()
        
        if request.sequence_num > self.node.last_executed_seq:
            gap_size = request.sequence_num - self.node.last_executed_seq
            if gap_size > 1:
                self.node.logger.debug(f"Execution gap: last_executed={self.node.last_executed_seq}, committed={request.sequence_num}")
                
                if self.node.last_checkpoint_seq > self.node.last_executed_seq:
                    self.node.logger.info(f"Attempting to recover using checkpoint at seq {self.node.last_checkpoint_seq}")
                    if self.node._recover_from_checkpoint():
                        self.node.try_execute_pending_transactions()
        
        return paxos_pb2.Empty()

    def HandleClientRequest(self, request, context):
        if self.node.is_idle:
            return paxos_pb2.ClientReply(success=False, message="Node is IDLE")

        self.node.logger.info(f"Received client request: {request.transaction}")
        
        txn_type = transaction_utils.get_transaction_type(request.transaction)
        
        if txn_type == 'BALANCE':
            parsed = transaction_utils.parse_transaction(request.transaction)
            if not parsed:
                return paxos_pb2.ClientReply(
                    success=False, 
                    message="Invalid balance query format"
                )
            
            client_id = parsed['client_id']
            success, balance, message = self.node.handle_balance_request(client_id)
            
            return paxos_pb2.ClientReply(
                success=success,
                message=f"Balance: {balance}" if success else message,
                leader_id=self.node.node_id
            )
        
        if self.node.new_leader_syncing:
            return paxos_pb2.ClientReply(success=False, message="Leader is syncing (try again shortly)")
        
        if self.node.role != 'LEADER':
            if self.node.leader_id:
                self.node.logger.info(f"Forwarding to leader S{self.node.leader_id}")
                try:
                    leader_stub = self.node.peers[self.node.leader_id]
                    reply = leader_stub.HandleClientRequest(request, timeout=3.0)
                    return reply
                except (grpc.RpcError, KeyError):
                    self.node.logger.info(f"Leader S{self.node.leader_id} not available")
                    self.node.leader_id = None
            
            return paxos_pb2.ClientReply(success=False, message="No leader available")
        
        parsed = transaction_utils.parse_transaction(request.transaction)
        if not parsed or parsed['type'] != 'TRANSFER':
            return paxos_pb2.ClientReply(
                success=False,
                message="Invalid transaction format",
                leader_id=self.node.node_id
            )
        
        sender = parsed['sender']
        receiver = parsed['receiver']
        amount = parsed['amount']
        
        if request.timestamp <= self.node.last_reply_timestamps.get(request.client_id, -1):
            self.node.logger.info(f"Duplicate request from {request.client_id}. Checking for stored reply.")
            stored_reply = self.node.get_stored_client_reply(request.client_id, request.timestamp)
            if stored_reply:
                self.node.logger.info(f"Resending stored reply for {request.client_id}, timestamp {request.timestamp}")
                return stored_reply
            else:
                return paxos_pb2.ClientReply(success=False, message="Duplicate request - no stored reply", leader_id=self.node.node_id)
        
        if txn_type == 'CROSS':
            self.node.logger.info(f"Cross-shard transaction detected: sender={sender}, receiver={receiver}")
            
            success, message = self.node.handle_cross_shard_transaction(
                sender, receiver, amount, request
            )
            
            reply = paxos_pb2.ClientReply(
                success=success,
                message=message,
                leader_id=self.node.node_id,
                request_timestamp=request.timestamp
            )
            
            self.node.store_client_reply(request.client_id, reply)
            self.node.last_reply_timestamps[request.client_id] = request.timestamp
            self.node.persistence.update_last_reply_timestamp(request.client_id, request.timestamp)
            return reply
        
        self.node.logger.info(f"Intra-shard transaction: sender={sender}, receiver={receiver}")
        
        if self.node.lock_manager.is_locked(sender):
            lock_owner = self.node.lock_manager.get_lock_owner(sender)
            return paxos_pb2.ClientReply(
                success=False,
                message=f"Sender {sender} is locked by transaction {lock_owner}",
                leader_id=self.node.node_id
            )
        
        if self.node.lock_manager.is_locked(receiver):
            lock_owner = self.node.lock_manager.get_lock_owner(receiver)
            return paxos_pb2.ClientReply(
                success=False,
                message=f"Receiver {receiver} is locked by transaction {lock_owner}",
                leader_id=self.node.node_id
            )
        
        seq_num = self.node.run_paxos_accept_phase(
            transaction_str=request.transaction,
            original_request=request,
            phase='',
            transaction_id='',
            transaction_type='INTRA'
        )
        
        if seq_num is None:
            reply = paxos_pb2.ClientReply(
                success=False,
                message="Failed to achieve quorum",
                leader_id=self.node.node_id,
                request_timestamp=request.timestamp
            )
            self.node.store_client_reply(request.client_id, reply)
            self.node.last_reply_timestamps[request.client_id] = request.timestamp
            self.node.persistence.update_last_reply_timestamp(request.client_id, request.timestamp)
            return reply
        
        self.node.run_paxos_commit_phase(seq_num, phase='', transaction_id='')
        
        status = self.node.get_transaction_status(seq_num)
        if status == "E":
            success = True
            msg = self.node.get_transaction_status_message(seq_num) or "Transaction Committed"
        else:
            self.node.signal_pending_execution()
            success = True
            msg = "Transaction Committed"
        
        reply = paxos_pb2.ClientReply(
            success=success,
            message=msg,
            leader_id=self.node.node_id,
            request_timestamp=request.timestamp
        )
        self.node.store_client_reply(request.client_id, reply)
        self.node.last_reply_timestamps[request.client_id] = request.timestamp
        self.node.persistence.update_last_reply_timestamp(request.client_id, request.timestamp)
        return reply


    def SendHeartbeat(self, request, context):
        if self.node.is_idle:
            self.node.logger.debug(f"Node is IDLE, rejecting HEARTBEAT.")
            return paxos_pb2.Empty()
        
        if not ballot_greater_than(self.node.highest_promised_ballot, request.ballot_num):
            
            if self.node.role == 'LEADER' and ballot_greater_than(request.ballot_num, self.node.ballot_num):
                self.node.logger.warning(f"STEPPING DOWN - Received heartbeat from new leader {request.leader_id} with higher ballot ({request.ballot_num.number},{request.ballot_num.node_id}) > our ballot ({self.node.ballot_num.number},{self.node.ballot_num.node_id})")
                self.node.logger.warning(f"We were partitioned or lost leadership!")
            else:
                self.node.logger.debug(f"Received HEARTBEAT from Leader {request.leader_id} ballot ({request.ballot_num.number},{request.ballot_num.node_id})")
            
            self.node.leader_id = request.leader_id
            self.node.role = 'BACKUP'
            self.node.ballot_num = request.ballot_num
            
            self.node.last_heartbeat_time = time.time()
            
            self.node.reset_leader_failure_timer()
            
            self.node.persistence.update_leader_id(self.node.leader_id)
        else:
            self.node.logger.warning(f"Ignoring HEARTBEAT from old leader {request.leader_id}")
        
        return paxos_pb2.Empty()


    def SendCheckpoint(self, request, context):
        if self.node.is_idle:
            return paxos_pb2.Empty()
 
        try:
            self.node.datastore = json.loads(request.state_datastore)
            self.node.last_checkpoint_seq = request.sequence_num
            
            state_digest = hashlib.sha256(request.state_datastore.encode('utf-8')).digest()
            self.node.latest_checkpoint_state = paxos_pb2.CheckpointState(
                sequence_num=request.sequence_num,
                state_digest=state_digest
            )
            
            to_remove = [seq for seq in self.node.log.keys() if seq <= request.sequence_num]
            for seq in to_remove:
                del self.node.log[seq]
                if seq in self.node.transaction_status:
                    del self.node.transaction_status[seq]
            
            self.node.persistence.remove_log_entries(to_remove)
            self.node.persistence.remove_transaction_status(to_remove)
            self.node.persistence.update_checkpoint_state(request.sequence_num, self.node.latest_checkpoint_state)
            self.node.persistence.update_datastore(self.node.datastore)
            
            old_ballots = []
            for ballot_key in self.node.promise_counts.keys():
                if ballot_key[0] <= request.sequence_num:
                    old_ballots.append(ballot_key)
            
            for ballot_key in old_ballots:
                if ballot_key in self.node.promise_counts:
                    del self.node.promise_counts[ballot_key]
                if ballot_key in self.node.promise_logs:
                    del self.node.promise_logs[ballot_key]
            
            self.node.last_executed_seq = max(self.node.last_executed_seq, request.sequence_num)
            
            self.node.logger.info(f"Applied checkpoint at sequence {request.sequence_num}")
            self.node.logger.info(f"Cleaned up {len(to_remove)} log entries and {len(old_ballots)} old ballot trackings")
        except Exception as e:
            self.node.logger.info(f"Error applying checkpoint: {e}")
        return paxos_pb2.Empty()


    def GetLog(self, request, context):
        with self.node.lock:
            entries = []
            for seq_num in sorted(self.node.log.keys()):
                entry = self.node.log[seq_num]
                log_entry = paxos_pb2.LogEntry(
                    ballot_num=entry.ballot_num,
                    sequence_num=entry.sequence_num,
                    request=entry.request,
                    is_noop=entry.is_noop,
                    status=self.node.get_transaction_status(seq_num),
                    phase=entry.phase if hasattr(entry, 'phase') else '',
                    transaction_type=entry.transaction_type if hasattr(entry, 'transaction_type') else '',
                    transaction_id=entry.transaction_id if hasattr(entry, 'transaction_id') else ''
                )
                entries.append(log_entry)
            
            return paxos_pb2.LogResponse(entries=entries, node_id=self.node.node_id)

    def GetDatabase(self, request, context):
        with self.node.lock:
            return paxos_pb2.DatabaseResponse(balances=self.node.datastore, node_id=self.node.node_id)
    
    def GetAllBalances(self, request, context):
        with self.node.lock:
            entries = []
            for client_id, balance in self.node.datastore.items():
                entry = paxos_pb2.BalanceEntry(client_id=client_id, balance=balance)
                entries.append(entry)
            return paxos_pb2.AllBalancesResponse(entries=entries, node_id=self.node.node_id)

    def GetStatus(self, request, context):
        with self.node.lock:
            status = self.node.get_transaction_status(request.sequence_num)
            status_message = self.node.get_transaction_status_message(request.sequence_num)
            node_status = {self.node.node_id: status}
            node_status_message = {self.node.node_id: status_message}
            return paxos_pb2.StatusResponse(
                sequence_num=request.sequence_num, 
                node_status=node_status,
                node_status_message=node_status_message
            )

    def GetView(self, request, context):
        with self.node.lock:
            new_views = []
            for view_info in self.node.new_view_history:
                ballot_num = paxos_pb2.BallotNumber(
                    number=view_info['ballot_num'][0],
                    node_id=view_info['ballot_num'][1]
                )
                
                accept_log = []
                for entry_dict in view_info['accept_log']:
                    log_entry = self.node.persistence._deserialize_log_entry(entry_dict)
                    accept_log.append(log_entry)
                
                checkpoint = None
                if view_info.get('checkpoint_seq', 0) > 0:
                    checkpoint = paxos_pb2.CheckpointState(
                        sequence_num=view_info['checkpoint_seq']
                    )
                
                new_view_msg = paxos_pb2.NewViewMsg(
                    ballot_num=ballot_num,
                    accept_log=accept_log,
                    latest_checkpoint=checkpoint
                )
                new_views.append(new_view_msg)
            
            return paxos_pb2.ViewResponse(new_views=new_views)

    def RequestCheckpoint(self, request, context):
        if (self.node.latest_checkpoint_state and 
            self.node.latest_checkpoint_state.sequence_num >= request.sequence_num):
            
            checkpoint_msg = paxos_pb2.CheckpointMsg(
                sequence_num=self.node.latest_checkpoint_state.sequence_num,
                state_datastore=json.dumps(self.node.datastore, sort_keys=True)
            )
            
            return paxos_pb2.CheckpointResponse(
                has_checkpoint=True,
                checkpoint=checkpoint_msg
            )
        else:
            return paxos_pb2.CheckpointResponse(has_checkpoint=False)
    
    def SetIdle(self, request, context):
        try:
            old_state = self.node.is_idle
            self.node.is_idle = request.is_idle
            
            if request.is_idle:
                if self.node.leader_timer:
                    self.node.leader_timer.cancel()
                    self.node.leader_timer = None
                if self.node.role == 'LEADER':
                    self.node.role = 'BACKUP'
                    self.node.leader_id = None
                return paxos_pb2.IdleResponse(success=True, message=f"Node {self.node.node_id} is now IDLE")
            else:
                self.node.start_leader_failure_timer()
                return paxos_pb2.IdleResponse(success=True, message=f"Node {self.node.node_id} is now ACTIVE")
        except Exception as e:
            self.node.logger.info(f"Error setting idle state: {e}")
            return paxos_pb2.IdleResponse(success=False, message=f"Error: {str(e)}")
    
    def GetNodeInfo(self, request, context):

        return paxos_pb2.NodeInfoResponse(
            node_id=self.node.node_id,
            role=self.node.role,
            is_idle=self.node.is_idle,
            leader_id=self.node.leader_id if self.node.leader_id else -1,
            ballot_number=self.node.ballot_num.number,
            ballot_node_id=self.node.ballot_num.node_id
        )
    
    def ResetSystemState(self, request, context):
        try:
            self.node.reset_system_state()
            return paxos_pb2.Empty()
        except Exception as e:
            self.node.logger.error(f"Error resetting system state: {e}")
            return paxos_pb2.Empty()
    
    def TriggerElection(self, request, context):
        if self.node.is_idle:
            return paxos_pb2.ElectionResponse(success=False, leader_id=0, message="Node is IDLE")
        
        if self.node.role == 'LEADER':
            return paxos_pb2.ElectionResponse(success=True, leader_id=self.node.node_id, message="Already leader")
        
        if self.node.leader_id is not None:
            return paxos_pb2.ElectionResponse(success=True, leader_id=self.node.leader_id, message="Leader known")
        

        self.node.initiate_leader_election()
        
        import time
        for _ in range(15):
            time.sleep(0.1)
            if self.node.role == 'LEADER':
                return paxos_pb2.ElectionResponse(success=True, leader_id=self.node.node_id, message="Became leader")
            if self.node.leader_id is not None:
                return paxos_pb2.ElectionResponse(success=True, leader_id=self.node.leader_id, message="Leader elected")
        
        return paxos_pb2.ElectionResponse(success=False, leader_id=0, message="Election in progress")
    
    def HandleBalanceRequest(self, request, context):
        client_id = request.client_id
        
        self.node.logger.debug(f"Received balance request for client {client_id}")
        
        success, balance, message = self.node.handle_balance_request(client_id)
        
        return paxos_pb2.BalanceResponse(
            success=success,
            balance=balance,
            client_id=client_id,
            message=message
        )
    
    def SendPrepare2PC(self, request, context):
        self.node.logger.info(f"Received 2PC PREPARE: txid={request.transaction_id}")
        
        if self.node.role != 'LEADER':
            return paxos_pb2.Prepared2PCMsg(
                prepared=False,
                transaction_id=request.transaction_id,
                participant_cluster=self.node.cluster_id,
                message="No leader"
            )
        
        prepared, message = self.node.handle_2pc_prepare(request)
        
        return paxos_pb2.Prepared2PCMsg(
            prepared=prepared,
            transaction_id=request.transaction_id,
            participant_cluster=self.node.cluster_id,
            message=message
        )
    
    def SendCommit2PC(self, request, context):
        self.node.logger.info(f"Received 2PC COMMIT: txid={request.transaction_id}")
        
        if self.node.role != 'LEADER':
            return paxos_pb2.Ack2PCMsg(
                success=False,
                transaction_id=request.transaction_id,
                message="No leader"
            )
        
        success, message = self.node.handle_2pc_commit(request)
        
        return paxos_pb2.Ack2PCMsg(
            success=success,
            transaction_id=request.transaction_id,
            message=message
        )
    
    def SendAbort2PC(self, request, context):
        self.node.logger.warning(f"Received 2PC ABORT: txid={request.transaction_id}, reason={request.reason}")
        
        success, message = self.node.handle_2pc_abort(request)
        
        return paxos_pb2.Ack2PCMsg(
            success=success,
            transaction_id=request.transaction_id,
            message=message
        )
