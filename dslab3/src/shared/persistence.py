"""
Author: sharshith1312
Course: CSE535 - Distributed Systems
SQLite3-based Persistence Layer for Multi-Cluster Paxos
"""

import sqlite3
import json
import os
import threading
import time
from typing import Dict, Any, List, Optional
import paxos_pb2
from shared import config

class ServerState:
    """
    SQLite3-based persistence for Paxos node state.
    Uses WAL mode and connection pooling for better concurrent performance.
    """
    
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.db_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), config.DB_DIR)
        self.db_path = os.path.join(self.db_dir, f'node_{node_id}.db')
        self.lock = threading.Lock()
        

        self._pool_size = 500
        self._connection_pool = []
        self._pool_lock = threading.Lock()
        self._pool_semaphore = threading.Semaphore(self._pool_size)
        
        os.makedirs(self.db_dir, exist_ok=True)
        
        self._init_database()
        
        self._enable_wal_mode()
        
        initial_pool_size = min(10, self._pool_size)
        for _ in range(initial_pool_size):
            try:
                conn = self._create_connection()
                self._connection_pool.append(conn)
            except Exception as e:
                print(f"Warning: Could not pre-create connection: {e}")
                break
        
    def _init_database(self):
        """Create tables if they don't exist"""
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Metadata table (singleton table for node state)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                )
            ''')
            
            # Log entries table (extended for 2PC)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS log_entries (
                    sequence_num INTEGER PRIMARY KEY,
                    ballot_number INTEGER NOT NULL,
                    ballot_node_id INTEGER NOT NULL,
                    client_id TEXT,
                    timestamp INTEGER,
                    transaction_data TEXT,
                    is_noop INTEGER DEFAULT 0,
                    status TEXT DEFAULT 'X',
                    phase TEXT,
                    transaction_type TEXT,
                    transaction_id TEXT
                )
            ''')
            
            # Datastore table (client balances)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS datastore (
                    client_id INTEGER PRIMARY KEY,
                    balance REAL NOT NULL DEFAULT 10.0
                )
            ''')
            
            # Client reply timestamps
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS client_replies (
                    client_id INTEGER PRIMARY KEY,
                    last_timestamp INTEGER DEFAULT -1
                )
            ''')
            
            # Transaction status
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transaction_status (
                    sequence_num INTEGER PRIMARY KEY,
                    status TEXT,
                    message TEXT DEFAULT ''
                )
            ''')
            
            # New view history
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS new_view_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ballot_number INTEGER,
                    ballot_node_id INTEGER,
                    timestamp REAL
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS locks (
                    client_id INTEGER PRIMARY KEY,
                    transaction_id TEXT NOT NULL,
                    acquired_at REAL NOT NULL,
                    lock_type TEXT DEFAULT 'X'
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS wal (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    transaction_id TEXT NOT NULL,
                    sequence_num INTEGER NOT NULL,
                    client_id INTEGER NOT NULL,
                    old_balance REAL NOT NULL,
                    new_balance REAL NOT NULL,
                    operation TEXT NOT NULL,
                    phase TEXT NOT NULL,
                    timestamp REAL NOT NULL
                )
            ''')
            
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_wal_txid 
                ON wal(transaction_id)
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS twopc_state (
                    transaction_id TEXT PRIMARY KEY,
                    state TEXT NOT NULL,
                    sender INTEGER NOT NULL,
                    receiver INTEGER NOT NULL,
                    amount REAL NOT NULL,
                    coordinator_cluster INTEGER NOT NULL,
                    participant_cluster INTEGER NOT NULL,
                    sequence_num INTEGER,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
            ''')
            
            cursor.execute("SELECT value FROM metadata WHERE key = 'initialized'")
            if not cursor.fetchone():
                self._initialize_defaults(cursor)
            
            conn.commit()
            conn.close()
    
    def _initialize_defaults(self, cursor):
        # Metadata defaults
        defaults = {
            'initialized': 'true',
            'node_id': str(self.node_id),
            'leader_id': 'null',
            'highest_promised_ballot_number': '0',
            'highest_promised_ballot_node_id': '0',
            'sequence_num': '0',
            'last_checkpoint_seq': '0',
            'last_executed_seq': '0',
            'latest_checkpoint_seq': '0',
            'latest_checkpoint_digest': ''
        }
        
        for key, value in defaults.items():
            cursor.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", (key, value))
        
        cluster_id = config.NODE_TO_CLUSTER[self.node_id]
        start_id, end_id = config.SHARD_RANGES[cluster_id]
        
        for client_id in range(start_id, end_id + 1):
            cursor.execute("INSERT OR IGNORE INTO datastore (client_id, balance) VALUES (?, ?)", 
                          (client_id, config.INITIAL_AMOUNT))
            cursor.execute("INSERT OR IGNORE INTO client_replies (client_id, last_timestamp) VALUES (?, ?)",
                          (client_id, -1))
    
    def _enable_wal_mode(self):
        conn = sqlite3.connect(self.db_path)
        if config.USE_SQLITE_WAL_MODE:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        else:
            conn.execute("PRAGMA journal_mode=DELETE")
            conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA cache_size=10000")
        conn.execute("PRAGMA temp_store=MEMORY")
        conn.commit()
        conn.close()
    
    def _create_connection(self):
        os.makedirs(self.db_dir, exist_ok=True)
        
        try:
            conn = sqlite3.connect(self.db_path, check_same_thread=False, timeout=30.0)
            if config.USE_SQLITE_WAL_MODE:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("PRAGMA synchronous=NORMAL")
            else:
                conn.execute("PRAGMA journal_mode=DELETE")
                conn.execute("PRAGMA synchronous=FULL")
            conn.execute("PRAGMA cache_size=10000")
            conn.execute("PRAGMA busy_timeout=30000")  # 30 second busy timeout
            return conn
        except sqlite3.OperationalError as e:
            raise RuntimeError(f"Failed to open database at {self.db_path}: {e}. Check directory permissions and disk space.")
    
    def _get_connection(self):

        if not self._pool_semaphore.acquire(timeout=5.0):
            raise RuntimeError("Timeout waiting for database connection from pool")
        with self._pool_lock:
            if self._connection_pool:
                return self._connection_pool.pop()
            else:
                return self._create_connection()
    
    def _return_connection(self, conn):
        with self._pool_lock:
            if len(self._connection_pool) < self._pool_size:
                self._connection_pool.append(conn)
            else:
                conn.close()
        self._pool_semaphore.release()
    
    def load_state(self) -> Dict[str, Any]:
        with self.lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("SELECT key, value FROM metadata")
            metadata = {row[0]: row[1] for row in cursor.fetchall()}
            
            cursor.execute("SELECT client_id, balance FROM datastore")
            datastore = {row[0]: row[1] for row in cursor.fetchall()}
            
            cursor.execute("SELECT client_id, last_timestamp FROM client_replies")
            last_reply_timestamps = {row[0]: row[1] for row in cursor.fetchall()}
            
            cursor.execute("SELECT sequence_num, status, message FROM transaction_status")
            transaction_status = {}
            transaction_status_message = {}
            for row in cursor.fetchall():
                transaction_status[row[0]] = row[1]
                transaction_status_message[row[0]] = row[2]
            
            self._return_connection(conn)
            
            def safe_int(val, default=0):
                if val is None or val == 'null' or val == '':
                    return default
                try:
                    return int(val)
                except (ValueError, TypeError):
                    return default
            
            state = {
                'node_id': safe_int(metadata.get('node_id'), self.node_id),
                'leader_id': None if metadata.get('leader_id') in (None, 'null', '') else safe_int(metadata.get('leader_id')),
                'highest_promised_ballot': {
                    'number': safe_int(metadata.get('highest_promised_ballot_number'), 0),
                    'node_id': safe_int(metadata.get('highest_promised_ballot_node_id'), 0)
                },
                'sequence_num': safe_int(metadata.get('sequence_num'), 0),
                'datastore': datastore,
                'last_reply_timestamps': last_reply_timestamps,
                'transaction_status': transaction_status,
                'transaction_status_message': transaction_status_message,
                'last_checkpoint_seq': safe_int(metadata.get('last_checkpoint_seq'), 0),
                'last_executed_seq': safe_int(metadata.get('last_executed_seq'), 0),
                'latest_checkpoint_state': self._load_checkpoint_state(metadata),
                'new_view_history': []
            }
            
            return state
    
    def _load_checkpoint_state(self, metadata):
        seq_val = metadata.get('latest_checkpoint_seq', 0)
        try:
            seq = int(seq_val) if seq_val and seq_val != 'null' else 0
        except (ValueError, TypeError):
            seq = 0
        if seq == 0:
            return None
        
        digest_hex = metadata.get('latest_checkpoint_digest', '')
        digest = bytes.fromhex(digest_hex) if digest_hex else b''
        
        return paxos_pb2.CheckpointState(
            sequence_num=seq,
            state_digest=digest
        )
    
    def save_state(self, force=False):
        pass
    
    def _set_metadata(self, key: str, value: str):
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", (key, value))
            conn.commit()
        finally:
            self._return_connection(conn)
    
    def update_leader_id(self, leader_id: Optional[int] = None):
        value = 'null' if leader_id is None else str(leader_id)
        self._set_metadata('leader_id', value)
    
    def update_highest_promised_ballot(self, ballot_num):
        self._set_metadata('highest_promised_ballot_number', str(ballot_num.number))
        self._set_metadata('highest_promised_ballot_node_id', str(ballot_num.node_id))
    
    def update_sequence_num(self, seq_num: int):
        self._set_metadata('sequence_num', str(seq_num))
    
    def add_log_entry(self, seq_num: int, log_entry):
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT OR REPLACE INTO log_entries 
                (sequence_num, ballot_number, ballot_node_id, client_id, timestamp, 
                 transaction_data, is_noop, status, phase, transaction_type, transaction_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                seq_num,
                log_entry.ballot_num.number,
                log_entry.ballot_num.node_id,
                log_entry.request.client_id if log_entry.request else None,
                log_entry.request.timestamp if log_entry.request else None,
                log_entry.request.transaction if log_entry.request else None,
                1 if log_entry.is_noop else 0,
                log_entry.status if hasattr(log_entry, 'status') else 'X',
                log_entry.phase if hasattr(log_entry, 'phase') else '',
                log_entry.transaction_type if hasattr(log_entry, 'transaction_type') else 'INTRA',
                log_entry.transaction_id if hasattr(log_entry, 'transaction_id') else ''
            ))
            
            conn.commit()
        finally:
            self._return_connection(conn)

    def remove_log_entries(self, seq_nums: list):
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            for seq_num in seq_nums:
                cursor.execute("DELETE FROM log_entries WHERE sequence_num = ?", (seq_num,))
            conn.commit()
        finally:
            self._return_connection(conn)
    
    def update_datastore(self, datastore: Dict[int, float]):
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            for client_id, balance in datastore.items():
                cursor.execute("INSERT OR REPLACE INTO datastore (client_id, balance) VALUES (?, ?)",
                              (client_id, balance))
            conn.commit()
        finally:
            self._return_connection(conn)
    
    def save_datastore_entry(self, client_id: int, balance: float):
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO datastore (client_id, balance) VALUES (?, ?)",
                          (client_id, balance))
            conn.commit()
        finally:
            self._return_connection(conn)
    
    def update_client_balance(self, client_id: int, balance: float):
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO datastore (client_id, balance) VALUES (?, ?)",
                          (client_id, balance))
            conn.commit()
        finally:
            self._return_connection(conn)
    
    def update_last_reply_timestamp(self, client_id: int, timestamp: int):
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO client_replies (client_id, last_timestamp) VALUES (?, ?)",
                          (client_id, timestamp))
            conn.commit()
        finally:
            self._return_connection(conn)
    
    def update_transaction_status(self, seq_num: int, status: str, message: str = ""):
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO transaction_status (sequence_num, status, message) VALUES (?, ?, ?)",
                          (seq_num, status, message))
            conn.commit()
        finally:
            self._return_connection(conn)
    
    def remove_transaction_status(self, seq_nums: list):
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            for seq_num in seq_nums:
                cursor.execute("DELETE FROM transaction_status WHERE sequence_num = ?", (seq_num,))
            conn.commit()
        finally:
            self._return_connection(conn)
    
    def add_new_view(self, view_info: Dict[str, Any]):
        with self.lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO new_view_history (ballot_number, ballot_node_id, timestamp)
                VALUES (?, ?, ?)
            ''', (
                view_info.get('ballot_number', 0),
                view_info.get('ballot_node_id', 0),
                time.time()
            ))
            conn.commit()
            self._return_connection(conn)
    
    def update_checkpoint_state(self, seq_num: int, checkpoint_state=None):
        self._set_metadata('last_checkpoint_seq', str(seq_num))
        if checkpoint_state:
            self._set_metadata('latest_checkpoint_seq', str(checkpoint_state.sequence_num))
            self._set_metadata('latest_checkpoint_digest', checkpoint_state.state_digest.hex() if checkpoint_state.state_digest else '')
    
    def update_last_executed_seq(self, seq_num: int):
        self._set_metadata('last_executed_seq', str(seq_num))
    
    def get_state(self) -> Dict[str, Any]:  
        return self.load_state()
    
    def deserialize_log(self) -> Dict[int, Any]:
        with self.lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT sequence_num, ballot_number, ballot_node_id, client_id, timestamp, transaction_data, is_noop, status
                FROM log_entries
                ORDER BY sequence_num
            ''')
            
            log = {}
            for row in cursor.fetchall():
                seq_num = row[0]
                ballot_num = paxos_pb2.BallotNumber(number=row[1], node_id=row[2])
                
                request = None
                if row[3]:  # client_id
                    request = paxos_pb2.ClientRequest(
                        client_id=row[3],
                        timestamp=row[4],
                        transaction=row[5]
                    )
                
                log_entry = paxos_pb2.LogEntry(
                    ballot_num=ballot_num,
                    sequence_num=seq_num,
                    request=request,
                    is_noop=bool(row[6]),
                    status=row[7]
                )
                
                log[seq_num] = log_entry
            
            self._return_connection(conn)
            return log
    
    def deserialize_ballot_num(self, ballot_dict: Dict[str, int]):
        return paxos_pb2.BallotNumber(
            number=ballot_dict['number'],
            node_id=ballot_dict['node_id']
        )
    
    def deserialize_checkpoint_state(self):
        state = self.load_state()
        return state.get('latest_checkpoint_state')
    
    def _serialize_log_entry(self, log_entry) -> Dict[str, Any]:
        return {
            'ballot_num': {
                'number': log_entry.ballot_num.number,
                'node_id': log_entry.ballot_num.node_id
            },
            'sequence_num': log_entry.sequence_num,
            'request': {
                'client_id': log_entry.request.client_id,
                'timestamp': log_entry.request.timestamp,
                'transaction': log_entry.request.transaction
            } if log_entry.request else None,
            'is_noop': log_entry.is_noop,
            'status': log_entry.status if hasattr(log_entry, 'status') else 'X'
        }
    
    def _deserialize_log_entry(self, entry_dict: Dict[str, Any]):
        ballot_num = paxos_pb2.BallotNumber(
            number=entry_dict['ballot_num']['number'],
            node_id=entry_dict['ballot_num']['node_id']
        )
        
        request = None
        if entry_dict['request']:
            request = paxos_pb2.ClientRequest(
                client_id=entry_dict['request']['client_id'],
                timestamp=entry_dict['request']['timestamp'],
                transaction=entry_dict['request']['transaction']
            )
        
        log_entry = paxos_pb2.LogEntry(
            ballot_num=ballot_num,
            sequence_num=entry_dict['sequence_num'],
            request=request,
            is_noop=entry_dict['is_noop'],
            status=entry_dict.get('status', 'X')
        )
        
        return log_entry
    

    
    def acquire_lock(self, client_id: int, transaction_id: str) -> bool:

        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            
            try:
                cursor.execute('''
                    SELECT transaction_id, acquired_at FROM locks 
                    WHERE client_id = ?
                ''', (client_id,))
                result = cursor.fetchone()
                
                if result:
                    return False
                
                cursor.execute('''
                    INSERT INTO locks (client_id, transaction_id, acquired_at, lock_type)
                    VALUES (?, ?, ?, 'X')
                ''', (client_id, transaction_id, time.time()))
                
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                conn.rollback()
                return False
        finally:
            self._return_connection(conn) 
    
    def release_lock(self, client_id: int, transaction_id: str) -> bool:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM locks 
                WHERE client_id = ? AND transaction_id = ?
            ''', (client_id, transaction_id))
            
            deleted = cursor.rowcount > 0
            conn.commit()
            return deleted
        finally:
            self._return_connection(conn)
    
    def is_locked(self, client_id: int) -> bool:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('SELECT 1 FROM locks WHERE client_id = ?', (client_id,))
            result = cursor.fetchone()
            return result is not None
        finally:
            self._return_connection(conn)
    
    def get_lock_info(self, client_id: int) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT transaction_id, acquired_at, lock_type 
                FROM locks WHERE client_id = ?
            ''', (client_id,))
            result = cursor.fetchone()
            
            if result:
                return {
                    'transaction_id': result[0],
                    'acquired_at': result[1],
                    'lock_type': result[2]
                }
            return None
        finally:
            self._return_connection(conn)
        
    def release_expired_locks(self, timeout: float = 5.0) -> int:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cutoff_time = time.time() - timeout
            cursor.execute('''
                DELETE FROM locks WHERE acquired_at < ?
            ''', (cutoff_time,))
            
            released_count = cursor.rowcount
            conn.commit()
            return released_count
        finally:
            self._return_connection(conn)
    
    def release_all_locks_for_transaction(self, transaction_id: str) -> int:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM locks WHERE transaction_id = ?
            ''', (transaction_id,))
            
            released_count = cursor.rowcount
            conn.commit()
            return released_count
        finally:
            self._return_connection(conn)
    
    def write_wal_entry(self, transaction_id: str, sequence_num: int, 
                        client_id: int, old_balance: float, new_balance: float,
                        operation: str, phase: str) -> bool:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO wal (transaction_id, sequence_num, client_id, 
                                old_balance, new_balance, operation, phase, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (transaction_id, sequence_num, client_id, old_balance, 
                  new_balance, operation, phase, time.time()))
            conn.commit()
            return True
        finally:
            self._return_connection(conn)
    
    def _get_wal_entries_internal(self, transaction_id: str):
        conn = self._get_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, sequence_num, client_id, old_balance, new_balance, 
                   operation, phase, timestamp
            FROM wal 
            WHERE transaction_id = ?
            ORDER BY id ASC
        ''', (transaction_id,))
        
        results = cursor.fetchall()

        
        entries = []
        for row in results:
            entries.append({
                'id': row[0],
                'sequence_num': row[1],
                'client_id': row[2],
                'old_balance': row[3],
                'new_balance': row[4],
                'operation': row[5],
                'phase': row[6],
                'timestamp': row[7]
            })
        
        return entries
    
    def get_wal_entries(self, transaction_id: str):
        with self.lock:
            return self._get_wal_entries_internal(transaction_id)
    
    def get_all_wal_entries(self):
        with self.lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('''
                SELECT DISTINCT transaction_id FROM wal
            ''')
            
            transaction_ids = [row[0] for row in cursor.fetchall()]
            
            result = {}
            for txid in transaction_ids:
                result[txid] = self.get_wal_entries(txid)
            
            return result
    
    def rollback_from_wal(self, transaction_id: str) -> List[Dict[str, Any]]:

        with self.lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            entries = self._get_wal_entries_internal(transaction_id)
            
            if not entries:
                return []
            
            for entry in entries:
                cursor.execute('''
                    UPDATE datastore 
                    SET balance = ? 
                    WHERE client_id = ?
                ''', (entry['old_balance'], entry['client_id']))
            
            conn.commit()
            
            return entries
    
    def clear_wal(self, transaction_id: str) -> bool:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM wal WHERE transaction_id = ?
            ''', (transaction_id,))
            conn.commit()
            return True
        finally:
            self._return_connection(conn)
    
    def has_wal_entries(self, transaction_id: str) -> bool:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT 1 FROM wal WHERE transaction_id = ? LIMIT 1
            ''', (transaction_id,))
            result = cursor.fetchone()
            return result is not None
        finally:
            self._return_connection(conn)
    
    def create_2pc_state(self, transaction_id: str, sender: int, receiver: int,
                         amount: float, coordinator_cluster: int, 
                         participant_cluster: int) -> bool:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    INSERT INTO twopc_state 
                    (transaction_id, state, sender, receiver, amount, 
                     coordinator_cluster, participant_cluster, created_at, updated_at)
                    VALUES (?, 'INIT', ?, ?, ?, ?, ?, ?, ?)
                ''', (transaction_id, sender, receiver, amount, 
                      coordinator_cluster, participant_cluster, 
                      time.time(), time.time()))
                conn.commit()
                return True
            except sqlite3.IntegrityError:
                conn.rollback()
                return False
        finally:
            self._return_connection(conn)
    
    def update_2pc_state(self, transaction_id: str, state: str, 
                         sequence_num: Optional[int] = None) -> bool:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            if sequence_num is not None:
                cursor.execute('''
                    UPDATE twopc_state 
                    SET state = ?, sequence_num = ?, updated_at = ?
                    WHERE transaction_id = ?
                ''', (state, sequence_num, time.time(), transaction_id))
            else:
                cursor.execute('''
                    UPDATE twopc_state 
                    SET state = ?, updated_at = ?
                    WHERE transaction_id = ?
                ''', (state, time.time(), transaction_id))
            conn.commit()
            return True
        finally:
            self._return_connection(conn)
    
    def get_2pc_state(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT state, sender, receiver, amount, coordinator_cluster,
                       participant_cluster, sequence_num, created_at, updated_at
                FROM twopc_state
                WHERE transaction_id = ?
            ''', (transaction_id,))
            
            result = cursor.fetchone()
            
            if result:
                return {
                    'state': result[0],
                    'sender': result[1],
                    'receiver': result[2],
                    'amount': result[3],
                    'coordinator_cluster': result[4],
                    'participant_cluster': result[5],
                    'sequence_num': result[6],
                    'created_at': result[7],
                    'updated_at': result[8]
                }
            return None
        finally:
            self._return_connection(conn)
    
    def delete_2pc_state(self, transaction_id: str) -> bool:
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM twopc_state WHERE transaction_id = ?
            ''', (transaction_id,))
            conn.commit()
            return True
        finally:
            self._return_connection(conn)
    
    def get_pending_2pc_transactions(self, max_age_seconds: float = 10.0):
        with self.lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cutoff_time = time.time() - max_age_seconds
            cursor.execute('''
                SELECT transaction_id, state, sender, receiver, amount,
                       coordinator_cluster, participant_cluster, sequence_num,
                       created_at, updated_at
                FROM twopc_state
                WHERE state NOT IN ('COMMITTED', 'ABORTED') AND created_at < ?
            ''', (cutoff_time,))
            
            results = cursor.fetchall()
            
            transactions = []
            for row in results:
                transactions.append({
                    'transaction_id': row[0],
                    'state': row[1],
                    'sender': row[2],
                    'receiver': row[3],
                    'amount': row[4],
                    'coordinator_cluster': row[5],
                    'participant_cluster': row[6],
                    'sequence_num': row[7],
                    'created_at': row[8],
                    'updated_at': row[9]
                })
            
            return transactions
    
    def reset_state(self, datastore: dict):
        with self.lock:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute('DELETE FROM log_entries')
            cursor.execute('DELETE FROM transaction_status')
            cursor.execute('DELETE FROM new_view_history')
            cursor.execute('DELETE FROM locks')
            cursor.execute('DELETE FROM wal')
            cursor.execute('DELETE FROM twopc_state')
            cursor.execute('DELETE FROM client_replies')
            
            cursor.execute("UPDATE metadata SET value = '0' WHERE key = 'sequence_num'")
            cursor.execute("UPDATE metadata SET value = '0' WHERE key = 'last_executed_seq'")
            cursor.execute("UPDATE metadata SET value = '0' WHERE key = 'last_checkpoint_seq'")
            cursor.execute("UPDATE metadata SET value = 'null' WHERE key = 'leader_id'")
            cursor.execute("UPDATE metadata SET value = '0,0' WHERE key = 'highest_promised_ballot'")
            cursor.execute("UPDATE metadata SET value = 'null' WHERE key = 'latest_checkpoint_state'")
            
            cursor.execute('DELETE FROM datastore')
            for client_id, balance in datastore.items():
                cursor.execute('''
                    INSERT INTO datastore (client_id, balance)
                    VALUES (?, ?)
                ''', (client_id, balance))
            
            conn.commit()

    
    
    def create_backup(self):
        try:
            backup_path = f"{self.db_path}.backup_{int(time.time())}"
            conn = self._get_connection()
            backup_conn = sqlite3.connect(backup_path)
            conn.backup(backup_conn)
            self._return_connection(conn)
            
            print(f"[Persistence] Created backup: {backup_path}")
        except Exception as e:
            print(f"[Persistence] Error creating backup: {e}")
