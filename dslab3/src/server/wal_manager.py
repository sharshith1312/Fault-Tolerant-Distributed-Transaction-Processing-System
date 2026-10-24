"""
Author: sharshith1312
Course: CSE535 - Distributed Systems
Write-Ahead Log Manager for 2PC Rollback
"""

import threading
import logging
from typing import List, Dict, Any
from shared.persistence import ServerState
from shared import config

class WALManager:
    
    def __init__(self, persistence: ServerState):
        self.persistence = persistence
        self.lock = threading.Lock()
        self.logger = logging.getLogger(f"WALManager-N{persistence.node_id}")
    
    def log_operation(self, transaction_id: str, sequence_num: int,
                      client_id: int, old_balance: float, new_balance: float,
                      operation: str, phase: str) -> bool:
        with self.lock:
            success = self.persistence.write_wal_entry(
                transaction_id, sequence_num, client_id,
                old_balance, new_balance, operation, phase
            )
            
            if success:
                self.logger.debug(
                    f"WAL logged: txn={transaction_id}, client={client_id}, "
                    f"{old_balance}->{new_balance}, op={operation}, phase={phase}"
                )
            else:
                self.logger.error(f"Failed to log WAL entry for txn {transaction_id}")
            
            return success
    
    def get_operations(self, transaction_id: str) -> List[Dict[str, Any]]:
        return self.persistence.get_wal_entries(transaction_id)
    
    def rollback_transaction(self, transaction_id: str, datastore: dict) -> bool:

        with self.lock:
            entries = self.persistence.get_wal_entries(transaction_id)
            
            if not entries:
                self.logger.debug(f"No WAL entries to rollback for txn {transaction_id}")
                return True
            
            self.logger.info(f"Rolling back {len(entries)} operations for txn {transaction_id}")
            
            # Rollback in database and get entries
            rolled_back_entries = self.persistence.rollback_from_wal(transaction_id)
            
            if rolled_back_entries:
                # Update in-memory datastore
                for entry in rolled_back_entries:
                    client_id = entry['client_id']
                    old_balance = entry['old_balance']
                    datastore[client_id] = old_balance
                
                self.logger.info(f"Rollback completed for txn {transaction_id}")
                
                if not config.KEEP_WAL_AFTER_COMMIT:
                    self.persistence.clear_wal(transaction_id)
                    self.logger.debug(f"Cleared WAL for txn {transaction_id}")
                
                return True
            else:
                self.logger.error(f"Rollback FAILED for txn {transaction_id}")
                return False
    
    def clear_transaction(self, transaction_id: str) -> bool:
        with self.lock:
            success = self.persistence.clear_wal(transaction_id)
            
            if success:
                self.logger.debug(f"Cleared WAL for txn {transaction_id}")
            
            return success
    
    def has_wal_entries(self, transaction_id: str) -> bool:

        return self.persistence.has_wal_entries(transaction_id)
    
    def finalize_commit(self, transaction_id: str) -> bool:
        if not config.KEEP_WAL_AFTER_COMMIT:
            return self.clear_transaction(transaction_id)
        else:
            self.logger.debug(f"Keeping WAL for txn {transaction_id} (KEEP_WAL_AFTER_COMMIT=True)")
            return True

