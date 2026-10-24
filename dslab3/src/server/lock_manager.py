"""
Author: sharshith1312
Course: CSE535 - Distributed Systems
Lock Manager for 2PC Concurrency Control
"""

import threading
import logging
from typing import Optional, Dict, Any
from shared.persistence import ServerState

class LockManager:

    def __init__(self, persistence: ServerState):
        self.persistence = persistence
        self._client_locks = {}
        self._locks_lock = threading.Lock()
        self.logger = logging.getLogger(f"LockManager-N{persistence.node_id}")
    
    def _get_client_lock(self, client_id: int) -> threading.Lock:
        with self._locks_lock:
            if client_id not in self._client_locks:
                self._client_locks[client_id] = threading.Lock()
            return self._client_locks[client_id]
    
    def try_acquire_lock(self, client_id: int, transaction_id: str) -> bool:
        client_lock = self._get_client_lock(client_id)
        with client_lock:
            success = self.persistence.acquire_lock(client_id, transaction_id)
            
            if success:
                self.logger.debug(f"Lock ACQUIRED on client {client_id} by txn {transaction_id}")
            else:
                lock_info = self.persistence.get_lock_info(client_id)
                owner = lock_info['transaction_id'] if lock_info else 'unknown'
                self.logger.debug(f"Lock DENIED on client {client_id} for txn {transaction_id} (held by {owner})")
            
            return success
    
    def release_lock(self, client_id: int, transaction_id: str) -> bool:
        client_lock = self._get_client_lock(client_id)
        with client_lock:
            success = self.persistence.release_lock(client_id, transaction_id)
            
            if success:
                self.logger.debug(f"Lock RELEASED on client {client_id} by txn {transaction_id}")
            else:
                self.logger.debug(f"Lock RELEASE FAILED on client {client_id} by txn {transaction_id} (not owner)")
            
            return success
    
    def is_locked(self, client_id: int) -> bool:
        return self.persistence.is_locked(client_id)
    
    def get_lock_owner(self, client_id: int) -> Optional[str]:
        lock_info = self.persistence.get_lock_info(client_id)
        return lock_info['transaction_id'] if lock_info else None
    
    def release_all_locks_for_transaction(self, transaction_id: str) -> int:
        count = self.persistence.release_all_locks_for_transaction(transaction_id)
        if count > 0:
            self.logger.debug(f"Released {count} locks for txn {transaction_id}")
        return count
    
    def release_expired_locks(self, timeout: float = 5.0) -> int:
        count = self.persistence.release_expired_locks(timeout)
        if count > 0:
            self.logger.warning(f"Auto-released {count} expired locks (timeout={timeout}s)")
        return count
    
    def get_lock_info(self, client_id: int) -> Optional[Dict[str, Any]]:
        return self.persistence.get_lock_info(client_id)

