"""
Author: sharshith1312
Course: CSE535 - Distributed Systems
Transaction Utilities for Multi-Cluster 2PC
"""

import re
import time
import random
from shared import config

def parse_transaction(transaction_str: str):

    transaction_str = transaction_str.strip()
    
    balance_pattern = r'\((\d+)\)'
    match = re.match(balance_pattern, transaction_str)
    if match:
        client_id = int(match.group(1))
        return {
            'type': 'BALANCE',
            'client_id': client_id
        }
    
    transfer_pattern = r'\((\d+)\s*,\s*(\d+)\s*,\s*([0-9.]+)\)'
    match = re.match(transfer_pattern, transaction_str)
    if match:
        sender = int(match.group(1))
        receiver = int(match.group(2))
        amount = float(match.group(3))
        return {
            'type': 'TRANSFER',
            'sender': sender,
            'receiver': receiver,
            'amount': amount
        }
    
    return None

def generate_transaction_id(client_id: int) -> str:
    timestamp = int(time.time() * 1000000)
    random_suffix = random.randint(1000, 9999)
    return f"{client_id}_{timestamp}_{random_suffix}"

def is_balance_query(transaction_str: str) -> bool:
    parsed = parse_transaction(transaction_str)
    return parsed is not None and parsed['type'] == 'BALANCE'

def is_cross_shard(sender: int, receiver: int) -> bool:
    sender_cluster = config.get_cluster_for_client(sender)
    receiver_cluster = config.get_cluster_for_client(receiver)
    return sender_cluster != receiver_cluster

def get_transaction_type(transaction_str: str) -> str:
    parsed = parse_transaction(transaction_str)
    
    if parsed is None:
        return 'UNKNOWN'
    
    if parsed['type'] == 'BALANCE':
        return 'BALANCE'
    
    sender = parsed['sender']
    receiver = parsed['receiver']
    
    if is_cross_shard(sender, receiver):
        return 'CROSS'
    else:
        return 'INTRA'

def format_transaction(sender: int, receiver: int, amount: float) -> str:
    return f"({sender},{receiver},{amount})"

def validate_transaction(parsed_txn: dict, datastore: dict) -> tuple:
    if parsed_txn is None:
        return False, "Invalid transaction format"
    
    if parsed_txn['type'] == 'BALANCE':
        client_id = parsed_txn['client_id']
        if client_id not in datastore:
            return False, f"Client {client_id} not found"
        return True, ""
    
    if parsed_txn['type'] == 'TRANSFER':
        sender = parsed_txn['sender']
        receiver = parsed_txn['receiver']
        amount = parsed_txn['amount']
        
        if sender not in datastore:
            return False, f"Sender {sender} not found"
        
        if datastore[sender] < amount:
            return False, f"Insufficient funds: sender {sender} has {datastore[sender]}, needs {amount}"
        
        return True, ""
    
    return False, "Unknown transaction type"

