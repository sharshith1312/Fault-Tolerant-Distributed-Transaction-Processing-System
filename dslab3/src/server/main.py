"""
Author: sharshith1312
Course: CSE535 - Distributed Systems
"""

import sys
import time
from concurrent import futures
import grpc
import paxos_pb2_grpc
from shared import config
from server.node import Node
from server.service import PaxosService
import signal

def serve(node_id):
    node = Node(node_id)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=28))
    paxos_pb2_grpc.add_PaxosServiceServicer_to_server(PaxosService(node), server)
    
    address = config.NODE_ADDRESSES[node_id]
    server.add_insecure_port(address)
    server.start()
    
    time.sleep(0.1)
    node.logger.info(f"Server started on {address}")
    
    # Setup graceful shutdown
    def signal_handler(signum, frame):
        node.logger.info(f"Shutting down gracefully")
        if node.leader_timer:
            node.leader_timer.cancel()
        server.stop(grace=2)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        node.logger.info("Server interrupted")
    finally:
        if node.leader_timer:
            node.leader_timer.cancel()
        server.stop(grace=0)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python -m src.server.main <node_id>")
        sys.exit(1)
    node_id = int(sys.argv[1])
    serve(node_id)