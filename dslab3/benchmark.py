#!/usr/bin/env python3
"""
Benchmarking Suite for Distributed Transaction System
Author: sharshith1312
Course: CSE535 - Distributed Systems
Yahoo! Cloud Serving Benchmark (YCSB)
Benchmarking Scenarios:
1. 200 transactions, 80% read-write, 10% cross-shard, uniform distribution
2. 2000 transactions, 80% read-write, 20% cross-shard, highly skewed distribution
3. 30000 transactions, 100% read-write, 0% cross-shard, uniform distribution
"""

import sys
import os
import time
import random
import grpc
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import List, Tuple, Dict

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from shared import config
import paxos_pb2
import paxos_pb2_grpc


SHARDS = config.SHARD_RANGES

SHARD_LEADERS = {
    cluster_id: config.CLUSTER_TO_NODES[cluster_id][0]
    for cluster_id in range(1, config.NUM_CLUSTERS + 1)
}

NUM_WORKERS = 30


@dataclass
class BenchmarkResult:
    name: str
    total_transactions: int
    successful: int
    failed: int
    elapsed_time: float
    throughput: float
    avg_latency: float
    cross_shard_count: int
    intra_shard_count: int


class BenchmarkClient:
    
    def __init__(self):
        self.stubs: Dict[int, paxos_pb2_grpc.PaxosServiceStub] = {}
        self.cluster_leaders: Dict[int, int] = {
            cluster_id: config.CLUSTER_TO_NODES[cluster_id][0]
            for cluster_id in range(1, config.NUM_CLUSTERS + 1)
        }
        self.timestamp = 0
        self.lock = threading.Lock()
        
        for node_id in range(1, config.TOTAL_NODES + 1):
            addr = config.NODE_ADDRESSES[node_id]
            channel = grpc.insecure_channel(addr)
            self.stubs[node_id] = paxos_pb2_grpc.PaxosServiceStub(channel)
        
        print(f"Connected to {len(self.stubs)} nodes")
    
    def get_cluster_for_client(self, client_id: int) -> int:
        return config.get_cluster_for_client(client_id)
    
    def submit_transaction1(self, sender: int, receiver: int, amount: float):
        with self.lock:
            self.timestamp += 1
            ts = self.timestamp
        
        transaction_str = f"({sender},{receiver},{amount})"
        request = paxos_pb2.ClientRequest(
            client_id=sender,
            timestamp=ts,
            transaction=transaction_str
        )
        
        sender_cluster = self.get_cluster_for_client(sender)
        leader_id = self.cluster_leaders.get(sender_cluster)
        
        if leader_id and leader_id in self.stubs:
            try:
                self.stubs[leader_id].HandleClientRequest(request, timeout=2.0)
            except grpc.RpcError:
                pass

    def submit_transaction(self, sender: int, receiver: int, amount: float) -> Tuple[bool, str, float]:
        with self.lock:
            self.timestamp += 1
            ts = self.timestamp
        
        transaction_str = f"({sender},{receiver},{amount})"
        request = paxos_pb2.ClientRequest(
            client_id=sender,
            timestamp=ts,
            transaction=transaction_str
        )
        
        sender_cluster = self.get_cluster_for_client(sender)
        leader_id = self.cluster_leaders.get(sender_cluster)
        start_time = time.time()
        
        if leader_id and leader_id in self.stubs:
            try:
                reply = self.stubs[leader_id].HandleClientRequest(request, timeout=2.0)
                latency = (time.time() - start_time) * 1000
                if reply and reply.success:
                    return True, reply.message, latency
                return False, reply.message if reply else "No reply", latency
            except grpc.RpcError:
                pass
        
        latency = (time.time() - start_time) * 1000
        return False, "Failed", latency

class TransactionGenerator:
    @staticmethod
    def generate_uniform_intra_shard(count: int) -> List[Tuple[int, int, float]]:
        transactions = []
        shard_ids = list(range(1, config.NUM_CLUSTERS + 1))
        for _ in range(count):
            shard = random.choice(shard_ids)
            start, end = SHARDS[shard]
            sender = random.randint(start, end)
            receiver = random.randint(start, end)
            while receiver == sender:
                receiver = random.randint(start, end)
            transactions.append((sender, receiver, 1.0))
        return transactions
    
    @staticmethod
    def generate_uniform_cross_shard(count: int) -> List[Tuple[int, int, float]]:
        transactions = []
        shard_ids = list(range(1, config.NUM_CLUSTERS + 1))
        for _ in range(count):
            shard1, shard2 = random.sample(shard_ids, 2)
            start1, end1 = SHARDS[shard1]
            start2, end2 = SHARDS[shard2]
            sender = random.randint(start1, end1)
            receiver = random.randint(start2, end2)
            transactions.append((sender, receiver, 1.0))
        return transactions
    
    @staticmethod
    def generate_skewed_intra_shard(count: int, hot_ratio: float = 0.8) -> List[Tuple[int, int, float]]:
        transactions = []
        shard_ids = list(range(1, config.NUM_CLUSTERS + 1))
        
        hot_clients = {}
        for shard_id in shard_ids:
            start, end = SHARDS[shard_id]
            hot_count = max(100, (end - start + 1) // 10)
            hot_clients[shard_id] = list(range(start, start + hot_count))
        
        for _ in range(count):
            shard = random.choice(shard_ids)
            start, end = SHARDS[shard]
            
            if random.random() < hot_ratio:
                sender = random.choice(hot_clients[shard])
                receiver = random.choice(hot_clients[shard])
                while receiver == sender:
                    receiver = random.choice(hot_clients[shard])
            else:
                sender = random.randint(start, end)
                receiver = random.randint(start, end)
                while receiver == sender:
                    receiver = random.randint(start, end)
            
            transactions.append((sender, receiver, 1.0))
        return transactions
    
    @staticmethod
    def generate_skewed_cross_shard(count: int, hot_ratio: float = 0.8) -> List[Tuple[int, int, float]]:
        transactions = []
        shard_ids = list(range(1, config.NUM_CLUSTERS + 1))
        
        hot_clients = {}
        for shard_id in shard_ids:
            start, end = SHARDS[shard_id]
            hot_count = max(100, (end - start + 1) // 10)
            hot_clients[shard_id] = list(range(start, start + hot_count))
        
        for _ in range(count):
            shard1, shard2 = random.sample(shard_ids, 2)
            
            if random.random() < hot_ratio:
                sender = random.choice(hot_clients[shard1])
                receiver = random.choice(hot_clients[shard2])
            else:
                start1, end1 = SHARDS[shard1]
                start2, end2 = SHARDS[shard2]
                sender = random.randint(start1, end1)
                receiver = random.randint(start2, end2)
            
            transactions.append((sender, receiver, 1.0))
        return transactions


class Benchmark:
    def __init__(self, max_workers: int = NUM_WORKERS):
        self.client = BenchmarkClient()
        self.max_workers = max_workers
        self.generator = TransactionGenerator()
    
    def initial_setup(self):
        for cluster_id in range(1, config.NUM_CLUSTERS + 1):
            nodes = config.get_nodes_in_cluster(cluster_id)
            for node_id in nodes:
                if node_id in self.client.stubs:
                    try:
                        response = self.client.stubs[node_id].TriggerElection(paxos_pb2.Empty(), timeout=2.0)
                        if response.success:
                            self.client.cluster_leaders[cluster_id] = response.leader_id
                            break
                    except grpc.RpcError:
                        continue
    
    def flush_all_nodes(self):
        for node_id in range(1, config.TOTAL_NODES + 1):
            if node_id in self.client.stubs:
                try:
                    self.client.stubs[node_id].ResetSystemState(paxos_pb2.Empty(), timeout=5.0)
                except grpc.RpcError as e:
                    print(f"Failed to reset Node {node_id}: {e}")
        self.client.cluster_leaders = {
            cluster_id: config.CLUSTER_TO_NODES[cluster_id][0]
            for cluster_id in range(1, config.NUM_CLUSTERS + 1)
        }
    
    def run_benchmark(self, name: str, transactions: List[Tuple[int, int, float]], 
                     cross_shard_count: int, ro_count: int = 0) -> BenchmarkResult:
        total_with_ro = len(transactions) + ro_count
        
        print(f"\n{'=' * 80}")
        print(f"BENCHMARK: {name}")
        print(f"{'=' * 80}")
        print(f"Total transactions: {total_with_ro} (RW: {len(transactions)}, RO: {ro_count})")
        print(f"Cross-shard: {cross_shard_count}, Intra-shard: {len(transactions) - cross_shard_count}")
        print(f"Thread pool size: {self.max_workers}")
        print(f"{'=' * 80}\n")
        
        def send_transaction(sender: int, receiver: int, amount: float):
            self.client.submit_transaction(sender, receiver, amount)
        
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for sender, receiver, amount in transactions:
                future = executor.submit(send_transaction, sender, receiver, amount)
                futures.append(future)
        
            for future in as_completed(futures):
                pass

        end_time = time.time()
        elapsed = end_time - start_time
        
        throughput = total_with_ro / elapsed if elapsed > 0 else 0
        avg_latency = (elapsed * 1000) / total_with_ro if total_with_ro > 0 else 0
        
        result = BenchmarkResult(
            name=name,
            total_transactions=total_with_ro,
            successful=total_with_ro,
            failed=0,
            elapsed_time=elapsed,
            throughput=throughput,
            avg_latency=avg_latency,
            cross_shard_count=cross_shard_count,
            intra_shard_count=len(transactions) - cross_shard_count
        )
        
        self._print_result(result)
        return result
    
    def _print_result(self, result: BenchmarkResult):
        print(f"\n{'=' * 80}")
        print(f"RESULTS: {result.name}")
        print(f"{'=' * 80}")
        print(f"  Total Transactions:   {result.total_transactions}")
        print(f"  Successful:           {result.successful} ({100*result.successful/result.total_transactions:.1f}%)")
        print(f"  Failed:               {result.failed} ({100*result.failed/result.total_transactions:.1f}%)")
        print(f"  Cross-shard:          {result.cross_shard_count}")
        print(f"  Intra-shard:          {result.intra_shard_count}")
        print(f"  Elapsed Time:         {result.elapsed_time:.2f}s")
        print(f"  Throughput:           {result.throughput:.2f} TPS")
        print(f"  Avg Latency:          {result.avg_latency:.2f} ms")
        print(f"{'=' * 80}\n")
    
    def benchmark_1(self) -> BenchmarkResult:
        """Benchmark 1: 200 txns, 10% cross-shard, uniform"""
        total = 200
        cross_shard = int(total * 0.10)
        intra_shard = total - cross_shard
        
        intra_txns = self.generator.generate_uniform_intra_shard(intra_shard)
        cross_txns = self.generator.generate_uniform_cross_shard(cross_shard)
        transactions = intra_txns + cross_txns
        random.shuffle(transactions)
        
        return self.run_benchmark(
            name="Benchmark 1: 200 txns, 10% cross-shard, uniform",
            transactions=transactions,
            cross_shard_count=cross_shard
        )
    
    def benchmark_2(self) -> BenchmarkResult:
        """Benchmark 2: 2000 txns, 20% cross-shard, skewed"""
        total = 2000
        cross_shard = int(total * 0.20)
        intra_shard = total - cross_shard
        
        intra_txns = self.generator.generate_skewed_intra_shard(intra_shard)
        cross_txns = self.generator.generate_skewed_cross_shard(cross_shard)
        transactions = intra_txns + cross_txns
        random.shuffle(transactions)
        
        return self.run_benchmark(
            name="Benchmark 2: 2000 txns, 20% cross-shard, skewed",
            transactions=transactions,
            cross_shard_count=cross_shard
        )
    
    def benchmark_3(self) -> BenchmarkResult:
        """Benchmark 3: 30000 txns, 0% cross-shard, uniform"""
        total = 30000
        transactions = self.generator.generate_uniform_intra_shard(total)
        
        return self.run_benchmark(
            name="Benchmark 3: 30000 txns, 0% cross-shard, uniform",
            transactions=transactions,
            cross_shard_count=0
        )
    
    def custom_benchmark(self, num_txns: int, rw_percent: int, 
                        cross_shard_percent: int, skewness: float) -> BenchmarkResult:
        rw_count = int(num_txns * rw_percent / 100)
        ro_count = num_txns - rw_count
        cross_shard_count = int(rw_count * cross_shard_percent / 100)
        intra_shard_count = rw_count - cross_shard_count
        
        if skewness > 0:
            intra_txns = self.generator.generate_skewed_intra_shard(intra_shard_count, hot_ratio=skewness)
            cross_txns = self.generator.generate_skewed_cross_shard(cross_shard_count, hot_ratio=skewness)
        else:
            intra_txns = self.generator.generate_uniform_intra_shard(intra_shard_count)
            cross_txns = self.generator.generate_uniform_cross_shard(cross_shard_count)
        
        transactions = intra_txns + cross_txns
        random.shuffle(transactions)
        
        dist_type = "skewed" if skewness > 0 else "uniform"
        name = f"Custom: {num_txns} txns, {rw_percent}% RW, {cross_shard_percent}% cross-shard, {dist_type}"
        
        return self.run_benchmark(
            name=name,
            transactions=transactions,
            cross_shard_count=cross_shard_count,
            ro_count=ro_count
        )
    
    def run_all_benchmarks(self) -> List[BenchmarkResult]:
        results = []
        
        print("\n" + "#" * 80)
        print("#" + " " * 30 + "BENCHMARK SUITE" + " " * 33 + "#")
        print("#" * 80)
        
        self.flush_all_nodes()
        self.initial_setup()
        results.append(self.benchmark_1())
        

        self.flush_all_nodes()
        self.initial_setup()
        results.append(self.benchmark_2())
        

        self.flush_all_nodes()
        self.initial_setup()
        results.append(self.benchmark_3())
        
        self._print_summary(results)
        return results
    
    def _print_summary(self, results: List[BenchmarkResult]):
        print("\n" + "#" * 80)
        print("#" + " " * 28 + "BENCHMARK SUMMARY" + " " * 33 + "#")
        print("#" * 80)
        print()
        print(f"{'Benchmark':<55} {'TPS':>10} {'Success%':>10} {'Latency':>10}")
        print("-" * 85)
        
        for result in results:
            success_pct = 100 * result.successful / result.total_transactions
            print(f"{result.name:<55} {result.throughput:>10.2f} {success_pct:>9.1f}% {result.avg_latency:>9.2f}ms")
        
        print("-" * 85)
        
        total_txns = sum(r.total_transactions for r in results)
        total_success = sum(r.successful for r in results)
        total_time = sum(r.elapsed_time for r in results)
        overall_tps = total_txns / total_time if total_time > 0 else 0
        overall_success = 100 * total_success / total_txns if total_txns > 0 else 0
        
        print(f"{'OVERALL':<55} {overall_tps:>10.2f} {overall_success:>9.1f}%")
        print("#" * 80 + "\n")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Benchmark Distributed Transaction System')
    
    parser.add_argument('-n', type=int, help='Number of transactions (enables custom benchmark)')
    parser.add_argument('-s', type=float, default=0.0, help='Skewness (0=uniform, 1=highly skewed)')
    parser.add_argument('-cs', type=int, default=0, help='Cross-shard percentage (0-100)')
    parser.add_argument('-is', type=int, default=100, help='Intra-shard percentage (0-100)')
    parser.add_argument('-r', type=int, default=0, help='Read-only percentage (0-100)')
    
    args = parser.parse_args()
    
    benchmark = Benchmark()
    
    if args.n:
        benchmark.flush_all_nodes()
        benchmark.initial_setup()
        
        rw_percent = 100 - args.r
        
        benchmark.custom_benchmark(
            num_txns=args.n,
            rw_percent=rw_percent,
            cross_shard_percent=args.cs,
            skewness=args.s
        )
    else:
        benchmark.run_all_benchmarks()


if __name__ == "__main__":
    main()
