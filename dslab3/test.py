import random

# Configuration
TOTAL_TXNS = 2000
RW_PERCENT = 90  # 90% read-write
CROSS_SHARD_PERCENT = 20  # 20% of RW are cross-shard
SKEWNESS = 0.8  # 80% of txns use hot clients

# Shard ranges
SHARDS = {
    1: (1, 3000),
    2: (3001, 6000),
    3: (6001, 9000)
}

# Hot clients (first 20 in each shard for skewed distribution)
HOT_CLIENTS = {
    1: list(range(1, 21)),
    2: list(range(3001, 3021)),
    3: list(range(6001, 6021))
}

def get_client(cluster, skewed=True):
    if skewed and random.random() < SKEWNESS:
        return random.choice(HOT_CLIENTS[cluster])
    else:
        start, end = SHARDS[cluster]
        return random.randint(start, end)

def generate_intra_shard():
    cluster = random.choice([1, 2, 3])
    sender = get_client(cluster)
    receiver = get_client(cluster)
    while receiver == sender:
        receiver = get_client(cluster)
    return f"({sender}, {receiver}, 1)"

def generate_cross_shard():
    clusters = [1, 2, 3]
    c1, c2 = random.sample(clusters, 2)
    sender = get_client(c1)
    receiver = get_client(c2)
    return f"({sender}, {receiver}, 1)"

def generate_balance_query():
    cluster = random.choice([1, 2, 3])
    client = get_client(cluster)
    return f"({client})"

# Calculate counts
rw_count = int(TOTAL_TXNS * RW_PERCENT / 100)
ro_count = TOTAL_TXNS - rw_count
cross_shard_count = int(rw_count * CROSS_SHARD_PERCENT / 100)
intra_shard_count = rw_count - cross_shard_count

print(f"# Generating: {rw_count} RW ({cross_shard_count} cross, {intra_shard_count} intra), {ro_count} RO")

# Generate transactions
transactions = []
for _ in range(intra_shard_count):
    transactions.append(generate_intra_shard())
for _ in range(cross_shard_count):
    transactions.append(generate_cross_shard())
for _ in range(ro_count):
    transactions.append(generate_balance_query())

random.shuffle(transactions)

# Write CSV
with open("generated_test.csv", "w") as f:
    f.write("Set Number,Transactions,Live Nodes\n")
    f.write(f'1,"{transactions[0]}","[n1, n2, n3, n4, n5, n6, n7, n8, n9]"\n')
    for tx in transactions[1:]:
        f.write(f',"{tx}",\n')

print(f"Generated {len(transactions)} transactions to generated_test.csv")