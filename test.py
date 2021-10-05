import os
import time
from random import randint

from tqdm import tqdm

from nodlite import Graph

if os.path.exists("test.db"):
    os.remove("test.db")

N = 100000
M = N * 10
G = Graph("test.db")

G.add_node(0, name="my name is 0")
for i in tqdm(range(M)):
    G.add_edge(randint(0, N-1), randint(0, N-1))

start = time.time()
print(G.n_nodes)
print(G.n_edges)
print(time.time() - start)
