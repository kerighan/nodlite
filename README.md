# A lightweight graph database powered by SQLite

This simple yet efficient graph database is developed in pure python and built upon SQLite. The syntax is as close as possible to NetworkX.

- **Source:** https://github.com/kerighan/nodlite

## Installation

```
pip install nodlite
```

## Basic usage

```python
from nodlite import Graph

# create graph if the file doesn't already exist
G = Graph("test.db")

# add nodes
G.add_node("A")
G.add_node("B")
G.add_node("C")

# add edges
G.add_edge("A", "B")
G.add_edge("A", "C")

# get a node
print(G.node("A"))
print(G["A"])

# get out neighbors
print(list(G.neighbors("A")))

# get incoming nodes
print(list(G.predecessors("B")))

# iterating through the nodes
for node in G.nodes:
    print(node)

# iterating through the edges
for edge in G.edges:
    print(edge)
```

## Using custom attributes

With Nodlite, nodes can contain arbitrary attributes. It is stored as a blob in the sqlite database.

```python
from nodlite import Graph


G = Graph("test.db")

# using the 'add_node' method:
G.add_node("Mark", age=25, occupation="data scientist")
# or using '__setitem__':
G["Mary"] = {"age": 32, "bio": "engineer"}

# adding an edge
G.add_edge("Mark", "Mary")
```

To increase performances, consider using PyPy.
