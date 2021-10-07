import itertools
import sqlite3
import zlib
from enum import Enum
from pickle import HIGHEST_PROTOCOL as PICKLE_PROTOCOL
from pickle import dumps, loads
from queue import Queue
from threading import Thread

__version__ = "0.0.3"


class Action(Enum):
    COMMIT = 0
    CLOSE = 1
    END = 2


class Node:
    __slots__ = ["key", "attributes"]

    def __init__(self, key, attributes):
        self.key = key
        self.attributes = attributes

    def __repr__(self):
        txt = f"Node(key='{self.key}'"
        if len(self.attributes) > 0:
            txt += ", "
        attr = []
        for key, value in self.attributes.items():
            tmp = f"{key}={value}"
            attr.append(tmp)
        txt += ", ".join(attr)
        txt += ")"
        return txt

    def __getattr__(self, name):
        return self.attributes[name]


def encode(obj):
    """Serialize an object using pickle to a binary format accepted by SQLite."""
    return sqlite3.Binary(dumps(obj, protocol=PICKLE_PROTOCOL))


def decode(obj):
    """Deserialize objects retrieved from SQLite."""
    return loads(bytes(obj))


def zip_encode(obj):
    return sqlite3.Binary(zlib.compress(dumps(obj, protocol=PICKLE_PROTOCOL)))


def zip_decode(obj):
    return loads(zlib.decompress(bytes(obj)))


class Graph:
    def __init__(
        self,
        filename,
        journal_mode="OFF",
        zip=False
    ):
        self.filename = filename
        self.journal_mode = journal_mode
        if zip:
            self.encode = zip_encode
            self.decode = zip_decode
        else:
            self.encode = encode
            self.decode = decode

        self.conn = self._new_conn()
        self._create_tables()

    def _new_conn(self):
        return GraphMultithread(self.filename, journal_mode=self.journal_mode)

    def __enter__(self):
        if not hasattr(self, "conn") or self.conn is None:
            self.conn = self._new_conn()
        return self

    def __exit__(self):
        self.close()

    def _create_tables(self):
        MAKE_TABLES = '''
            CREATE TABLE IF NOT EXISTS nodes (
                key TEXT VIRTUAL NOT NULL UNIQUE,
                attributes BLOB);
            CREATE TABLE IF NOT EXISTS edges (
                source TEXT NOT NULL,
                target TEXT NOT NULL,
                UNIQUE(source, target) ON CONFLICT IGNORE,
                FOREIGN KEY(source) REFERENCES nodes(key),
                FOREIGN KEY(target) REFERENCES nodes(key)
            )
        '''
        self.conn.executescript(MAKE_TABLES)

        CREATE_EDGE_INDEX = '''
            CREATE INDEX IF NOT EXISTS source_idx ON edges (source);
            CREATE INDEX IF NOT EXISTS target_idx ON edges (target);
        '''
        self.conn.executescript(CREATE_EDGE_INDEX)

        CREATE_COUNT_VIEW = '''
            CREATE VIEW IF NOT EXISTS count_nodes(n_nodes)
                AS SELECT COUNT(*) FROM nodes;
            CREATE VIEW IF NOT EXISTS count_edges(n_edges)
                AS SELECT COUNT(*) FROM edges;
        '''
        self.conn.executescript(CREATE_COUNT_VIEW)

        self.conn.commit()

    def close(self):
        self.conn.close()

    @property
    def n_nodes(self):
        GET_N_NODES = 'SELECT n_nodes FROM "count_nodes"'
        return next(self.conn.select(GET_N_NODES))[0]

    @property
    def n_edges(self):
        GET_N_EDGES = 'SELECT n_edges FROM "count_edges"'
        return next(self.conn.select(GET_N_EDGES))[0]

    def node(self, key):
        GET_NODE = 'SELECT key, attributes FROM "nodes" WHERE key = ?'
        try:
            node, attr = next(self.conn.select(GET_NODE, (key,)))
        except StopIteration:
            raise KeyError(f"Node '{key}' not found")

        if attr is None:
            return node
        return Node(node, self.decode(attr))

    def has_node(self, key):
        GET_NODE = 'SELECT key FROM "nodes" WHERE key = ?'
        try:
            _ = next(self.conn.select(GET_NODE, (key,)))
            return True
        except StopIteration:
            return False

    def add_node(self, key, **attributes):
        if len(attributes) == 0:
            ADD_NODE = '''
                INSERT OR IGNORE INTO "nodes" (key) VALUES (?)
            '''
            self.conn.execute(ADD_NODE, (key,))
        else:
            ADD_NODE = 'REPLACE INTO "nodes" (key, attributes) VALUES (?,?)'
            self.conn.execute(ADD_NODE, (key, self.encode(attributes)))

        self.commit()

    def add_nodes_from(self, keys):
        for key in keys:
            ADD_ITEM = 'REPLACE INTO "nodes" (key) VALUES (?)'
            self.conn.execute(ADD_ITEM, (key,))
        self.commit()

    def remove_node(self, u):
        # delete all edges starting from u
        DEL_EDGES = 'DELETE FROM "edges" WHERE source = ? or target = ?'
        self.conn.execute(DEL_EDGES, (u, u))

        DEL_NODE = 'DELETE FROM "nodes" WHERE key = ?'
        self.conn.execute(DEL_NODE, (u,))

    def edge(self, u, v):
        GET_EDGE = 'SELECT COUNT() FROM "edges" WHERE source=? AND target=?'
        n = next(self.conn.select(GET_EDGE, (u, v)))[0]
        if n == 1:
            return (u, v)
        raise KeyError

    def has_edge(self, u, v):
        GET_EDGE = 'SELECT COUNT() FROM "edges" WHERE source=? AND target=?'
        n = next(self.conn.select(GET_EDGE, (u, v)))[0]
        if n == 1:
            return True
        return False

    def add_edge(self, source, target):
        ADD_NODES = 'INSERT OR IGNORE INTO "nodes" (key) VALUES (?), (?)'
        self.conn.execute(ADD_NODES, (source, target))

        ADD_EDGE = '''
            INSERT OR IGNORE INTO "edges" (source, target) VALUES (?, ?)
        '''
        self.conn.execute(ADD_EDGE, (source, target))
        self.commit()

    def add_edges_from(self, edges):
        n_edges = len(edges)
        if n_edges == 0:
            return
        edges = list(itertools.chain(*edges))
        nodes = list(set(edges))

        QUERY = ", ".join(["(?)" for _ in range(len(nodes))])
        ADD_NODES = 'INSERT OR IGNORE INTO "nodes" (key) VALUES ' + QUERY
        self.conn.execute(ADD_NODES, nodes)

        QUERY = ", ".join(["(?, ?)" for _ in range(n_edges)])
        ADD_EDGES = 'INSERT OR IGNORE INTO "edges" (source, target) VALUES'
        ADD_EDGES += QUERY
        self.conn.execute(ADD_EDGES, edges)
        self.conn.commit()

    def remove_edge(self, u, v):
        # delete all edges starting from u
        DEL_EDGES = 'DELETE FROM "edges" WHERE source = ? and target = ?'
        self.conn.execute(DEL_EDGES, (u, v))

    def neighbors(self, source):
        GET_NEIGHBORS = '''
            SELECT target FROM "edges" WHERE source = ?
        '''
        for it in self.conn.select(GET_NEIGHBORS, (source,)):
            yield it[0]
    
    def random_neighbors(self, source, n=1):
        GET_RANDOM_NEIGHBORS = f'''
            SELECT target FROM "edges" WHERE source = ?
            ORDER BY RANDOM() LIMIT {n};
        '''
        if n == 1:
            return next(self.conn.select(GET_RANDOM_NEIGHBORS, (source,)))[0]
        else:
            res = []
            for it in self.conn.select(GET_RANDOM_NEIGHBORS, (source,)):
                res.append(it[0])
            return res

    def neighbors_from(self, nodes):
        QUERY = ', '.join(["?" for _ in range(len(nodes))])
        GET_NEIGHBORS = f'''
            SELECT source, target FROM "edges" WHERE source IN ({QUERY})
        '''
        data = {}
        for src, tgt in self.conn.select(GET_NEIGHBORS, nodes):
            data.setdefault(src, []).append(tgt)
        return data

    def set_neighbors(self, u, neighbors):
        # delete all edges starting from u
        DEL_EDGES = 'DELETE FROM "edges" WHERE source = ?'
        self.conn.execute(DEL_EDGES, (u,))

        # add all edges
        edges = [(u, tgt) for tgt in neighbors]
        self.add_edges_from(edges)

    def predecessors(self, target):
        GET_PREDECESSORS = '''
            SELECT source FROM "edges" WHERE target = ?
        '''
        for it in self.conn.select(GET_PREDECESSORS, (target,)):
            yield it[0]

    def random_predecessors(self, target, n=1):
        GET_RANDOM_PREDECESSORS = f'''
            SELECT source FROM "edges" WHERE target = ?
            ORDER BY RANDOM() LIMIT {n};
        '''
        if n == 1:
            return next(self.conn.select(GET_RANDOM_PREDECESSORS, (target,)))[0]
        else:
            res = []
            for it in self.conn.select(GET_RANDOM_PREDECESSORS, (target,)):
                res.append(it[0])
            return res

    def predecessors_from(self, nodes):
        QUERY = ', '.join(["?" for _ in range(len(nodes))])
        GET_PREDECESSORS = f'''
            SELECT source, target FROM "edges" WHERE target IN ({QUERY})
        '''
        data = {}
        for src, tgt in self.conn.select(GET_PREDECESSORS, nodes):
            data.setdefault(tgt, []).append(src)
        return data

    def set_predecessors(self, u, predecessors):
        # delete all edges starting from u
        DEL_EDGES = 'DELETE FROM "edges" WHERE target = ?'
        self.conn.execute(DEL_EDGES, (u, 2))

        # add all edges
        edges = [(tgt, u) for tgt in predecessors]
        self.add_edges_from(edges)

    def subgraph(self, nodes):
        QUERY = ', '.join(["?" for _ in range(len(nodes))])
        GET_SUBGRAPH = f'''
            SELECT source, target FROM "edges"
            WHERE source IN ({QUERY}) AND target IN ({QUERY})
        '''
        for it in self.conn.select(GET_SUBGRAPH, nodes * 2):
            yield it

    def degree(self, source):
        GET_DEGREE = '''
            SELECT COUNT(target) FROM "edges" WHERE source = ?
        '''
        return next(self.conn.select(GET_DEGREE, (source,)))[0]

    @ property
    def nodes(self):
        GET_NODES = 'SELECT key, attributes FROM "nodes" ORDER BY rowid'
        for it in self.conn.select(GET_NODES):
            if it[1] is None:
                yield it[0]
            yield Node(it[0], self.decode(it[1]))

    @ property
    def edges(self):
        GET_EDGES = 'SELECT source, target FROM "edges" ORDER BY rowid'
        for it in self.conn.select(GET_EDGES):
            yield it

    def batch_get_nodes(self, batch_size=100, page=0):
        offset = page * batch_size
        GET_NODES = f'''
        SELECT key FROM "nodes" ORDER BY rowid
        LIMIT {batch_size} OFFSET {offset}
        '''
        for it in self.conn.select(GET_NODES):
            yield it[0]

    def batch_get_edges(self, batch_size=100, page=0):
        offset = page * batch_size
        GET_EDGES = f'''
        SELECT source, target FROM "edges" ORDER BY rowid
        LIMIT {batch_size} OFFSET {offset}
        '''
        for it in self.conn.select(GET_EDGES):
            yield it

    def __getitem__(self, key):
        if isinstance(key, tuple):
            return self.edge(key[0], key[1])
        return self.node(key)

    def __delitem__(self, key):
        if isinstance(key, tuple):
            self.remove_edge(key[0], key[1])
        else:
            self.remove_node(key)

    def __setitem__(self, key, attributes):
        self.add_node(key, **attributes)

    def commit(self):
        if self.conn is not None:
            self.conn.commit()

    def clear(self):
        CLEAR_ALL = '''
            DELETE FROM "nodes";
            DELETE FROM "edges";
        '''
        self.conn.executescript(CLEAR_ALL)
        self.conn.commit()

    def __str__(self):
        return (f"Graph(filename={self.filename}, "
                f"n_nodes={self.n_nodes}, n_edges={self.n_edges})")

    def __repr__(self):
        return str(self)

    def __contains__(self, key):
        if isinstance(key, tuple):
            return self.has_edge(key[0], key[1])
        return self.has_node(key)

    def __del__(self):
        self.close()


class GraphMultithread(Thread):
    def __init__(self, filename, journal_mode):
        super(GraphMultithread, self).__init__()
        self.conn = sqlite3.connect(filename)
        self.filename = filename
        self.reqs = Queue()
        self.journal_mode = journal_mode
        self._ready = False
        self.setDaemon(True)
        self.start()

    def run(self):
        conn = sqlite3.connect(
            self.filename, isolation_level=None, check_same_thread=False)
        conn.text_factory = lambda x: x.decode("utf8")
        cursor = conn.cursor()

        conn.execute(f'PRAGMA journal_mode={self.journal_mode}')
        conn.commit()
        cursor = conn.cursor()
        cursor.execute('PRAGMA synchronous=OFF')
        conn.commit()
        self._ready = True

        res = None
        while True:
            req, arg, res = self.reqs.get()
            if req == Action.COMMIT:
                conn.commit()
                if res:
                    res.put(Action.END)
            elif req == Action.CLOSE:
                break
            else:
                cursor.execute(req, arg)
                if res:
                    records = cursor.fetchall()
                    for rec in records:
                        res.put(rec)
                    res.put(Action.END)
                # conn.commit()
        conn.close()

    def execute(self, req, arg=None, res=None):
        self.reqs.put((req, arg or tuple(), res))

    def executescript(self, req, arg=None, res=None):
        for r in req.split(";"):
            self.execute(r)

    def commit(self):
        self.select_one(Action.COMMIT)

    def select_one(self, req, arg=None):
        try:
            return next(iter(self.select(req, arg)))
        except StopIteration:
            return None

    def close(self):
        self.execute(Action.CLOSE)

    def select(self, req, arg=None):
        res = Queue()
        self.execute(req, arg, res)
        while True:
            rec = res.get()
            if rec == Action.END:
                break
            yield rec
