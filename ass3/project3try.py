#!/usr/bin/env python3
import sys, math
from pyspark import SparkConf, SparkContext
from operator import add

# tuple layout for a record: (SIDE, NUMID, RECID, X, Y, TERMS)
SIDE, NUMID, RECID, X, Y, TERMS = 0, 1, 2, 3, 4, 5
PREFIX_TOKS = 6  # ### NEW ###

def lineParse(line, prefix=''):
    recId, coOrd, terms = line.strip().split('#')
    numId = int(recId[1:])
    x, y = coOrd.strip('()').split(',')
    x, y = float(x), float(y)
    terms = set(terms.split())
    parsedLine = (prefix, numId, recId, x, y, terms)
    return parsedLine

def add_prefix(rec, bcFreq, s):
    side, numId, recId, x, y, terms = rec
    freq = bcFreq.value
    ordered = sorted(terms, key=lambda t: (freq.get(t, 0), t))
    n = len(ordered)
    l = int(math.ceil(n * s))
    p = n - l + 1 if n > 0 else 0
    prefix_tokens = ordered[:p]
    return (side, numId, recId, x, y, set(ordered), tuple(prefix_tokens))

# ### NEW: helpers ###
def jaccard(a_set, b_set):
    if not a_set and not b_set:
        return 1.0
    inter = len(a_set & b_set)
    if inter == 0:
        return 0.0
    union = len(a_set) + len(b_set) - inter
    return inter / float(union)

def dist2(ax, ay, bx, by):
    return (ax - bx) ** 2 + (ay - by) ** 2

def cell_of(x, y, g):
    return (int(math.floor(x / g)), int(math.floor(y / g)))

def neighbor_cells(cx, cy):
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            yield (cx + dx, cy + dy)

def post_A(rec, g):
    side, numId, recId, x, y, termset, preftoks = rec
    cx, cy = cell_of(x, y, g)
    for nx, ny in neighbor_cells(cx, cy):
        for tok in preftoks:
            yield ((nx, ny, tok), ("A", numId, recId, x, y, termset))

def post_B(rec, g):
    side, numId, recId, x, y, termset, preftoks = rec
    cx, cy = cell_of(x, y, g)
    for tok in preftoks:
        yield ((cx, cy, tok), ("B", numId, recId, x, y, termset))

def group_to_pairs(k_vals):
    _k, vals = k_vals
    As, Bs = [], []
    for v in vals:
        if v[0] == "A":
            As.append(v[1:])  # (numId, recId, x, y, termset)
        else:
            Bs.append(v[1:])
    for a in As:
        for b in Bs:
            yield (a, b)

def verify(pair, d2, s):
    (anum, arec, ax, ay, aset), (bnum, brec, bx, by, bset) = pair
    if dist2(ax, ay, bx, by) > d2:
        return None
    js = jaccard(aset, bset)
    if js + 1e-12 < s:
        return None
    return (arec, anum, brec, bnum, math.sqrt(dist2(ax, ay, bx, by)), js)

class project3:
    def run(self, inputpathA, inputpathB, outputpath, d, s):
        d = float(d)
        s = float(s)
        g = d
        d2 = d * d
        conf = SparkConf().setAppName("Project3")
        sc = SparkContext(conf=conf)

        rddA = sc.textFile(inputpathA).map(lambda line: lineParse(line, 'A'))
        rddB = sc.textFile(inputpathB).map(lambda line: lineParse(line, 'B'))

        # Global token frequencies over A ∪ B
        termPairsA = rddA.flatMap(lambda rec: [(term, 1) for term in rec[TERMS]])
        termPairsB = rddB.flatMap(lambda rec: [(term, 1) for term in rec[TERMS]])
        termFreqs = termPairsA.union(termPairsB).reduceByKey(add)
        bcFreq = sc.broadcast(dict(termFreqs.collect()))  # ### EDIT ###

        # Per-record ordered tokens + prefix tokens
        A = rddA.map(lambda rec: add_prefix(rec, bcFreq, s))
        B = rddB.map(lambda rec: add_prefix(rec, bcFreq, s))

        # Postings: grid + prefix filtering
        postings = A.flatMap(lambda r: post_A(r, g)).union(B.flatMap(lambda r: post_B(r, g)))

        # Candidates within each (cell,token)
        candidates = postings.groupByKey().flatMap(group_to_pairs)

        # Exact verify
        verified = candidates.map(lambda pr: verify(pr, d2, s)).filter(lambda x: x is not None)

        # De-dup and sort, then format and save
        verifiedDistinct = (verified
            .map(lambda t: ((t[0], t[2]), t))  # key = (Arec,Brec)
            .reduceByKey(lambda a, b: a)
            .map(lambda kv: kv[1]))

        outRDD = (verifiedDistinct
            .sortBy(lambda t: (t[1], t[3], t[0], t[2]))     # (numA, numB, Arec, Brec)
            .map(lambda t: f"({t[0]},{t[2]}):{t[4]:.6f},{t[5]:.6f}"))

        outRDD.saveAsTextFile(outputpath)
        sc.stop()

if __name__ == '__main__':
    if len(sys.argv) != 6:
        print("Wrong arguments")
        sys.exit(-1)
    project3().run(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])

