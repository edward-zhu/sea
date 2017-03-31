#!/usr/bin/env python
# encoding: utf-8

# server settings

N_INDEX_SRV = 8
N_DOC_SRV = 8

FRONTEND = "http://127.0.0.1:9021/"

MULTIPROCESS = False

'''
INDEX_SRV = [
    "http://linserv2.cims.nyu.edu:35315/",
    "http://linserv2.cims.nyu.edu:35316/",
    "http://linserv2.cims.nyu.edu:35317/",
]

DOC_SRV = [
    "http://linserv2.cims.nyu.edu:35318/",
    "http://linserv2.cims.nyu.edu:35319/",
    "http://linserv2.cims.nyu.edu:35320/",
]
'''

INDEX_SRV = [
    "http://localhost:22000/",
    "http://localhost:22001/",
    "http://localhost:22002/",
    "http://localhost:22003/",
    "http://localhost:22004/",
    "http://localhost:22005/",
    "http://localhost:22006/",
    "http://localhost:22007/",
]

DOC_SRV = [
    "http://localhost:23000/",
    "http://localhost:23001/",
    "http://localhost:23002/",
    "http://localhost:23003/",
    "http://localhost:23004/",
    "http://localhost:23005/",
    "http://localhost:23006/",
    "http://localhost:23007/",
]

# data settings

DATA_DIR = "./data"
INDEX_PREFIX = "indexes"
DOC_PREFIX = "docs"
TFIDF_FILE = "tfidf.pkl"

# scorer settings

TITLE_BONUS = 100

# query settings

MAX_DOC_PER_QUERY = 10

# snippeter settings

# - snippet length in words
SNIPPET_LEN = 40

# util funcs

import os

def get_tfidf():
    return os.path.join(DATA_DIR, TFIDF_FILE)

def get_index_data(srvid):
    if (srvid > N_INDEX_SRV):
        raise (KeyError())
    return os.path.join(DATA_DIR, INDEX_PREFIX + "_%d.pkl" % srvid)

def get_doc_data(srvid):
    if (srvid > N_DOC_SRV):
        raise (KeyError())
    return os.path.join(DATA_DIR, DOC_PREFIX + "_%d.pkl" % srvid)
