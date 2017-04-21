#!/usr/bin/env python
# encoding: utf-8

# server settings
import os

N_INDEX_SRV = 8
N_DOC_SRV = 8

BASE_PORT = int(os.environ.get("BASE_PORT", default="24000")) #need to be exported
FRONT_PORT = int(os.environ.get("FRONT_PORT", default="22333")) #need to be exported
FRONTEND = "http://localhost:%d/" % (FRONT_PORT, )

MULTIPROCESS = False

INDEX_SRV = []
for i in range(0, N_INDEX_SRV):
    temp = int(BASE_PORT)+i
    INDEX_SRV.append("http://localhost:"+str(temp)+"/")

DOC_SRV = []
for i in range(0, N_DOC_SRV):
    temp = int(BASE_PORT)+i+1000
    DOC_SRV.append("http://localhost:"+str(temp)+"/")

# data settings
DATA_DIR = os.environ.get("SEARCH_DATA_DIR", "data/output1") #need to be expoerted
INDEX_PREFIX = "indexes"
DOC_PREFIX = "docs"
TFIDF_FILE = "tfidf.pkl"

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
    return os.path.join(DATA_DIR, INDEX_PREFIX + "_%d.pkl.bz2" % srvid)

def get_doc_data(srvid):
    if (srvid > N_DOC_SRV):
        raise (KeyError())
    return os.path.join(DATA_DIR, DOC_PREFIX + "_%d.pkl.bz2" % srvid)
