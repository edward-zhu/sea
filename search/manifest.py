#!/usr/bin/env python
# encoding: utf-8

# server settings
import os

N_INDEX_SRV = 8
N_DOC_SRV = 8

# data settings
DATA_BASE = os.environ.get("DATA_BASE", "data/") #need to be expoerted
ETCD_CLUSTER = os.environ.get("ETCD_CLUSTER", None)

# query settings
MAX_DOC_PER_QUERY = 100

# snippeter settings
SNIPPET_LEN = 20

MASTER_PORT = os.environ.get("MASTER_PORT", "11111")
