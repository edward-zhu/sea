#!/usr/bin/env python
# encoding: utf-8

# server settings
import os

N_INDEX_SRV = 4
N_DOC_SRV = 4

# data settings
DATA_BASE = os.environ.get("DATA_BASE", "data/") #need to be expoerted

# query settings
MAX_DOC_PER_QUERY = 100

# snippeter settings
SNIPPET_LEN = 80
