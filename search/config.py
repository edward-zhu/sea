'''
config.py

Config class
'''
import os
import sys
import socket
import time
import logging

import etcd

from search.common import get_etcd_cli
from search import manifest

LOG_FMT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format=LOG_FMT)
logger = logging.getLogger(__name__)

def get_internal_ip():
    '''get host internal ip'''
    return socket.gethostbyname(socket.gethostname())

class Config:
    '''search engine configures'''

    INDEX_PREFIX = "indexes"
    DOC_PREFIX = "docs"
    TFIDF_FILE = "tfidf.pkl"

    def __init__(self, srvid, n_index, n_doc, data_base, front_port, srv_desc,
                 base_port=24000, max_q_doc=100, snippet_len=80):
        self.n_index = n_index
        self.n_doc = n_doc

        self.base_port = base_port #need to be exported
        self.front_port = front_port #need to be exported

        print(self.front_port)
        self.frontend = "http://localhost:%d/" % (self.front_port, )
        self.srv_desc = srv_desc

        self.index_srv = []
        for i in range(0, self.n_index):
            temp = self.base_port + i
            self.index_srv.append("http://localhost:%d/" % (temp, ))

        self.doc_srv = []
        for i in range(0, self.n_doc):
            temp = int(self.base_port) + i + 1000
            self.doc_srv.append("http://localhost:%d/" % (temp, ))

        # data settings
        self.data_base = data_base #need to be expoerted
        self.srvid = srvid
        self.data_dir = os.path.join(self.data_base, str(self.srvid))

        # query settings
        self.max_q_doc = max_q_doc
        self.snippet_len = snippet_len

    def index_srv_port(self, shardid):
        '''get index server port'''
        if shardid > self.n_index:
            raise KeyError()
        return self.base_port + shardid

    def doc_srv_port(self, shardid):
        '''get doc server port'''
        if shardid > self.n_doc:
            raise KeyError()
        return self.base_port + 1000 + shardid

    def get_tfidf(self):
        '''get tfidf file'''
        return os.path.join(self.data_dir, Config.TFIDF_FILE)

    def get_index_data(self, shardid):
        '''get index shard'''
        if shardid > self.n_index:
            raise KeyError()
        return os.path.join(self.data_dir, Config.INDEX_PREFIX + "_%d.pkl.bz2" % shardid)

    def get_doc_data(self, shardid):
        '''get docs shard'''
        if shardid > self.n_doc:
            raise KeyError()
        return os.path.join(self.data_dir, Config.DOC_PREFIX + "_%d.pkl.bz2" % shardid)

    def host(self):
        '''get frontend address'''
        return "http://%s:%d" % (get_internal_ip(), self.front_port, )

class EtcdConfigFactory:
    '''Generate config from etcd setting'''
    def __init__(self, front_port=22333):
        self.cli = get_etcd_cli()
        self.host = get_internal_ip()
        self.front_port = front_port

    def _get_until_exist(self, key):
        while True:
            try:
                val = self.cli.read(key).value
                return val
            except etcd.EtcdKeyNotFound:
                logger.info("No key: %s found, sleep 1 second...", key)
                time.sleep(1)

    def _srv_desc(self):
        return "http://%s:%d" % (self.host, self.front_port, )

    def _get_srv_id(self, n_srv):
        i = 0
        while True:
            try:
                self.cli.write(
                    "/misaki/srvs/%d" % (i, ), self._srv_desc(), ttl=10, prevExist=False)
                return i
            except etcd.EtcdAlreadyExist:
                pass
            logger.info("srv %d occupied.", i)
            i = (i + 1) % n_srv

            # if all slot is full, sleep 30 sec. and retry
            if i == 0:
                logger.info("no slot available, sleep 30 sec.")
                time.sleep(30)

    def get_cfg(self):
        n_srv = int(self._get_until_exist("/misaki/n_srv"))
        srvid = self._get_srv_id(n_srv)
        logger.info("successfully occupied srv %d for 10 sec.", srvid)
        return Config(
            srvid, manifest.N_INDEX_SRV, manifest.N_DOC_SRV, manifest.DATA_BASE,
            self.front_port, self._srv_desc())

