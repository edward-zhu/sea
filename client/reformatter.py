'''
reformatter.py

retrive file from enwiki, then output preprocessed and shard files.
'''

import bz2
import os
import json
import traceback

import bz2

from concurrent.futures import ProcessPoolExecutor
from indexer.bz2parse import WikipediaParser
from urllib.request import urlopen

from tornado.ioloop import IOLoop
from tornado.gen import coroutine

from functools import reduce
from itertools import chain

from multiprocessing import Queue
from multiprocessing import Manager

class ReformatterError(Exception):
    pass

def _parse(url, qs):
    parser = WikipediaParser()

    count = 0
    with bz2.open(urlopen(url)) as fd:
        for doc, md in parser.iterparse(fd):
            qs[count % len(qs)].put((doc, md))
            count += 1

def parse(url, qs):
    '''
    parse one url to (doc, md) array
    '''
    ret = True
    try:
        _parse(url, qs)
    except Exception as e:
        raise ReformatterError("Exception in parse phase %s" % (str(e),))
    finally:
        print('clean parse process...')
        for q in qs:
            q.put(-1)

    return ret

def _split(q, n_parser, n_part, job_path, files, compress=False):
    if not os.path.exists(job_path):
        os.mkdir(job_path)

    for i in range(0, n_part):
        fn = gen_output_filename(job_path, i, compress)
        print("output file #%d:%s" % (i, fn))
        if compress:
            f = bz2.open(fn, "wt")
        else:
            f = open(fn, "w")
        files.append(f)

    finished = 0
    count = 0
    while finished < n_parser:
        msg = q.get()
        if msg == -1:
            finished += 1
            continue
        doc, md = msg

        try:
            doc = WikipediaParser.preprocess(doc)
            doc = doc.replace("\n\n", "")
        except Exception as e:
            print('illegal doc. error:', e)
            continue

        md["doc_id"] = count
        pid = md["doc_id"]% n_part
        text = json.dumps({
            "metadata" : md,
            "doc" : doc,
        }) + '\n'

        files[pid].write(text)
        count += 1

def split(q, n_parser, n_part, job_path, compress=False):
    '''
    split group data into N parts
    '''
    ret = True
    files = []

    try:
        _split(q, n_parser, n_part, job_path, files, compress)
    except Exception as e:
        raise ReformatterError("Exception in split phase %s" % (str(e),))
    finally:
        map(lambda f: f.close(), files) # close all files

    return ret

def gen_output_filename(job_path, i, compress=False):
    if compress:
        return os.path.join(job_path, "reformatted_%d.in.bz2" % i)

    return os.path.join(job_path, "reformatted_%d.in" % i)


class Reformatter:
    executor = ProcessPoolExecutor()

    def __init__(self, input_urls, job_path, n_part, n_group, start_gid, compress=True):
        self.input_urls = input_urls
        self.job_path = job_path
        self.n_part = n_part
        self.n_group = n_group
        self.start_gid = start_gid
        self.manager = Manager()
        self.queues = [self.manager.Queue() for _ in range(0, n_group)]
        self.compress = compress

    def _gen_split_futures(self):
        return [Reformatter
                .executor
                .submit(split,
                        self.queues[i],
                        len(self.input_urls),
                        self.n_part,
                        os.path.join(self.job_path, str(self.start_gid + i)), self.compress)
                for i in range(0, self.n_group)]
    def _gen_parse_futures(self):
        return [Reformatter.executor.submit(parse, url, self.queues) for url in self.input_urls]

    @coroutine
    def run(self):
        try:
            rets = yield [self._gen_parse_futures(), self._gen_split_futures()]
            ret = reduce(lambda x, y: x and y, chain(*rets), True)
        except ReformatterError as e:
            print("reformat failed: %s" % str(e))
            return False, str(e)
        except Exception as e:
            print("reformat failed: %s" % str(e))
            return False, str(e)
        return ret, ""


URLS = [
    "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles1.xml-p10p30302.bz2",
]

@coroutine
def main():
    '''main func'''
    r = Reformatter(URLS, "data/invindex_jobs", 2, 3, 0)
    ok, err = yield r.run()
    print(ok)
    IOLoop.instance().stop()

if __name__ == "__main__":
    ioloop = IOLoop.instance()
    ioloop.add_callback(main)
    ioloop.start()
