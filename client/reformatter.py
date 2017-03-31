'''
reformatter.py

retrive file from enwiki, then output preprocessed and shard files.
'''

import bz2
import os
import json
import traceback

from concurrent.futures import ProcessPoolExecutor
from indexer.bz2parse import WikipediaParser
from urllib.request import urlopen

from tornado.ioloop import IOLoop
from tornado.gen import coroutine

from itertools import chain

def parse(url, n_part, job_path):
    parser = WikipediaParser()
    ud = urlopen(url)
    bd = bz2.open(ud)
    ret = parser.parse(bd)
    ud.close()
    bd.close()

    files = []
    for i in range(0, n_part):
        fn = gen_output_filename(job_path, i)
        print("output file #%d:%s" % (i, fn))
        files.append(open(fn, "w"))

    count = 0
    for doc, md in chain(*ret):
        md["doc_id"] = count
        pid = md["doc_id"] % n_part
        files[pid].write(json.dumps({
            "metadata" : md,
            "doc" : doc.replace("\n\n", "").lower(),
        }) + '\n')
        count += 1

    for f in files:
        f.close()

def gen_output_filename(job_path, i):
    return os.path.join(job_path, "reformatted_%d.in" % i)

class Reformatter:
    executor = ProcessPoolExecutor()

    def __init__(self, input_urls, job_path, n_part):
        self.input_urls = input_urls
        self.job_path = job_path
        self.n_part = n_part

    def _gen_parse_futures(self):
        return [Reformatter.executor.submit(parse, url,
                                            self.n_part, self.job_path) for url in self.input_urls]

    @coroutine
    def run(self):
        try:
            yield self._gen_parse_futures()
        except Exception:
            return False, traceback.format_exc()
        return True, ""


URLS = [
    "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles1.xml-p000000010p000030302.bz2",
    "https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles2.xml-p000030304p000088444.bz2",
]

@coroutine
def main():
    '''main func'''
    r = Reformatter(URLS, "data/invindex_jobs", 4)
    ok, err = yield r.run()

    IOLoop.instance().stop()

if __name__ == "__main__":

    ioloop = IOLoop.instance()
    ioloop.add_callback(main)
    ioloop.start()
