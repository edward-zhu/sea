from indexer.bz2parse import WikipediaParser
from indexer.WikiExtractor import Extractor

import argparse
import pickle
import os
import json
import re

import bz2

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", nargs=1, help="input file.")
    parser.add_argument("--job_path", required=True, help="df job's path")
    parser.add_argument("--num_partitions", type=int, required=True, help="df job's path")

    return parser.parse_args()

def show_args(args):
    print("Reformatter")
    print("input_file:", args.input_file)
    print("job_path:", args.job_path)
    print("num_partitions:", args.num_partitions)

def gen_output_filename(job_path, i):
    return os.path.join(job_path, "reformatted_%d.in" % i)

if __name__ == "__main__":
    args = parse_args()
    show_args(args)
    input_file = args.input_file[0]

    parser = WikipediaParser()

    files = []
    for i in range(0, args.num_partitions):
        fn = gen_output_filename(args.job_path, i)
        print("output file #%d:%s" % (i, fn))
        files.append(open(fn, "w"))

    extractor = Extractor(1, 1, "", "")

    ref_exp1 = re.compile(r"<ref.*?/>")
    ref_exp2 = re.compile(r"<ref.*?>.*?</ref>")
    
    def extract(text):
        text = extractor.transform(text)
        text = extractor.wiki2text(text)
        text = ref_exp1.sub("", text)
        text = ref_exp2.sub("", text)
        return text

    with bz2.open(input_file) as fd:
        docid = 0
        for doc, md in parser.iterparse(fd):
            try:
                md["doc_id"] = docid
                pid = md["doc_id"] % args.num_partitions
                text = json.dumps({
                    "metadata" : md,
                    "doc" : extract(doc),
                }) + '\n'
            except Exception as e:
                print(str(e))
                continue
            docid += 1
            files[pid].write(text)
    
    for f in files:
        f.close()




