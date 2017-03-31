"""
integrate.py

Generate assignment2 compatible binary invindex and
document presentation from mapreduce jobs' output.

"""

import os
import pickle
import argparse
import multiprocessing
import leveldb
import base64
import json

from assignment4.dist_tfidf import DistTFIDFVectorizer

def toByte(doc):
    return json.dumps({
        "metadata" : doc["metadata"],
        "sents" : doc["sents"],
        "sents_rep" : str(base64.urlsafe_b64encode(pickle.dumps(doc["sents_rep"])))
    }).encode('utf-8')

def gen(doc_out, invindex_out, output_path, doc_prefix, invindex_prefix, pid):
    """generate binary invindex and doc_rep for one parition"""
    doc_data = None
    with open(doc_out, 'rb') as doc_f:
        doc_data = pickle.load(doc_f)
    inv_f = open(invindex_out, 'r')

    inv_iter = map(lambda x: x.strip().split('\t'), filter(lambda x: x[0] != '#', inv_f))
    invindex = {}
    for term, docs in inv_iter:
        invindex[term] = [int(x.split(':')[0]) for x in docs.split(',')]
    inv_f.close()
    doc_path = os.path.join(output_path, "%s_%d.pkl" % (doc_prefix, pid))
    invidx_path = os.path.join(output_path, "%s_%d.pkl" % (invindex_prefix, pid))
    with open(invidx_path, 'wb') as f:
        pickle.dump({
            "doc_rep" : doc_data["doc_reps"],
            "doc_invidx" : invindex,
            "id2repid" : doc_data["id2repid"]
        }, f)

    with open(doc_path, 'wb') as f:
        pickle.dump(doc_data["docs"], f)

    tfidf_path = os.path.join(output_path, "tfidf.pkl")
    idf_file = "assignment4/idf_jobs/0.out"

    with open(tfidf_path, 'wb') as f:
        vec = DistTFIDFVectorizer(idf_file)
        pickle.dump(vec, f)

def parse_args():
    """parse arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("--doc_result",
                        help="docs job result file pattern.",
                        default="assignment4/docs_jobs/%d.out")
    parser.add_argument("--inv_result",
                        help="invindex job result file pattern.",
                        default="assignment4/idf_jobs/%d.in")
    parser.add_argument("--out_path",
                        help="output path", default="assignment4/output/")
    parser.add_argument("--doc_prefix",
                        help="docs output prefix", default="docs")
    parser.add_argument("--inv_prefix",
                        help="invindex output prefix", default="indexes")
    parser.add_argument("nparts", type=int, help="partition number")

    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()

    procs = []
    for i in range(0, args.nparts):
        procs.append(multiprocessing.Process(
            target=gen,
            args=(args.doc_result % i, args.inv_result % i,
                  args.out_path, args.doc_prefix, args.inv_prefix, i)))
        procs[i].start()
    for p in procs:
        p.join()
