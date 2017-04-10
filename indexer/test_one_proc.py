import json
import pickle
import math

from sklearn.feature_extraction.text import TfidfVectorizer
from tornado.ioloop import IOLoop

from search.utils.tokenizer import StemTokenizer
from mapreduce.coordinator import Coordinator
from indexer.dist_tfidf import DistTFIDFVectorizer

INV_MAPPER_PATH = "mr_apps/invindex_mapper.py"
INV_REDUCER_PATH = "mr_apps/invindex_reducer.py"
INV_JOB_PATH = "invindex_jobs"

IDF_MAPPER_PATH = "mr_apps/idf_mapper.py"
IDF_REDUCER_PATH = "mr_apps/idf_reducer.py"
IDF_JOB_PATH = "idf_jobs"

def get_inv_coordinator():
    return Coordinator(INV_MAPPER_PATH, INV_REDUCER_PATH, INV_JOB_PATH, 5)

def get_idf_coordinator():
    return Coordinator(IDF_MAPPER_PATH, IDF_REDUCER_PATH, IDF_JOB_PATH, 5)

def get_docs(files_):
    docs = []
    for fn in files_:
        with open(fn, "r") as f:
            docs.extend([json.loads(line)["doc"] for line in f])

    return docs

def get_idf(vec, term):
    tid = vec.vocabulary_[term]
    return vec.idf_[tid]

def test(vec, dist_vec, term):
    idf = get_idf(vec, term)
    idf2 = dist_vec.get_idf(term)
    print(term, idf, idf2, math.fabs(idf-idf2))

if __name__ == "__main__":
    c_inv = get_inv_coordinator()
    files = c_inv._get_input_files()
    docs = get_docs(files)
    vec = TfidfVectorizer(use_idf=True, tokenizer=StemTokenizer(), max_df=0.998, min_df=0.002, smooth_idf=True, lowercase=False)
    vec.fit(docs)

    dist_vec = DistTFIDFVectorizer("idf_jobs/0.out")
    test(vec, dist_vec, "person")
