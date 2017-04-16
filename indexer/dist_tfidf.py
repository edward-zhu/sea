import pickle

from scipy.sparse import spdiags
from sklearn.feature_extraction.text import TfidfVectorizer

from search.utils.tokenizer import StemTokenizer

class DistTFIDFVectorizer(TfidfVectorizer):
    """customed TFIDF Vectorizer"""
    def __init__(self, fn):
        super().__init__(tokenizer=StemTokenizer(), norm=None)
        try:
            data = self._get_distrubuted_idf(fn)
            TfidfVectorizer.idf_ = data["idf"]
            self.vocabulary_ = data["dic"]
        except Exception as e:
            raise RuntimeError("invalid distributed vectorizer output: %s" % str(e))
        self._tfidf._idf_diag = spdiags(self.idf_, diags=0, m=len(self.idf_), n=len(self.idf_))

    def _get_distrubuted_idf(self, fname):
        """load idf data from distributed tf-idf output"""
        dis_idf = None
        with open(fname, "rb") as f:
            dis_idf = pickle.load(f)
        return dis_idf

    def get_idf(self, term):
        """get idf of given term"""
        tid = self.vocabulary_[term]
        return self.idf_[tid]

if __name__ == '__main__':
    vec = DistTFIDFVectorizer('idf_jobs/0.out')
    print(vec.transform(["personalized test"]))