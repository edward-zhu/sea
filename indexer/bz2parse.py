'''
bz2parse.py

A bz2 support Wikipedia xml parser
'''

import re
import json

from lxml import etree

from indexer.WikiExtractor import Extractor

class WikipediaParser:
    STANDBY = 0
    TITLE = 1
    TEXT = 2

    WIKI_URL_PREFIX = "https://en.wikipedia.org/wiki/"
    WIKI_NAMESPACE = "{http://www.mediawiki.org/xml/export-0.10/}"

    EXTRACTOR = Extractor(1, 1, "", "")

    REF_EXP1 = re.compile(r"<ref.*?/>|==.*?==(=)?")
    REF_EXP2 = re.compile(r"<ref.*?>.*?</ref>")

    @staticmethod
    def preprocess(text):
        '''preprocess string'''
        text = WikipediaParser.EXTRACTOR.transform(text)
        text = WikipediaParser.EXTRACTOR.wiki2text(text)
        text = WikipediaParser.REF_EXP1.sub("", text)
        text = WikipediaParser.REF_EXP2.sub("", text)
        return text

    def iterparse(self, fn):
        '''parse iteratively'''
        for _, elem in etree.iterparse(fn, tag=WikipediaParser.WIKI_NAMESPACE + 'page'):
            title = elem.findtext(WikipediaParser.WIKI_NAMESPACE + 'title')
            rev = elem.find(WikipediaParser.WIKI_NAMESPACE + 'revision')
            doc = rev.findtext(WikipediaParser.WIKI_NAMESPACE + 'text')
            if "Wikipedia:" in title:
                continue
            if "Template:" in title:
                continue
            
            metadata = {
                "title": title,
                "url": WikipediaParser.WIKI_URL_PREFIX + title.replace(" ", "_")
            }
            yield doc, metadata
            elem.clear()

if __name__ == '__main__':
    import bz2
    fd = bz2.open(open("data/info_ret.xml.bz2", "rb"))
    wp = WikipediaParser()
    for _, md in wp.iterparse(fd):
        print(md)
    fd.close()

