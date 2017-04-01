import re
import json

from lxml import etree

class WikipediaParser:
    STANDBY = 0
    TITLE = 1
    TEXT = 2

    WIKI_URL_PREFIX = "https://en.wikipedia.org/wiki/"
    WIKI_NAMESPACE = "{http://www.mediawiki.org/xml/export-0.10/}"

    trim_re = re.compile(r"http.*?\s|<ref.*?/>|&lt;ref.*?ref&gt;|&lt;math.*?math&gt;|\\t|<code>.*?</code>|"+
                                  r"<ref.*?>.*?</ref>|{{.*?}}|{.*?}|--+|[!+<>*{}#\\/|=']|"+
                                  r"\[.*?\]|\]",
                                  flags=re.DOTALL|re.MULTILINE)

    @staticmethod
    def preprocess(str):
        return WikipediaParser.trim_re.sub("", str)

    def iterparse(self, f):
        for _, elem in etree.iterparse(f, tag=WikipediaParser.WIKI_NAMESPACE + 'page'):
            title = elem.findtext(WikipediaParser.WIKI_NAMESPACE + 'title')
            rev = elem.find(WikipediaParser.WIKI_NAMESPACE + 'revision')
            doc = rev.findtext(WikipediaParser.WIKI_NAMESPACE + 'text')
            metadata = {
                "title": title,
                "url": WikipediaParser.WIKI_URL_PREFIX + title.replace(" ", "_") 
            }
            yield doc, metadata
            elem.clear()

if __name__ == '__main__':
    import bz2
    f = bz2.open(open("data/info_ret.xml.bz2", "rb"))
    wp = WikipediaParser()
    for doc, md in wp.iterparse(f):
        print(md)
    f.close()
    
