import re
import json

from xml.parsers.expat import ParserCreate

class WikipediaParser:
    STANDBY = 0
    TITLE = 1
    TEXT = 2

    WIKI_URL_PREFIX = "https://en.wikipedia.org/wiki/"

    def __init__(self):
        self.p = ParserCreate()
        self.p.StartElementHandler = self.start_element
        self.p.EndElementHandler = self.end_element
        self.p.CharacterDataHandler = self.char_data
        self.state = WikipediaParser.STANDBY
        self.cur_title = ""
        self.cur_text = ""
        self.cur_docid = 0
        self.result = []
        self.trim_re = re.compile(r"http.*?\s|<ref.*?/>|&lt;ref.*?ref&gt;|&lt;math.*?math&gt;|\\t|<code>.*?</code>|"+
                                  r"<ref.*?>.*?</ref>|{{.*?}}|{.*?}|--+|[!+<>*{}#\\/|=']|"+
                                  r"\[.*?\]|\]",
                                  flags=re.DOTALL|re.MULTILINE)

    def preprocess(self, str):
        return self.trim_re.sub("", str)

    def start_element(self, name, attrs):
        # print(name)
        if name == 'title':
            self.state = WikipediaParser.TITLE
        elif name == 'text':
            self.state = WikipediaParser.TEXT

    def end_element(self, name):
        if name != 'text':
            return
        try:
            data = self.cur_text
            self.cur_text = ""
            data= self.preprocess(data).replace(" \n", "").replace("\n\n", "")
            # print(data)
        except TypeError as e:
            print("parsing error: ", data, e)
            self.state = WikipediaParser.STANDBY
            return
        url = WikipediaParser.WIKI_URL_PREFIX + self.cur_title.replace(" ", "_")
        metadata = {
            "title" : self.cur_title,
            "url" : url,
        }
        self.result.append((data, metadata))
        self.cur_docid += 1
        self.state = WikipediaParser.STANDBY

    def char_data(self, data):
        if self.state == WikipediaParser.TITLE:
            self.cur_title = repr(data)
            self.state = WikipediaParser.STANDBY
        elif self.state == WikipediaParser.TEXT:
            d = repr(data).strip()
            if len(d) > 20:
                self.cur_text += d + '\n'


    def parse(self, f):
        self.p.ParseFile(f)
        return self.result

if __name__ == '__main__':
    f = open("info_ret.xml", "rb")
    wp = WikipediaParser()
    ret = wp.parse(f)
    f.close()

    with open("out.json", "w") as f:
        json.dump(ret, f)
