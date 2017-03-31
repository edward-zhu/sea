#!/usr/bin/env python
# encoding: utf-8

import re
import sys
import xml.etree.ElementTree as ET

class Parser:
    def parse(self, input_file):
        return [], []

class MediaWikiParser(Parser):
    TAG_PAGE = "{http://www.mediawiki.org/xml/export-0.10/}page"
    TAG_TITLE = "{http://www.mediawiki.org/xml/export-0.10/}title"
    TAG_REVISION = "{http://www.mediawiki.org/xml/export-0.10/}revision"
    TAG_TEXT = "{http://www.mediawiki.org/xml/export-0.10/}text"
    WIKI_URL_PREFIX = "https://en.wikipedia.org/wiki/"

    def parseGenerator(self, input_file):
        tree = ET.parse(input_file)
        docid = 0

        trim_re = re.compile(r"[ ][ ]|<ref.*?/>|<code>.*?</code>|<ref.*?>.*?</ref>|{{.*?}}|{.*?}|\[.*?\]|---+|[<>*{}\\\[\]/|=']", flags=re.DOTALL)

        for page in tree.findall(MediaWikiParser.TAG_PAGE):
            title = page.find(MediaWikiParser.TAG_TITLE).text
            url = MediaWikiParser.WIKI_URL_PREFIX + title.replace(" ", "_")
            text = page.find(MediaWikiParser.TAG_REVISION).find(MediaWikiParser.TAG_TEXT).text

            try:
                text = trim_re.sub("", text)
            except TypeError as e:
                print("parsing error: ", text, e)
                continue

            metadata = {
                "doc_id" : docid,
                "title" : title,
                "url" : url,
            }
            yield text, metadata
            docid += 1

    def parse(self, input_file):
        docs = []
        metadata = []
        tree = ET.parse(input_file)
        docid = 0

        trim_re = re.compile(r"|[ ][ ]|<ref.*?/>|<code>.*?</code>|<ref.*?>.*?</ref>|\[.*?\]|---+|[<>*{}\[\]|=']", flags=re.DOTALL)

        for page in tree.findall(MediaWikiParser.TAG_PAGE):
            title = page.find(MediaWikiParser.TAG_TITLE).text
            url = MediaWikiParser.WIKI_URL_PREFIX + title.replace(" ", "_")
            text = page.find(MediaWikiParser.TAG_REVISION).find(MediaWikiParser.TAG_TEXT).text
            try:
                text = trim_re.sub("", text)
            except TypeError as e:
                print("parsing error: ", text, e)
            # print("parsing: %s." % title);
            metadata.append({
                "doc_id" : docid,
                "title" : title,
                "url" : url,
            })
            docs.append(text)
            docid += 1

        print(u"parse finish! total doc: %d."% len(docs))

        return docs, metadata
