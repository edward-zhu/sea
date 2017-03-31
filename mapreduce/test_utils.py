#!/usr/bin/env python
# encoding: utf-8

from unittest import TestCase
from mapreduce import utils

class TestGetUrl(TestCase):
    def test_get_url(self):
        base = utils.worker_url(0)
        print(utils.gen_req_url(base, "map", test=3, ids=["2", "3", "4"]))
