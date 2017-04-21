#!/bin/bash

python -m search.index_app 0 &
python -m search.index_app 1 &
python -m search.index_app 2 &
python -m search.index_app 3 &
python -m search.index_app 4 &
python -m search.index_app 5 &
python -m search.index_app 6 &
python -m search.index_app 7 &
python -m search.doc_app 0 &
python -m search.doc_app 1 &
python -m search.doc_app 2 &
python -m search.doc_app 3 &
python -m search.doc_app 4 &
python -m search.doc_app 5 &
python -m search.doc_app 6 &
python -m search.doc_app 7 &
python -m search.frontend_app
killall python

