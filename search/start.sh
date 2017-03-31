#!/bin/bash

python ./index_app.py 0 &
python ./index_app.py 1 &
python ./index_app.py 2 &
python ./index_app.py 3 &
python ./index_app.py 4 &
python ./index_app.py 5 &
python ./index_app.py 6 &
python ./index_app.py 7 &
python ./doc_app.py 0 &
python ./doc_app.py 1 &
python ./doc_app.py 2 &
python ./doc_app.py 3 &
python ./doc_app.py 4 &
python ./doc_app.py 5 &
python ./doc_app.py 6 &
python ./doc_app.py 7 &
python ./frontend_app.py
killall python

