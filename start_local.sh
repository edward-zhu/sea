#!/bin/bash

killall python

NPART=8

echo "start mapreduce workers..."
python -m assignment3.worker 20030 &
python -m assignment3.worker 20031 &
python -m assignment3.worker 20032 &
python -m assignment3.worker 20033 &
python -m assignment3.worker 20034 &
python -m assignment3.worker 20035 &
python -m assignment3.worker 20036 &
python -m assignment3.worker 20037 &

sleep 2

rm -f assignment4/invindex_jobs/*.out
rm -f assignment4/idf_jobs/*.out assignment4/idf_jobs/*.in
rm -f assignment4/docs_jobs/*.out assignment4/docs_jobs/*.in

SECONDS=0

#echo "reformatting raw data..."
#python -m assignment4.reformatter assignment2/data/enwiki_10.xml \
#    --job_path="assignment4/invindex_jobs" --num_partitions=$NPART

echo "reformatting finished second elapsed: $SECONDS"

SECONDS=0

echo "start generating inverted index..."
python -m assignment3.coordinator \
    --mapper_path=assignment4/mr_apps/invindex_mapper.py \
    --reducer_path=assignment4/mr_apps/invindex_reducer.py \
    --job_path=assignment4/invindex_jobs \
    --num_reducers=$NPART

echo "move result to idf job..."

mv assignment4/invindex_jobs/*.out assignment4/idf_jobs
find ./assignment4/idf_jobs -name "*.out" -exec sh -c 'a={};mv $a ${a%%.out}.in' \;

echo "start generating idf..."
python -m assignment3.coordinator \
    --mapper_path=assignment4/mr_apps/idf_mapper.py \
    --reducer_path=assignment4/mr_apps/idf_reducer.py \
    --job_path=assignment4/idf_jobs \
    --num_reducers=1

echo "move reformatted docs to docs job..."
mv assignment4/invindex_jobs/*.in assignment4/docs_jobs

echo "start generating docs..."
python -m assignment3.coordinator \
    --mapper_path=assignment4/mr_apps/docs_mapper.py \
    --reducer_path=assignment4/mr_apps/docs_reducer.py \
    --job_path=assignment4/docs_jobs \
    --num_reducers=$NPART

echo "start generating compatible files..."
python -m assignment4.integrate $NPART

echo "genertaion finished second elapsed: $SECONDS"

echo "put reformatted docs back..."
mv assignment4/docs_jobs/*.in assignment4/invindex_jobs

killall python
exit

