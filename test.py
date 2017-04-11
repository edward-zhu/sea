import mapreduce.worker

mapreduce.worker.runMapper("mapreduce/wordcount/mapper.py", "mapreduce/pg_jobs/pg-ulysses.in.bz2", 4)

