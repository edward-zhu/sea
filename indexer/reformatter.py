from assignment2.parser import MediaWikiParser

import argparse
import pickle
import os
import json

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_file", nargs=1, help="input file.")
    parser.add_argument("--job_path", required=True, help="df job's path")
    parser.add_argument("--num_partitions", type=int, required=True, help="df job's path")

    return parser.parse_args()

def show_args(args):
    print("Reformatter")
    print("input_file:", args.input_file)
    print("job_path:", args.job_path)
    print("num_partitions:", args.num_partitions)

def gen_output_filename(job_path, i):
    return os.path.join(job_path, "reformatted_%d.in" % i)

if __name__ == "__main__":
    args = parse_args()
    show_args(args)
    input_file = args.input_file[0]

    parser = MediaWikiParser()

    files = []
    for i in range(0, args.num_partitions):
        fn = gen_output_filename(args.job_path, i)
        print("output file #%d:%s" % (i, fn))
        files.append(open(fn, "w"))


    for doc, md in parser.parseGenerator(input_file):
        pid = md["doc_id"] % args.num_partitions
        files[pid].write(json.dumps({
            "metadata" : md,
            "doc" : doc.replace("\n\n", "").lower(),
        }) + '\n')
    
    for f in files:
        f.close()




