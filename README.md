# Misaki

## Deployment Guide on CIMS's servers

In this section, we will introduce how to deploy Misaki search engine (indexer with job management system and queryer) on CIMS's server. In this case, we use 1% Wikipedia data and produce 2 data groups each has 2 shards.

### Requirements

- 2+ server (1 master, 1+ workers)
- Python 3.6+
- cross-server available data folder (CIMS `/data` directory or GCS with `gcs-fuse`)

### Note

**Different CIMS Server may have different python version. One may need to configure different virtualenv on different server.**

### Clone
    git clone git@github.com:edward-zhu/sea.git

### Install Requirements
    pip install -r requirements.txt

### Indexer

#### Job Tracker

    export JOB_TRACKER_PORT=23233
    cd /path/to/sea
    export PYTHONPATH=$(pwd)
    python -m master.job_tracker

#### Create Data Folders

Here shows 2 groups case.

    $mkdir -p /path/to/data/invindex_out/0
    $mkdir -p /path/to/data/invindex_out/1
    $mkdir -p /path/to/data/idf_out/0
    $mkdir -p /path/to/data/idf_out/1
    $mkdir -p /path/to/data/docs_out/0
    $mkdir -p /path/to/data/docs_out/1
    $mkdir -p /path/to/data/output/0
    $mkdir -p /path/to/data/output/1

#### Task Tracker (for each worker node)

    cd /path/to/sea
    export PYTHONPATH=$(pwd)
    export TASK_TRACKER_PORT=30001
    export JOB_TRACKER="http://[job_tracker]:23233"
    export IDF_FILE=/path/to/data/idf_out/0.out
    python -m mapreduce.workers &    
    python -m client.task_tracker

#### Check Deployment

    $ curl http://[job_tracker]:23233/status
    {
        "workers": [
        {
        "host": "http://172.22.80.150:30001",
        "state": 0,
        "last_hbt": 1493739581.0538707,
        "owner_jid": "None",
        "owner_tid": "None"
        },
        {
        "host": "http://172.22.80.145:30001",
        "state": 0,
        "last_hbt": 1493739580.8972585,
        "owner_jid": "None",
        "owner_tid": "None"
        }
        ],
        "current": "None",
        "jobs": []
    }%

#### Create Job

Please run the following task one-by-one.

Reformat task

    $ curl http://[job_tracker]:23233/new?spec=/path/to/enwiki-mini.yaml
    {"status": "ok"}%

Inverted Index task

    $ curl http://[job_tracker]:23233/new?spec=/path/to/invindex-mini.yaml
    {"status": "ok"}%

In-Group IDF task

    $ curl http://[job_tracker]:23233/new?spec=/path/to/idf-mini.yaml
    {"status": "ok"}%

Global IDF task

    $ curl http://[job_tracker]:23233/new?spec=/path/to/accu_idf-mini.yaml
    {"status": "ok"}%
    $ bzip2 -d /path/to/data/idf_out/0.out.bz2

**Need to decompress IDF before next step.**

Document task

    $ curl http://[job_tracker]:23233/new?spec=/path/to/docs-mini.yaml
    {"status": "ok"}%

Integrate task

    $ curl http://[job_tracker]:23233/new?spec=/path/to/integrate-mini.yaml
    {"status": "ok"}%

Final result:

    (venv) jz2653@linax2[jobs]$ tree /data/jz2653/mini-serval/
    ├── 0
    │   ├── reformatted_0.in.bz2
    │   └── reformatted_1.in.bz2
    ├── 1
    │   ├── reformatted_0.in.bz2
    │   └── reformatted_1.in.bz2
    ├── docs_out
    │   ├── 0
    │   │   ├── 0.err
    │   │   ├── 0.out.bz2
    │   │   ├── 1.err
    │   │   └── 1.out.bz2
    │   └── 1
    │       ├── 0.err
    │       ├── 0.out.bz2
    │       ├── 1.err
    │       └── 1.out.bz2
    ├── idf_out
    │   ├── 0
    │   │   ├── 0.err
    │   │   └── 0.out.bz2
    │   ├── 0.err
    │   ├── 0.out
    │   └── 1
    │       ├── 0.err
    │       └── 0.out.bz2
    ├── invindex_out
    │   ├── 0
    │   │   ├── 0.err
    │   │   ├── 0.out.bz2
    │   │   ├── 1.err
    │   │   └── 1.out.bz2
    │   └── 1
    │       ├── 0.err
    │       ├── 0.out.bz2
    │       ├── 1.err
    │       └── 1.out.bz2
    └── output
        ├── 0
        │   ├── docs_0.pkl.bz2
        │   ├── docs_1.pkl.bz2
        │   ├── indexes_0.pkl.bz2
        │   ├── indexes_1.pkl.bz2
        │   └── tfidf.pkl
        └── 1
            ├── docs_0.pkl.bz2
            ├── docs_1.pkl.bz2
            ├── indexes_0.pkl.bz2
            ├── indexes_1.pkl.bz2
            └── tfidf.pkl

    14 directories, 36 files

### Queryer

#### `etcd` service

First, download `etcd` source code and compile.

    $ git clone https://github.com/coreos/etcd.git
    $ cd etcd
    $ ./build
    $ ./bin/etcd

> from `etcd` github page.

Start `etcd` service

    $ export EXTERNAL_IP=[this host's external IP]
    $ /path/to/bin/etcd \
        --advertise-client-urls=http://$(hostname):32379 \
        --listen-client-urls=http://127.0.0.1:2379,http://$EXTERNAL_IP:32379

#### Master

    $ cd /path/to/sea
    $ etcdctl --endpoints http://linserv2.cims.nyu.edu:32379 set /misaki/n_srv 2
    $ export ETCD_CLUSTER=[etcd_host]:32379
    $ export MASTER_PORT=11112
    $ python -m search.master

#### Host

change shard size to 2:

    $ vim /path/to/sea/search/manifest.py

modify line 7 and 8 from:

    N_INDEX_SRV = 8
    N_DOC_SRV = 8

to:

    N_INDEX_SRV = 2
    N_DOC_SRV = 2

start host:

    $ export ETCD_CLUSTER=[etcd_host]:32379
    $ export DATA_BASE=/path/to/data/output
    $ python -m search.start

open browser:

    $ firefox http://[master host]:11112/
    




    


