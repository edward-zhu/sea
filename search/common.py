import etcd

from search.manifest import ETCD_CLUSTER

def get_etcd_cli():
    '''get a new etcd client'''
    if ETCD_CLUSTER is None:
        return etcd.Client()

    clis = tuple(map(lambda x: (x[0], int(x[1])),
                map(lambda x: x.split(":"), ETCD_CLUSTER.split(","))))

    print(list(clis))

    if len(clis) > 1:
        return etcd.Client(host=clis, allow_reconnect=True)
    else:
        cli = clis[0]
        return etcd.Client(host=cli[0], port=cli[1])
