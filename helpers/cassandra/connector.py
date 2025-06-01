from cassandra.cluster import Cluster

def connect_cassandra(hosts, keyspace):
    cluster = Cluster(hosts)
    session = cluster.connect(keyspace)
    return session
