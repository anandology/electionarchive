import couchdb
import simplejson
import sys

def load(path):
    db = couchdb.Database("http://127.0.0.1:5984/electionarchive")

    doc = simplejson.loads(open(path).read())
    olddoc = db.get(doc['_id']) or {}
    if "_rev" in olddoc:
        doc['_rev'] = olddoc['_rev']
    db.save(doc)

if __name__ == "__main__":
    load(sys.argv[1])
