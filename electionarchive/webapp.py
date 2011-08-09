"""Web interface to election archive.
"""

import web
import os
import simplejson

urls = (
    "/", "index",
    "/([^/]*)", "election",
    "/([^/]*)/C(\d+)", "constituency",
)
app = web.application(urls, globals())
render = web.template.render(os.path.join(os.path.dirname(__file__), "templates"))

def first(seq):
    seq = iter(seq)
    try:
        return seq.next()
    except StopIteration:
        return None
        
def storify(value):
    if isinstance(value, list):
        return [storify(v) for v in value]
    elif isinstance(value, dict):
        return web.storage((k, storify(v)) for k, v in value.items())
    else:
        return value

class Election:
    def __init__(self, data):
        self.data = storify(data)
        
    @property
    def name(self):
        return self.data.get("name") or self.id
        
    @property
    def id(self):
        return self.data.get("_id")
        
    @property
    def constituencies(self):
        return self.data['constituencies']
        
    def get_constituency(self, cid):
        return first(c for c in self.constituencies if c.id == cid)
    
    @staticmethod
    def list():
        """Returns ids of all available elections.
        """
        return [os.path.splitext(f)[0] for f in os.listdir("db") if f.endswith(".json")]
        
    @staticmethod
    def get(id):
        """Returns the election object with given id.
        """
        json = open("db/%s.json" %id).read()
        data = simplejson.loads(json)
        return data and Election(data)
    
class index:
    def GET(self):
        elections = Election.list()
        return render.site(render.index(elections))
        
class election:
    def GET(self, eid):
        election = Election.get(eid)
        return render.site(render.election(election))
        
class constituency:
    def GET(self, eid, cid):
        election = Election.get(eid)
        constituency = election.get_constituency(cid)
        return render.site(render.constituency(eid, constituency))
        
if __name__ == "__main__":
    app.run()