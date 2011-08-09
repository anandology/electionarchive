"""Crawler of 2011 Assembly Elections in Puducherry.
"""
import logging
import re
import urllib

from base import BaseCrawler, disk_memoize

logger = logging.getLogger("crawl")

class Crawler(BaseCrawler):
    url = "http://ceopondicherry.nic.in/AFFIDAVITS2011/puducherry.asp"
    RE_CONSTITUENCY_NAME = re.compile(r"^(\d+) *[\.-] *([A-Za-z\. ]*) *(?:\(([A-Z]*)\))?$")
    
    @disk_memoize("data.json")
    def get_data(self):
        """Returns the data in data.json format.
        """
        return {
            "_id": "AE-2011-PY",
            "state": "Puducherry",
            "election_type": "assembly",
            "year": "2011",
            "url": self.url,
            "constituencies": self.get_constituencies()
        }
        
    def get_constituencies(self):
        soup = self.get_soup(self.url)
        sections = soup.findAll("p", dict(align="center", style="text-align:center"))
        
        items = [self._parse_consituency(p) for p in sections 
                if p.parent.name == "div" 
                and p.parent['class'] == "Section1" 
                and p.get("class") is None]
        return [item for item in items if item is not None]
            
    def _parse_consituency(self, p):
        print "_parse_consituency", p
        name = self.get_text(p).replace("\r", "").replace("\n", "").replace("&ndash;", "-")
        match = self.RE_CONSTITUENCY_NAME.match(name)
        
        if not match:
            return None
        
        id, name, category = match.groups()
        category = category[1:-1] # strip parenthesis
        
        rows = p.findNext("table").findAll("tr")
        rows = rows[2:] # skip headers
        return {
            "id": id,
            "name": name,
            "category": category,
            "candidates": [self._parse_candidate(tr) for tr in rows]
        }
    
    def _parse_candidate(self, tr):
        cells = tr.findAll("td")        
        cell4_url = urllib.basejoin(self.url, cells[4].find("a")['href'])
        cell5_url = urllib.basejoin(self.url, cells[5].find("a")['href'])
        
        return {
            "id": cells[0].text.strip(),
            "name": cells[1].text.strip(),
            "party": cells[2].text.strip(),
            "gender": cells[3].text.strip(),
            "affidavits": [{
                "name": "Assets & Liabilities",
                "url": cell4_url,
                "filename": "files/" + cell4_url[len("http://"):]
            }, {
                "name": "Criminal Records",
                "url": cell5_url,
                "filename": "files/" + cell5_url[len("http://"):]
            }]
        }
        
    @disk_memoize("data/results.json")
    def get_results(self):
        url = "http://ceopondicherry.nic.in/form20/form20.asp"
        soup = self.get_soup(url)
        
        def parse(a):
            name, href = self.parse_link(a, base_url=url)
            id, name, category = self.split_name(name)
            filename = "files/" + href[len("http://"):]
            return {
                "id": id,
                "name": name,
                "category": category,
                "url": href,
                "filename": filename
            }
        return [parse(a) for a in soup.findAll("a") if a.get("href") and a["href"].endswith(".pdf") and "/" not in a["href"]]
        
    @disk_memoize("data/expenditure.json")
    def get_expenditures(self):
        url = "http://ceopondicherry.nic.in/expenditure/2011/expend.asp"
        soup = self.get_soup(url)
        table = soup.findAll("table")[-1] # requird data is in the last table
        rows = table.findAll("tr")
        rows = rows[1:] # skip header
        
        def _parse_link(a):
            title, href = self.parse_link(a, base_url=url)
            return {
                "name": title,
                "url": href,
                "filename": "files" + href[len("http://"):]
            }
        
        def parse(tr):
            cells = tr.findAll("td")
            id, name, category = self.split_name(cells[0].text)
            
            winner = cells[1].find("a")
            runner1 = cells[2].find("a")
            runner2 = cells[3].find("a")
            
            return {
                "id": id,
                "name": name,
                "category": category,
                "winner": winner and _parse_link(winner),
                "runner1": winner and _parse_link(runner1),
                "runner2": winner and _parse_link(runner2),
            }
                    
        return [parse(tr) for tr in rows]
        
    def split_name(self, name):
        name = self.trim(name)
        match = self.RE_CONSTITUENCY_NAME.match(name)
        return tuple(s and s.strip() for s in match.groups())
        
    def get_downloadables(self):
        for cons in self.get_data()['constituencies']:
            for c in cons['candidates']:
                for a in c['affidavits']:
                    yield a['filename'], a['url']

        for x in self.get_results():
            yield x['filename'], x['url']

        for cons in self.get_expenditures():
            for key in ['winner', 'runner1', 'runner2']:
                if key in cons:
                    yield cons[key]['filename'], cons[key]['url']

    def download_all(self):
        for filename, url in self.get_downloadables():
            # download adds files/ to the filename. removing here to balance it.
            filename = filename[len("files/"):]
            self.download(url, path=filename)

def main():
    FORMAT = "%(asctime)s [%(name)s] [%(levelname)s] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    
    crawler = Crawler("data/AE-2011-PY")
    crawler.download_all()
    
class TestCrawler:
    """py.test test
    """
    def setup_method(self, method):
        self.crawler = Crawler("data/AE-2011-PY")
        
    def test_split_name(self):
        f = self.crawler.split_name
        assert f("01. MANNADIPET") == ("01", "MANNADIPET", None)
        assert f("03. OUSSUDU (SC)") == ("03", "OUSSUDU", "SC")
        assert f("01- MANNADIPET (GEN) ") == ("01", "MANNADIPET", "GEN")

if __name__ == '__main__':
    main()