"""Crawler of 2011 Assembly Elections in Kerala.
"""
import urllib
import urllib2
import BeautifulSoup
import logging
import simplejson
import os

from base import BaseCrawler, disk_memoize

logger = logging.getLogger("kerala")

class Crawler(BaseCrawler):
    url = "http://www.ceo.kerala.gov.in/affidavit.html"
    
    @disk_memoize("data.json")
    def get_data(self):
        """Returns the data in data.json format.
        """
        d = {
            "_id": "AE-2011-KL",
            "state": "Kerala",
            "election_type": "assembly",
            "year": "2011",
            "url": "http://www.ceo.kerala.gov.in/affidavit.html",
            "constituencies": []
        }
        
        for dist in self.get_districts():
            for c in dist['constituencies']:
                c['candidates'] = self.get_candidates(c['id'], dist['id'])
                c['district'] = dist['name']
                c['district_id'] = dist['id']
                d['constituencies'].append(c)
        return d
        
    def download_files(self):
        for filename, url in self.get_files_to_download():
            self.download(url, path=filename)
    
    def get_files_to_download(self):
        """Returns an iterator over (filename, url) for all downloadable urls.
        """
        d = self.get_data()
        for cons in d['constituencies']:
            for c in cons['candidates']:
                for a in c['affidavits']:
                    yield c['filename'], c['url']
    
    @disk_memoize("data/districts.json")
    def get_districts(self):
        url = "http://www.ceo.kerala.gov.in/districtlacs.html"
        soup = self.get_soup(url)
        
        rows = soup.find("div", "content inner-width").find("table").findAll("tr")
        rows = rows[1:] # skip the header
        
        districts = []
        for row in rows:
            cells  = row.findAll("td")
            number = cells[0].text
            
            a = cells[1].find("a")
            name = a.text
            constituencies = self.get_constituencies(number, a['href'])
            districts.append({
                "id": number,
                "name": name, 
                "constituencies": constituencies
            })
        return districts
        
    @disk_memoize("data/district_%(dist_id)s.json")
    def get_constituencies(self, dist_id, district_url):
        #url = "http://www.ceo.kerala.gov.in/%s.html" % district.lower()
        soup = self.get_soup(district_url)
        
        rows = soup.find("div", {"class": "content inner-width"}).find("table").findAll("tr")
        
        # first row is header
        # second row is counts
        counts = rows[1].findAll("td")
        # third entry is the count of number of consistuencies
        count = int(counts[2].text)
        
        # skip first 2 rows
        rows = rows[2:]
        
        def parse_constituency(name):
            num, name = name.split(None, 1)
            return {"id": num, "name": name}
            
        # constituency name is the second last
        values = [row.findAll("td")[-2].text for row in rows[:count]]
        return [parse_constituency(v) for v in values]
        
    @disk_memoize("data/candidates_%(cid)s.json")
    def get_candidates(self, cid, dist_id):
        """Returns the candidates info from a constituency.
        """
        logger.info("get_candidates %s", cid)
        url = "http://www.ceo.kerala.gov.in/affidavit/partsListAjax.html"
        json = self.get(url, {"distNo": dist_id, 'lacNo': cid})
        # convert to UTF-8 and ignore errors
        # One particular page was failing with unicode code. This conversion takes care of that.
        json = json.decode('utf-8', 'ignore')
        d = simplejson.loads(json)
        
        def parse_candidate(c):
            a = BeautifulSoup.BeautifulSoup(c[3]).find("a")
            return {
                "id": c[0],
                "name": c[1],
                "party": c[2],
                "affidavits": [{
                    "url": a['href'],
                    "filename": "files/" + a['href'][len("http://"):].replace(":80/", "/"),
                    "name": "affidavit"
                }]
            }
        return [parse_candidate(c) for c in d['aaData']]
                
def main():
    FORMAT = "%(asctime)s [%(name)s] [%(levelname)s] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.INFO)
    
    crawler = Crawler("data/AE-2011-KL")
    crawler.get_data()
    crawler.download_files()
        
if __name__ == '__main__':
    main()