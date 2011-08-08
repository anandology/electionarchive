"""Crawler for 2011 Assembly Elections in West Bengal.
"""

import logging
import re
import urllib

from base import BaseCrawler, disk_memoize

logger = logging.getLogger("crawler")


class Crawler(BaseCrawler):
    url = "http://www.ceo.kerala.gov.in/affidavit.html"

    @disk_memoize("data.json")
    def get_data(self):
        """Returns the data in data.json format.
        """
        d = {
            "state": "West Bengal",
            "election_type": "assembly",
            "year": "2011",
            "url": "http://www.ceowb.in/districtlistaffidavits.aspx",
            "constituencies": []
        }
        
        for dist in self.get_districts():
            for c in self.get_constituencies(dist['id']):
                c['candidates'] = self.get_candidates(c['id'])
                c['district'] = dist['name']
                c['district_id'] = dist['id']
                d['constituencies'].append(c)
        return d
        
    def download_all(self):
        for c in self.get_all_candidates():
            logger.info("downloading affidavits of %s (%s)" % (c['name'], c['id']))
            self.download_affidavits(c['affidavit_id'])
        
    def get_all_candidates(self):
        d = self.get_data()
        for cons in d['constituencies']:
            for c in cons['candidates']:
                yield c
        
    def get_links(self):
        return [{
            "title": "Winning Canidates",
            "url": "http://ceowestbengal.nic.in/mis_pdf/election_2011/WinningCandidatesList_2011.pdf"
        }, {
            "title": "Result - Assembly Election 2011",
            "url": "http://ceowestbengal.nic.in/mis_pdf/election_2011/cand_tot_vote_2011.pdf"
        }, {
            "title": "Voters' Turnout 2011",
            "url": "http://ceowestbengal.nic.in/mis_pdf/election_2011/vt_2011.pdf"
        }]
                
    def get_all_districts(self):    
        for d in self.get_districts():
            consistuencies = self.get_district_info(d['id'])
            for c in consistuencies:
                candidates = self.get_candidates(c['id'])
        
    @disk_memoize("data/districts.json")
    def get_districts(self):
        url = "http://www.ceowb.in/districtlistaffidavits.aspx"
        soup = self.get_soup(url)
        
        def parse(a):
            name = a.text.strip()
            id = re.sub(r".*DCID=(\d*)", r"\1", a['href'])
            return {
                "id": id,
                "name": name,
                "url": a['href']
            }
        # read all district links
        return [parse(a) for a in soup.findAll("a") if a['href'].startswith("ACLISTAffidavits.aspx")]
        
    @disk_memoize("data/district_%(dist_id)s.json")
    def get_constituencies(self, dist_id):
        url = "http://www.ceowb.in/ACLISTAffidavits.aspx?DCID=" + str(dist_id)
        soup = self.get_soup(url)
        
        def parse(a):
            id, name = a.text.strip().split("-", 1)
            return {
                "id": id,
                "name": name,
                "url": a['href']
            }
        # read all district links
        return [parse(a) for a in soup.findAll("a") if a['href'].startswith("CandidateAffidavitsForAc.aspx")]
        
    @disk_memoize('data/consituency_%(acid)s.json')
    def get_candidates(self, acid):
        url = "http://www.ceowb.in/CandidateAffidavitsForAc.aspx?ACID=" + str(acid)
        soup = self.get_soup(url)
        table = soup.find("table", {"id": "ctl00_ContentPlaceHolder1_GridView1"})
        
        def parse(tr):
            cells = tr.findAll("td")
            download_url = urllib.basejoin(url, cells[4].find("a")['href'])
            affidavit_id = download_url.split("=")[1] # split ?AffidavitsID=1234 
            
            return {
                "id": cells[0].text.strip(),
                "name": cells[1].text.strip(),
                "gender": cells[2].text.strip(),
                "party": cells[3].text.strip(),
                "download_url": download_url,
                "affidavit_id": affidavit_id,
                "affidavits": [
                    {
                        "filename": "files/%s-CR.pdf" % affidavit_id,
                        "name": "Criminal Records"
                    },
                    {
                        "filename": "files/%s-SC.pdf" % affidavit_id,
                        "name": "Assets"
                    }
                ]
                
            }
    
        rows = table.findAll("tr")[1:] # skip header
        return [parse(tr) for tr in rows]
    
    def download_affidavits(self, AffidavitsID):
        url = "http://www.ceowb.in/ViewCandidateAffidavits.aspx?AffidavitsID=" + str(AffidavitsID)
        soup = self.get_soup(url)
        inputs = soup.findAll("input", {"type": "hidden"})
        formdata = dict((i['name'], i['value']) for i in inputs)
        
        post_links = [a for a in soup.findAll("a") if a['href'].startswith("javascript:__doPostBack")]
        
        assert post_links[0]['href'] == "javascript:__doPostBack('ctl00$ContentPlaceHolder1$dlsCr$ctl00$lnkbtnDownload','')"
        assert post_links[1]['href'] == "javascript:__doPostBack('ctl00$ContentPlaceHolder1$dlsSC$ctl00$lnkbtnDownload','')"
        assert len(post_links) == 2
        
        self._download_affidavit(AffidavitsID, "CR", formdata, "ctl00$ContentPlaceHolder1$dlsCr$ctl00$lnkbtnDownload")
        self._download_affidavit(AffidavitsID, "SC", formdata, "ctl00$ContentPlaceHolder1$dlsSC$ctl00$lnkbtnDownload")
        
    @disk_memoize("files/%(AffidavitsID)s-%(suffix)s.pdf")
    def _download_affidavit(self, AffidavitsID, suffix, formdata, target):
        url = "http://www.ceowb.in/ViewCandidateAffidavits.aspx?AffidavitsID=" + str(AffidavitsID)
        data = dict(formdata)
        data['__EVENTTARGET'] = target
        return self.post(url, data)

def main():
    FORMAT = "%(asctime)s [%(name)s] [%(levelname)s] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.INFO)

    crawler = Crawler("data/AE-2011-WB")
    # crawler.get_candidates(1)    
    #print crawler.download_affidavits(76)
    crawler.download_all()
        
if __name__ == '__main__':
    main()