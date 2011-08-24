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
            "_id": "AE-2011-WB",
            "state": "West Bengal",
            "election_type": "assembly",
            "year": "2011",
            "url": "http://www.ceowb.in/districtlistaffidavits.aspx",
            "constituencies": list(self.get_all_constituencies())
        }
        return d
        
    def download_all(self):
        for c in self.get_all_candidates():
            logger.info("downloading affidavits of %s (%s/%s)" % (c['name'], c['constituency_id'], c['id']))
            self.download_affidavits(c['affidavit_id'])
            
        self.download_links()
        self.download_expenditures()
            
    def get_all_constituencies(self):
        for dist in self.get_districts():
            for c in self.get_constituencies(dist['id']):
                c['candidates'] = self.get_candidates(c['id'])
                c['district'] = dist['name']
                c['district_id'] = dist['id']
                yield c
        
    def get_all_candidates(self):
        d = self.get_data()
        for cons in d['constituencies']:
            for c in cons['candidates']:
                c['constituency_id'] = cons['id']
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
        for link in post_links:
            suffix = self.get_suffix_from_jslink(link['href'])
            target = link['href'].split("'")[1]
            try:
                self._download_affidavit(AffidavitsID, suffix, formdata, target)
            except Exception:
                logger.error("failed to download files/%s-%s.pdf", AffidavitsID, suffix, exc_info=True)
            
    def get_suffix_from_jslink(self, link, suffix_map={}):
        """Returns file suffix from href.
        
        same hrefs:
        
        javascript:__doPostBack('ctl00$ContentPlaceHolder1$dlsCr$ctl00$lnkbtnDownload','')
        javascript:__doPostBack('ctl00$ContentPlaceHolder1$dlsSC$ctl00$lnkbtnDownload','')
        javascript:__doPostBack('ctl00$ContentPlaceHolder1$dlsSC$ctl01$lnkbtnDownload','')
        
        suffixs for the above:
        CR
        SC
        SC1
        
        When called with suffix_map {"CR": "X", "SC": "Y"}:
        X
        Y
        Y1
        """
        match = re.search(r"ctl00\$ContentPlaceHolder1\$dls(..)\$ctl(\d\d)\$lnkbtnDownload", link)
        suffix, count = match.groups()
        suffix = suffix.upper()
        suffix = suffix_map.get(suffix, suffix)
        
        if count == "00":
            return suffix
        else:
            return suffix + "-" + count
        
    @disk_memoize("files/%(AffidavitsID)s-%(suffix)s.pdf")
    def _download_affidavit(self, AffidavitsID, suffix, formdata, target):
        url = "http://www.ceowb.in/ViewCandidateAffidavits.aspx?AffidavitsID=" + str(AffidavitsID)
        data = dict(formdata)
        data['__EVENTTARGET'] = target
        return self.post(url, data)
        
    def download_links(self):
        for link in self.get_links():
            self.download(link['url'], path=link['filename'])
        
    @disk_memoize("data/links.json")
    def get_links(self):
        urls = [
            "http://ceowestbengal.nic.in/",
            "http://ceowestbengal.nic.in/FirstHyperLink.asp?m_menu_id=142"
        ]
        
        links = [x for url in urls for x in self._get_links(url)]
        return [{"title": title, "filename": filename, "url": url} 
                for title, filename, url in links
                if url.endswith(".pdf") or url.endswith(".jpg")]
        
    def _get_links(self, url):
        soup = self.get_soup(url)
        links = soup.findAll("a")
        
        for a in links:
            href = a.get('href', '')
            if href.startswith("javascript:show_window"):
                filename = re.sub(r"javascript:show_window\('./(mis_pdf/.*)'\)", r'\1', href)
                url = "http://ceowestbengal.nic.in/" + filename
                title = self.get_text(a)
                yield title, filename, url
            elif href.startswith("mis_pdf/election_2011/"):
                title = self.get_text(a)
                filename = href
                url = "http://ceowestbengal.nic.in/" + href
                yield title, filename, url
                
    def download_expenditures(self):
        for cons in self.get_expenditure_monitoring():
            for c in cons:
                print "c", c
                self._download_expenditures_for_ac(c['download_id'])
        
    def _download_expenditures_for_ac(self, id):
        url = "http://www.ceowb.in/ViewExpenditureMonitoring.aspx?ID=" + str(id)
        soup = self.get_soup(url)
        inputs = soup.findAll("input", {"type": "hidden"})
        formdata = dict((i['name'], i['value']) for i in inputs)
        
        post_links = [a for a in soup.findAll("a") if a['href'].startswith("javascript:__doPostBack")]

        for link in post_links:
            suffix = self.get_suffix_from_jslink(link['href'], suffix_map={"CR": "abstract", "SC": "affidavit"})
            target = link['href'].split("'")[1]
            try:
                self._download_expenditure(id, suffix, formdata, target)
            except IOError:
                logger.error("Downloading expediture failed", exc_info=True)
            
    @disk_memoize("files/expediture/%(id)s-%(suffix)s.pdf")
    def _download_expenditure(self, id, suffix, formdata, target):
        url = "http://www.ceowb.in/ViewExpenditureMonitoring.aspx?ID=" + str(id)
        data = dict(formdata)
        data['__EVENTTARGET'] = target
        return self.post(url, data)            
                
    def get_expenditure_monitoring(self):
        return [self.get_expenditure_for_ac(c['id']) for c in list(self.get_all_constituencies())]
        
    @disk_memoize("data/expenditure/%(acid)s.json")
    def get_expenditure_for_ac(self, acid):
        url = "http://ceowb.in/ExpenditureMonitoringForAc.aspx?ACID=" + str(acid)
        soup = self.get_soup(url)
        table = soup.find("table", {"id": "ctl00_ContentPlaceHolder1_GridView1"})
        def parse(tr):
            cells = tr.findAll("td")
            download_url = urllib.basejoin(url, cells[4].find("a")['href'])
            download_id = download_url.split("=")[1] # split ?ID=2
            
            return {
                "id": cells[0].text.strip(),
                "name": cells[1].text.strip(),
                "gender": cells[2].text.strip(),
                "party": cells[3].text.strip(),
                "download_url": download_url,
                "download_id": download_id,
                "affidavits": [
                    {
                        "filename": "files/expenditure/%s-abstract.pdf" % download_id,
                        "name": "Abstract Statement"
                    },
                    {
                        "filename": "files/expenditure/%s-affidavit.pdf" % download_id,
                        "name": "Affidavit on Accounts"
                    }
                ]
            }
    
        rows = table.findAll("tr")[1:] # skip header
        return [parse(tr) for tr in rows]

def main():
    FORMAT = "%(asctime)s [%(name)s] [%(levelname)s] %(message)s"
    logging.basicConfig(format=FORMAT, level=logging.INFO)

    crawler = Crawler("data/AE-2011-WB")
    # crawler.get_candidates(1)    
    #print crawler.download_affidavits(76)
    crawler.get_data()
    crawler.download_all()
    
class TestCrawler:
    def setup_method(self, method):
        self.crawler = Crawler("data/AE-2011-WB")
        
    def test_get_suffix_from_jslink(self):
        f = self.crawler.get_suffix_from_jslink
    
        assert f("javascript:__doPostBack('ctl00$ContentPlaceHolder1$dlsCr$ctl00$lnkbtnDownload','')") == "CR"
        assert f("javascript:__doPostBack('ctl00$ContentPlaceHolder1$dlsSC$ctl00$lnkbtnDownload','')") == "SC"
        assert f("javascript:__doPostBack('ctl00$ContentPlaceHolder1$dlsSC$ctl01$lnkbtnDownload','')") == "SC-01"
        assert f("javascript:__doPostBack('ctl00$ContentPlaceHolder1$dlsSC$ctl01$lnkbtnDownload','')", {"SC": "Y"}) == "Y-01"

if __name__ == '__main__':
    main()