"""Base Crawler
"""
import os
import re
import urllib
import urllib2
import simplejson
import logging
import functools
import inspect
import htmlentitydefs

from BeautifulSoup import BeautifulSoup
from httpcache import CacheHandler, ThrottlingProcessor

logger = logging.getLogger("base")

def to_kwargs(f, *args, **kwargs):
    """Takes arguments given to a function and converts all of them into keyword arguments by looking at the function signature.
    
    >>> def f(a, b=2): pass
    ...
    >>> to_kwargs(f, 1)
    {'a': 1, 'b': 2}
    >>> to_kwargs(f, 1, 3)
    {'a': 1, 'b': 3}
    >>> to_kwargs(f, b=3, a=1)
    {'a': 1, 'b': 3}
    """

    s = inspect.getargspec(f)
    defaults = s.defaults or []
    default_args = s.args[-len(defaults):]

    kw = {}
    kw.update(zip(default_args, defaults))
    kw.update(kwargs)
    kw.update(zip(s.args, args))
    return kw

def disk_memoize(path):
    """Returns a decorator to cache the function return value in the specified file path.

    If the path ends with .json, the return value is encoded into JSON before saving and decoded on read.
    
    String formatting opeator can be used in the path, the actual path is constructed by formatting the specified path using the argument values.
    
    Usage:
    
        @disk_memoize("data/c_%(number)s")
        def get_consititency(self, number):
            ...
    """
    def decorator(f):
        @functools.wraps(f)
        def g(self, *a, **kw):
            kwargs = to_kwargs(f, self, *a, **kw)
            filepath = os.path.join(self.root, path % kwargs)
            disk = Disk()
            content = disk.read(filepath)
            if content:
                return content
            else:
                content = f(self, *a, **kw)
                disk.write(filepath, content)
                return content
        return g
    return decorator

class Disk:
    """Simple wrapper to read and write files in various formats.
    
    This takes care of coverting the data to and from the required format. The default format is text.

    Other supported formats are:
        * json
    """
    def write(self, path, content):
        if path.endswith(".json"):
            content = simplejson.dumps(content, indent=4)
            
        dirname = os.path.dirname(path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        
        logger.info("saving %s", path)
        with open(path, 'w') as f:
            f.write(content)
    
    def read(self, path):
        if os.path.exists(path):
            logger.info("reading %s", path)
            content = open(path).read()
            if path.endswith(".json"):
                return simplejson.loads(content)
            else:
                return content
            
class BaseCrawler:
    """Base Crawler with useful utilities. 
    
    All the crawlers are extended from this class.
    """
    def __init__(self, root):
        """Creates the crawler.
        
        The root directory is used to keep all the files created by this crawler.
        """
        self.root = root
        
        self.cache_dir = os.path.join(root, "cache")
        self.files_dir = os.path.join(root, "files")
        self.data_dir = os.path.join(root, "data")
        
        self.makedirs(self.cache_dir)
        self.makedirs(self.files_dir)
        self.makedirs(self.data_dir)
        
        self.opener = urllib2.build_opener(
            CacheHandler(self.cache_dir), 
            ThrottlingProcessor(2))

        self.nocache_opener = urllib2.build_opener(
            ThrottlingProcessor(2))
            
        self.post_opener = urllib2.build_opener(
            ThrottlingProcessor(5))
            
    def makedirs(self, path):
        if not os.path.exists(path):
            os.makedirs(path)

    def get(self, url, params=None, _cache=True):
        if params:
            url += "?" + urllib.urlencode(params)
            
        if _cache:
            opener = self.opener
        else:
            opener = self.nocache_opener
        
        logger.info("GET %s", url)
        return opener.open(url).read()
        
    def post(self, url, params):
        if isinstance(params, dict):
            params = urllib.urlencode(params)

        logger.info("POST %s", url)
        return self.post_opener.open(url, params).read()
        
    def get_soup(self, url):
        html = self.get(url)
        return BeautifulSoup(html)
        
    def save(self, path, content):
        path = os.path.join(self.root, path)
        with open(path, "w") as f:
            f.write(content)
    
    def save_json(self, path, data):
        self.save(path, simplejson.dumps(data, indent=4))
    
    def download(self, url, method="GET", data=None, path=None):
        path = path or url.split("/")[-1]
        self._download(url, method=method, data=data, path=path)
        
    @disk_memoize("files/%(path)s")
    def _download(self, url, method, data, path):
        if method.upper() == "GET":
            return self.get(url, data, _cache=False)
        else:
            return self.post(url, data)
            
    def get_text(self, e=None):
        """Returns content of BeautifulSoup element as text."""
        return ''.join([c for c in e.recursiveChildGenerator() if isinstance(c, unicode)]).strip()
        
    def unescape(self, text):
        """Removes HTML or XML character references and entities from a text string.
        
        from Fredrik Lundh
        http://effbot.org/zone/re-sub.htm#unescape-html
        
        @param text The HTML (or XML) source text.
        @return The plain text, as a Unicode string, if necessary.
        """
        def fixup(m):
            text = m.group(0)
            if text[:2] == "&#":
                # character reference
                try:
                    if text[:3] == "&#x":
                        return unichr(int(text[3:-1], 16))
                    else:
                        return unichr(int(text[2:-1]))
                except ValueError:
                    pass
            else:
                # named entity
                try:
                    text = unichr(htmlentitydefs.name2codepoint[text[1:-1]])
                except KeyError:
                    pass
            return text # leave as is
        return re.sub("&#?\w+;", fixup, text)
        
    def trim(self, s):
        """Replaces multiple spaces with single space and remove whitespace at both the ends.
        """
        return re.sub("\s+", " ", s).strip()
    
    def parse_link(self, a, base_url=None):
        """Parses the link node of BeautifulSoup and returns title and url.
        
        If base_url is provided, the url is converted to absolute url with base_url as base.
        """
        title = self.unescape(self.get_text(a)).strip()
        url = a['href']
        if base_url:
            url = urllib.basejoin(base_url, url)
        return title, url