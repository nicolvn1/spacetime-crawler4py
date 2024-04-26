from threading import Thread

from inspect import getsource
from utils.download import download, download_header
from utils import get_logger
import scraper
import time
from urllib.parse import urlparse, urljoin
import urllib.robotparser

from bs4 import BeautifulSoup
import re
import requests

class Worker(Thread):
    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        stopwords = ["a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"]
        unique_pages = set()
        max_len = 0
        max_url = ""
        freq = {}
        ics_subdomains = {}
        ics_subdomains_formatted = []
        
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            # check robot.txt
            can_crawl, delay = self.checkRobotTxt(tbd_url)
            if not can_crawl:
                print("ROBOT NOT CRAWLABLE")
                self.frontier.mark_url_complete(tbd_url)
                time.sleep(self.config.time_delay)
                continue
            
            # REQUEST HEAD FOR STATUS CODE, REDIRECT, AND FILE SIZE
            head = download_header(tbd_url, self.config, self.logger)
            if not str(head).startswith("<Response [2") or not str(head).startswith("<Response [3"):
                print("HEAD IS NOT STATUS 200 or 300")
                self.frontier.mark_url_complete(tbd_url)
                time.sleep(self.config.time_delay)
                continue
            
            # find redirect as long as returned is different
            redirect = self.headerRedirect(tbd_url, head)
            while redirect != tbd_url:
                head = download_header(tbd_url, self.config, self.logger)
                redirect = self.headerRedirect(tbd_url, head)
                print("REDIRECTING FROM HEAD")
            # check if reported length is too big, if so, skip file
            if "Content-Length" in head.headers and int(head.headers["Content-Length"]) > 1048576:
                print("SKIP SIZE FROM HEAD")
                self.frontier.mark_url_complete(tbd_url)
                time.sleep(self.config.time_delay)
                continue
            resp = download(tbd_url, self.config, self.logger)
            # Check if status is 200
            if resp.status != 200:
                continue
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            if delay > self.config.time_delay:
                print(f"COOLDOWN: {self.config.time_delay - delay}")
                time.sleep(delay - self.config.time_delay)
            #if completed, add to unique pages set. otherwise, add to pages to be downloaded.
            unique_pages.add(tbd_url.split("#")[0])

            size = 1048576 #initalized file size as 1 mb so file is not crawled in case status is not 200
            # headers = download_header(tbd_url, self.config, self.logger).headers ALREADY DOWNLOADED
            # Check if the status is 200
            if resp.status == 200:
                #detect and avoid crawling if file size is above threshold
                if resp.raw_response is not None:
                    # Hello I added a thing more ealier that does the same thing cuz I needed to use HEAD
                    # Maybe we can move this?
                    if 'Content-Length' in head.headers:
                        size = int(head.headers['Content-Length'])
                    #if content length header not present, download max size and check if size is greater than threshold.
                    #if not, proceed. if so, break so file is not crawled.
                    else:
                        response = requests.get(tbd_url, stream = True)
                        size = 0
                        with open('tempTestFile', 'wb') as f:
                            for chunk in response.iter_content(chunk_size=1024):
                                if chunk:
                                    f.write(chunk)
                                    size += len(chunk)
                                    if size > 1048576:
                                        break
                                        
                # Get the content of the url
                soup = BeautifulSoup(resp.raw_response.content, 'html.parser', from_encoding = "iso-8859-1")
                # check if html allows crawling
                if self.checkNoIndex(soup):
                    self.frontier.mark_url_complete(tbd_url)
                    time.sleep(self.config.time_delay)
                    print("NOINDEX,NOFOLLOW")
                    continue
                # after downlading, if we need to redirect, add redirect to frontier and move on
                pos_redirect = self.checkRedirect(soup)
                if pos_redirect is not None:
                    print("ADDING REDIRECT TO FRONTIER")
                    self.frontier.add_url(pos_redirect)
                    self.frontier.mark_url_complete(tbd_url)
                    time.sleep(self.config.time_delay)
                    continue
                # checking if canonical
                canonical = self.checkCanonical(soup)
                if canonical.startswith("https"):
                    comp_can = canonical[5:]
                else:
                    comp_can = canonical[4:]
                if tbd_url.startswith("https"):
                    comp_url = tbd_url[5:]
                else:
                    comp_url = tbd_url[4:]
                if canonical is not None and comp_can.strip("/") != comp_url.strip("/"):
                    self.frontier.mark_url_complete(tbd_url)
                    time.sleep(self.config.time_delay)
                    print(f"NON-CANONICAL: {canonical} VS {tbd_url}")
                    continue
                    
                # does not crawl if website is titled page not found 
                title = soup.find("title")
                title = title.text.lower() if title else ""
                print(f"TITLE IS {title}")
                if "page not found" in title or "404" in title or "403" in title:
                    self.frontier.mark_url_complete(tbd_url)
                    time.sleep(self.config.time_delay)
                    continue
                # Remove the script and style elements of the page
                for s in soup(["script", "style"]):
                    s.extract()

                # Create a list of words composed of alphanumeric characters and apostrophes
                words = [word for word in re.split("[^a-zA-Z0-9']", soup.get_text()) if word != ""]
                # Find the page that has the most number of words
                if max_len < len(words):
                    max_len = len(words)
                    max_url = tbd_url

                # Create a dictionary with a word as a key and its frequency as the value
                for k in words:
                    k = k.lower()
                    if len(k) > 1 and k not in stopwords:
                        if k not in freq:
                            freq[k] = 1
                        else:
                            freq[k] += 1
                #if file size is below 1mb and has > 100 distinct words, crawl. otherwise, avoid.
                if size < 1048576 and len(freq.keys()) > 100:
                    scraped_urls = scraper.scraper(tbd_url, resp)
                    for scraped_url in scraped_urls:
                        self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)

        # Sort the items in the dictionary of frequencies by descending order
        result = sorted(freq.items(), key=lambda x:(-x[1],x[0]))
        
        # Check the subdomains
        for link in unique_pages:
            if ".ics.uci.edu" in link:
                subdomain = link.split(".ics.uci.edu")[0] + ".ics.uci.edu"
                if subdomain not in ics_subdomains:
                    ics_subdomains[subdomain] = 1
                else:
                    ics_subdomains[subdomain] += 1

        for subdomain in sorted(ics_subdomains.keys()):
            ics_subdomains_formatted.append(f"{subdomain}, {ics_subdomains[subdomain]}")
            
        self.logger.info(
            f"{len(unique_pages)} total unique urls discovered. "
            f"The longest page {max_url} has {max_len} words. "
            f"Top 50 most common words: {result[0:50]}"
            f"All ics.uci.edu subdomains: {ics_subdomains_formatted}")

    def checkRobotTxt(self, url):
        # check robot.txt to see if web is crawlable
        # returns tuple of (Bool, delay-time)
        rp = urllib.robotparser.RobotFileParser()
        link = urlparse(url)
        # invalid link
        if not link.scheme or not link.netloc:
            return (False, 0)
        robot_link = link.scheme + "://" + link.netloc
        robot_link = urljoin(robot_link, "robot.txt")
        rp.set_url(robot_link)
        try:
            rp.read()
            delay = rp.crawl_delay('*') 
            delay = delay if delay else 0
            return (rp.can_fetch('*', url), delay)
        except:
            return (False, 0)
    
    def checkNoIndex(self, soup):
        noIndex = soup.find("meta", content="noindex")
        noFollow = soup.find("meta", content="nofollow")
        noNeither = soup.find("meta", content="noindex,nofollow")
        if noIndex or noFollow or noNeither:
            return True
    
    def checkRedirect(self, soup):
        # FOR META TAG ONLY
        refreshes = soup.find("meta", http_equiv="refresh")
        redirect = refreshes["content"] if refreshes else None
        if redirect is not None and ";" in redirect:
            redir_url = redirect.split(";")[-1]
            return redir_url
        return None
    
    def headerRedirect(self, url, head):
        # request header only to check FOR REDIRECT
        # check if location exists
        if "Location" in head.headers:
            redir = head.headers["Location"]
            # if relative
            if urlparse(redir).netloc == "": 
                return urljoin(url, redir)
            else:
                return redir
        # check if refresh exists
        elif "Refresh" in head.headers:
            redir = head.header["Refresh"]
            # if relative
            if urlparse(redir).netloc == "": 
                return urljoin(url, redir)
            else:
                return redir
        else:
            return url   

    def checkCanonical(self, soup):
        # check for canonical web, which is a duplicate of current
        # first check link rel
        rel = soup.find("link", rel="canonical")
        canonical = rel["href"] if rel else None
        if canonical:
            return canonical
        # second check og url
        og_url = soup.find("meta", property="og:url")
        canonical = og_url["content"] if og_url else None
        return canonical
