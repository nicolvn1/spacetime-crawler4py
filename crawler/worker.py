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
        self.counter = 0
        self.stopwords = ["a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"]
        self.unique_pages = set()
        self.max_len = 0
        self.max_url = ""
        self.freq = {}
        self.ics_subdomains = {}
        self.ics_subdomains_formatted = []
        # basic check for requests in scraper
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
        
    def run(self):
        # runs the thing
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break

            # Header checking
            header = download_header(tbd_url)
            if header:
                # check header for redirect
                header_redirect = self.headerRedirect(tbd_url, header)
                if not self.checkSameUrl(tbd_url, header_redirect):
                    if not self.checkDiscovered(header_redirect):
                        self.frontier.add_url(header_redirect)
                    self.frontier.mark_url_complete(tbd_url)
                    time.sleep(self.config.time_delay)
                    continue
                # check header to see if the file is too big to download > 1mb
                if not self.checkLengthHeader(header):
                    self.frontier.mark_url_complete(tbd_url)
                    time.sleep(self.config.time_delay)
                    continue
            # download the web
            resp = download(tbd_url, self.config, self.logger)
            # just in case the download fails
            if resp is None or resp.raw_response is None:
                self.frontier.mark_url_complete(tbd_url)
                time.sleep(self.config.time_delay)
                continue
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            # to print out result every 100 downloads
            self.count += 1
            if self.count == 100:
                self.count = 0
                self.finalResults()
            # Check if status is 2xx
            if resp.status // 100 != 2:
                self.frontier.mark_url_complete(tbd_url)
                time.sleep(self.config.time_delay)
                continue
            # try to soup it
            try:
                soup = BeautifulSoup(resp.raw_response.content, 'html.parser', from_encoding = "iso-8859-1")
            except Exception as e:
                self.frontier.mark_url_complete(tbd_url)
                time.sleep(self.config.time_delay)
                continue
            # check crawlable
            if not self.checkCrawlable(soup):
                self.frontier.mark_url_complete(tbd_url)
                time.sleep(self.config.time_delay)
                continue
            # check redirects 
            redirect = self.checkRedirect(soup)
            if redirect and not self.checkSameUrl(redirect, tbd_url):
                if not self.checkDiscovered(redirect):
                    self.frontier.add_url(redirect)
                self.frontier.mark_url_complete(tbd_url)
                time.sleep(self.config.time_delay)
                continue
            # add to self data and calculate word count
            word_count = self.cakcData(resp)
            # if less than 100 word, let's not scrap other links from it
            if word_count < 100:
                self.frontier.mark_url_complete(tbd_url)
                time.sleep(self.config.time_delay)
                continue

            """
            # COMMENT REASON: header length already checked
            # ANOTHER THING: code is checking size by downloading the web AGAIN -> potential impoliteness for downloading again too soon
            size = 1048576 #initalized file size as 1 mb so file is not crawled in case status is not 200
            # headers = download_header(tbd_url, self.config, self.logger).headers ALREADY DOWNLOADED
            #detect and avoid crawling if file size is above threshold
            if resp.raw_response is not None:
                # temp head download here
                head = download_header(tbd_url, self.config, self.logger)
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
                        if size > 1048576:
                            self.frontier.mark_url_complete(tbd_url)
                            time.sleep(self.config.time_delay)
                            continue
            """      

            # does not crawl if website is titled page not found 
            title = soup.find("title")
            title = title.text.lower() if title else ""
            if "page not found" in title or "404" in title or "403" in title:
                self.frontier.mark_url_complete(tbd_url)
                time.sleep(self.config.time_delay)
                continue

            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)


    def calcData(self, resp):
        # calculates the word count of content
        unique_pages.add(resp.url)
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
        # Remove the script and style elements of the page
        for s in soup(["script", "style"]):
            s.extract()
        # Create a list of words composed of alphanumeric characters and apostrophes
        words = [word for word in re.split("[^a-zA-Z0-9-']", soup.get_text()) if word != ""]
        # check if this is the longest page so far
        if len(words) > self.max_len:
            self.max_len = len(words)
            self.max_url = resp.url
        # Add frequency of words to class dict of words
        for k in words:
            k = k.lower()
            if len(k) > 1 and k not in self.stopwords:
                if k not in self.freq:
                    self.freq[k] = 1
                else:
                    self.freq[k] += 1
        # Check the subdomains
        if ".ics.uci.edu" in resp.url:
            subdomain = resp.url.split(".ics.uci.edu")[0] + ".ics.uci.edu"
            if subdomain not in self.ics_subdomains:
                self.ics_subdomains[subdomain] = 1
            else:
                self.ics_subdomains[subdomain] += 1
        return len(words)
    
    def checkCrawlable(self, soup):
        # check if web is crawlable or readable
        xmlExist = soup.find("xml")
        noIndex = soup.find("meta", content="noindex")
        noFollow = soup.find("meta", content="nofollow")
        noNeither = soup.find("meta", content="noindex,nofollow")
        if noIndex or noFollow or noNeither or xmlExist:
            return False
        return True
    
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

    def checkLengthHeader(self, head):
        # check the header to see if file too big
        if "Content-Length" in head.headers:
            if int(head.headers["Content-Length"]) > 1048576:
                return False
        return True

    def checkRedirect(self, soup):
        # various redirect checking
        # check canonical
        canonical = self.checkCanonical(soup)
        if canonical:
            return canonical
        # check refresh
        refresh = self.checkRefresh(soup)
        if refresh:
            return refresh
        return None
    
    def checkRefresh(self, soup):
        # check for refresh link
        refreshes = soup.find("meta", http_equiv="refresh")
        redirect = refreshes["content"] if refreshes else None
        if redirect:
            if ";" in redirect:
                return redirect.split(";")[0]
            return redir_url
        return None
    
    def checkCanonical(self, soup):
        # check for canonical web, which points to the main website that it should be, which might be duplicated
        # first check link rel
        rel = soup.find("link", rel="canonical")
        canonical = rel["href"] if rel else None
        if canonical:
            return canonical
        # second check og url
        og_url = soup.find("meta", property="og:url")
        canonical = og_url["content"] if og_url else None
        return canonical

    def checkSameUrl(self, url1, url2):
        # check two urls regardless of scheme and www.
        parsed1 = urlparse(url1)
        parsed2 = urlparse(url2)
        netloc1 = parsed1.netloc.strip("www.")
        netloc2 = parsed2.netloc.strip("www.")
        return (netloc1 == netloc2 and parsed1.path == parsed2.path and parsed1.query == parsed2.query
                and parsed1.param == parsed2.param)

    def finalResults(self):
        # print all 4 questions
        for subdomain in sorted(self.ics_subdomains.keys()):
            self.ics_subdomains_formatted.append(f"{subdomain}, {self.ics_subdomains[subdomain]}")
        result = sorted(self.freq.items(), key = lambda x: (-x[1], x[0]))
        self.logger.info(
            f"{len(self.unique_pages)} total unique urls discovered. "
            f"The longest page {self.max_url} has {self.max_len} words. "
            f"Top 50 most common words: {result[0:50]}. "
            f"All ics.uci.edu subdomains: {self.ics_subdomains_formatted}")

    def checkDiscovered(self, url):
        # open one of the four discovered files to see if link is there
        file_name = "weblog/"
        parsed = urlparse(url)
        if "informatics.uci.edu" in parsed.netloc:
            file_name += "inf_discovered.txt"
        elif "ics.uci.edu" in parsed.netloc:
            file_name += "ics_discovered.txt"
        elif "cs.uci.edu" in parsed.netloc:
            file_name += "cs_discovered.txt"
        else:
            file_name += "stat_discovered.txt"
        # open file and see if link is there
        open_file = open(file_name, "r")
        all_links = open_file.readlines()
        withNewLine = url + "\n"
        if withNewLine in all_links:
            open_file.close()
            return True
        open_file.close()
        return False
        
        
