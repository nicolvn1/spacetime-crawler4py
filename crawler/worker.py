from threading import Thread

from inspect import getsource
from utils.download import download
from utils import get_logger
import scraper
import time

from bs4 import BeautifulSoup
import re

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
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            
            
            #########
            #if completed, add to unique pages set. otherwise, add to pages to be downloaded.
            unique_pages.add(tbd_url.split("#")[0])

            # Check if the status is 200
            if resp.status == 200:
                # Get the content of the url
                soup = BeautifulSoup(resp.raw_response.content, 'html.parser')
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

            # Sort the items in the dictionary of frequencies by descending order
            result = sorted(freq.items(), key=lambda x:(-x[1],x[0]))

            for link in unique_pages:
                if ".ics.uci.edu" in link:
                    subdomain = link.split(".ics.uci.edu")[0] + ".ics.uci.edu"
                    if subdomain not in ics_subdomains:
                        ics_subdomains[subdomain] = 0
                    else:
                        ics_subdomains[subdomain] += 1

            for subdomain in sorted(ics_subdomains.keys()):
                ics_subdomains_formatted.append(f"{subdomain}, {ics_subdomains[subdomain]}")
            #########
            
            
            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)
            
        self.logger.info(
            f"{len(unique_pages)} total unique urls discovered. "
            f"The longest page {max_url} has {max_len} words. "
            f"Top 50 most common words: {result[0:50]}"
            f"All ics.uci.edu subdomains: {ics_subdomains_formatted}")