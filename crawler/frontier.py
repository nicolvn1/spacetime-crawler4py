import os
import shelve

from threading import Thread, RLock
from queue import Queue, Empty

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

import re
import requests
from bs4 import BeautifulSoup

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = list()
        
        if not os.path.exists(self.config.save_file) and not restart:
            # Save file does not exist, but request to load save.
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            # Save file does exists, but request to start from seed.
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        # Load existing save file, or create one if it does not exist.
        self.save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                self.add_url(url)
        else:
            # Set the frontier state with contents of save file.
            self._parse_save_file()
            if not self.save:
                for url in self.config.seed_urls:
                    self.add_url(url)

    def _parse_save_file(self):
        ''' This function can be overridden for alternate saving techniques. '''
        stopwords = ["a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't", "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"]
        
        total_count = len(self.save)
        tbd_count = 0
        unique = set()
        max_len = 0
        max_url = ""
        freq = {}
        for url, completed in self.save.values():
            if completed and is_valid(url):
                
                page = requests.get(url)
                soup = BeautifulSoup(page.content, 'html.parser')
                for s in soup(["script", "style"]):
                    s.extract()
                words = [word for word in re.split("[^a-zA-Z0-9']", soup.get_text()) if word != ""]
                if max_len < len(words):
                    max_len = len(words)
                    max_url = url
                    
                for k in words:
                    k = k.lower()
                    if len(k) > 1 and k not in stopwords:
                        if k not in freq:
                            freq[k] = 1
                        else:
                            freq[k] += 1
                
            if not completed and is_valid(url):
                self.to_be_downloaded.append(url)
                tbd_count += 1
                
        result = []
        maxVal = 0
        maxIndex = 0

        # Iterate through the dictionary and add the pairs to a list
        for key, value in freq.items():
            result.append([key, value])

        # Iterate by index through the list
        for i in range(len(result)):
            # Have the max value be the starting value
            maxVal = result[i][1]
            maxIndex = i

            # Iterate by index from the starting i value to the end of the list
            for j in range(i, len(result)):
                # If the values are the same, order them alphabetically
                if result[j][1] == maxVal:
                    if result[j][0] < result[maxIndex][0]:
                        maxVal = result[j][1]
                        maxIndex = j

                # If the new value is larger than the current max,
                # replace the max value and keep track of its index
                elif result[j][1] > maxVal:
                    maxVal = result[j][1]
                    maxIndex = j

            # Swap the starting index value with the max value
            temp = result[i]
            result[i] = result[maxIndex]
            result[maxIndex] = temp
        
        self.logger.info(
            f"Found {tbd_count} urls to be downloaded from {total_count} "
            f"total urls discovered. "
            f"Page {max_url} has {max_len} words. "
            f"Top 50 most common words: {result[0:50]}")

    def get_tbd_url(self):
        try:
            return self.to_be_downloaded.pop()
        except IndexError:
            return None

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            self.save[urlhash] = (url, False)
            self.save.sync()
            self.to_be_downloaded.append(url)
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        if urlhash not in self.save:
            # This should not happen.
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")

        self.save[urlhash] = (url, True)
        self.save.sync()
