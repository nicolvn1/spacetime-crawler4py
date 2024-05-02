import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from utils import response
from dateutil.parser import parse

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link) and not check_tribe_bar_date(url, link) and not is_crawled(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    
    # Initialize empty set to keep track of unique links
    link_set = set()
    # Get the content from the response
    try:
        # from_encoding keep???
        soup = BeautifulSoup(resp.raw_response.content, 'html.parser', from_encoding = "iso-8859-1")
    except Exception as e:
        return list()
    temp = ""
    # Get the links
    for link in soup.find_all('a'):
        temp = link.get('href')
        # no temp or temp is just fragment
        if not temp or temp.startswith('#'):
            continue
        # defragment
        temp = urldefrag(temp)[0]
        #checks if url is absolute or relative. Transforms relative urls to absolute before adding to list.
        if urlparse(temp).netloc == "": 
            temp = urljoin(resp.url, temp)
        # add to set if not already there
        link_set.add(temp)
    return list(link_set)

def pos_trap(url):
    # detect possible trap by checking if the last 3 path sections are duplicates
    freq = {}
    link = urlparse(url)
    path = link.path
    path_list = path.split("/")
    for k in path_list:
        if len(k) > 1:
            if k not in freq:
                freq[k] = 1
            else:
                freq[k] += 1
                if freq[k] > 1:
                    return True
    return False

def check_tribe_bar_date(current, next):
    # check if tribe-bar-date query is in current and next (possile calendar trap?)
    current_link = urlparse(current)
    next_link = urlparse(next)
    if "tribe-bar-date" in current_link.query and "tribe-bar-date" in next_link.query:
        return True
    return False

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        elif "ics.uci.edu" not in parsed.netloc and "cs.uci.edu" not in parsed.netloc and "informatics.uci.edu" not in parsed.netloc and "stat.uci.edu" not in parsed.netloc:
            return False
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower())

    except TypeError:
        print ("TypeError for ", parsed)
        raise    


def is_crawled(url):
    # check if url is already crawled/discovered
    file_name = "weblog/"
    if "informatics.uci.edu" in urlparse(url).netloc:
        file_name += "inf_discovered.txt"
    elif "ics.uci.edu" in urlparse(url).netloc:
        file_name += "ics_discovered.txt"
    elif "cs.uci.edu" in urlparse(url).netloc:
        file_name += "cs_discovered.txt"
    else: 
        file_name += "stat_discovered.txt"
    open_file = open(file_name, "r")
    all_links = open_file.readlines()
    withnewline = url + "\n"
    if withnewline in all_links:
        open_file.close()
        return True
    open_file.close()
    open_file = open(file_name, "a")
    open_file.write(withnewline)
    open_file.close()
    return False
