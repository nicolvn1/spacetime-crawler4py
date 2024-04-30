import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
from utils import response
from dateutil.parser import parse

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

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
    
    # Initialize empty list to keep track of links
    links = []
    # Only look at the link if the status is 200
    if resp.status == 200:
        # Get the content from the response
        try:
            soup = BeautifulSoup(resp.raw_response.content, 'html.parser', from_encoding = "iso-8859-1")
        except Exception as e:
            return list()
        # Extract the script and style elements of the page
        for s in soup(["script", "style"]):
            s.extract()
        temp = ""
        # Get the links
        for link in soup.find_all('a'):
            temp = link.get('href')
            temp = temp.split("#")[0]
            #checks if url is absolute or relative. Transforms relative urls to absolute before adding to list.
            if urlparse(temp).netloc == "": 
                temp = urljoin(resp.url, temp)
            # Add a link without the fragment to the list of links
            if is_valid(temp) and not pos_trap(temp) and not pos_calendar(temp) and not is_crawled(temp):
                links.append(temp)
    return links

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

def pos_calendar(url):
    # detect possible trap by checking if query or path has a calendar date
    link = urlparse(url)
    queries = link.query.split("&")
    path_list = link.path.split("/")
    try:
        path_list.pop()
        path_list.pop(0)
        if path_list != []:
            path_list[-1] = path_list[-1].split(".")[0] # just in case last one ends in .html or something
    except IndexError:
        pass
    # for every query, check if it has date format in the query = 
    for query in queries:
        filtered = query.split("=")[-1]
        # thank you stackoverflow for the date checking piece of code 
        try:
            parse(filtered, fuzzy=False)
            return True
        except ValueError:
            continue
    # for every section in path, check if path is in date format
    for path in path_list:
        try: 
            parse(path, fuzzy=False)
            return True
        except ValueError:
            continue
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
    if "ics.uci.edu" in urlparse(url).netloc:
        file_name += "ics_discovered.txt"
    elif "cs.uci.edu" in urlparse(url).netloc:
        file_name += "cs_discovered.txt"
    elif "inf.uci.edu" in urlparse(url).netloc:
        file_name += "inf_discovered.txt"
    else: 
        file_name += "stat_discovered.txt"
    with open(file_name, "r") as file:
        all_links = file.readlines()
        withnewline = url + "\n"
        if withnewline in all_links:
            print(f"DUPLICATE/DISCOVERED: {url}")
            return True
    with open(file_name, "a") as file:
        file.write(url + "\n")
    return False
