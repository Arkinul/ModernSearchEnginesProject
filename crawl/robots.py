from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

#TODO: Instead of robots_cache dictonary use database to save and fetch robots.txt. We probably need an additional field for the robots.txt in the database
robots_cache = {}

def get_host(url):
    '''
    Extracts the host (scheme and netloc) from the given URL.

    Parameters:
    url (str): The URL from which the host is to be extracted.

    Returns:
    str: The host part of the URL, including the scheme.
    '''
    parsed_url = urlparse(url)
    return parsed_url.scheme + "://" + parsed_url.netloc


def is_allowed_by_robots(url):
    '''
    Determines if the given URL is allowed to be crawled according to the robots.txt rules of the host.

    Parameters:
    url (str): The URL to check against the robots.txt rules.

    Returns:
    bool: True if the URL is allowed to be crawled, False otherwise.
    '''
    host = get_host(url)
    if host not in robots_cache:
        robots_txt_url = host + "/robots.txt"
        rp = RobotFileParser()
        rp.set_url(robots_txt_url)
        rp.read()
        robots_cache[host] = rp

    rp = robots_cache[host]
    user_agent = 'MSE_Crawler'
    if rp.can_fetch(user_agent, url):
        return True
    else:
        return False

