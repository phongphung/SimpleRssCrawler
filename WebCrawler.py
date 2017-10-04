import urllib.request
import http.client
import tldextract
from bs4 import BeautifulSoup
from Feedfinder import FeedFinder
import urllib.parse
import time
import pandas as pd


def coerce_url(url):
    url = url.strip()
    if url.startswith("feed://"):
        return "http://{0}".format(url[7:])
    for proto in ["http://", "https://"]:
        if url.startswith(proto):
            return url
    return "http://{0}".format(url)


def check_filters(url, parent_url):
    check = (tldextract.extract(url).domain == tldextract.extract(parent_url).domain)
    check = check and (not ('.jpg' in url))
    return check


def trim_url(url):
    extract_url = tldextract.extract(url)
    temp_main_url = (extract_url.domain + '.' + extract_url.suffix)
    if extract_url.subdomain != '':
        temp_main_url = extract_url.subdomain + '.' + temp_main_url
    temp_main_url = coerce_url(temp_main_url)
    return temp_main_url


def get_domain(url):
    return tldextract.extract(url).domain


class WebCrawlerWraper:
    def __init__(self, url_list, max_depth, min_wait):
        self.control = pd.DataFrame(columns=['domain', 'crawled', 'level'])
        self.data = pd.DataFrame(columns=['rss_sets', 'links_sets', 'time_crawled', 'next_available'])
        self.feed_finder = FeedFinder()
        self.url_list = url_list
        self.max_depth = max_depth
        self.min_wait = min_wait

        for url in list(url_list):
            self.control.loc[url] = [get_domain(url), 0, 1]
            self.data.loc[get_domain(url)] = [set(), {url}, 0, 0]

    def decide_url(self):

        control = self.control.loc[((self.control['crawled'] == 0) & (self.control['level'] <= self.max_depth))]
        if control.empty:
            return 'done'

        for url in control.index:
            domain = self.control.loc[url, 'domain']
            if self.data.loc[domain, 'next_available'] < time.time():
                self.data.loc[domain, 'time_crawled'] = time.time()
                self.data.loc[domain, 'next_available'] = time.time() + self.min_wait
                self.control.loc[url, 'crawled'] = 1
                return url

    def update_control(self, level, links):
        for link in links:
            self.control.loc[link] = [get_domain(link), 0, level + 1]
        return

    def crawl(self):
        while True:
            url = self.decide_url()
            while url is None:
                min_wait_time = self.data['next_available'].min()
                print('waiting: ' + str(min_wait_time + 1 - time.time()))
                if min_wait_time + 1 - time.time() < 1:
                    time.sleep(1)
                else:
                    time.sleep(min_wait_time + 1 - time.time())
                url = self.decide_url()

            current_level = self.control.loc[url, 'level']
            if url == 'done':
                break

            domain = get_domain(url)
            web_crawler = WebCrawler(self.data.loc[domain, 'rss_sets'], self.data.loc[domain, 'links_sets'],
                                     self.feed_finder, url)
            web_crawler.crawl()
            additional_links = web_crawler.additional
            self.data.loc[domain, 'rss_sets'].update(web_crawler.rss_set)
            self.data.loc[domain, 'links_sets'].update(additional_links)

            # update level
            self.update_control(current_level, additional_links)


class WebCrawler:
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)' \
                 ' Chrome/61.0.3163.100 Safari/537.36'
    timeout = 10

    def __init__(self, rss_set, links, feed_finder, url):
        self.links = links
        self.feed_finder = feed_finder
        self.rss_set = rss_set
        self.url = url
        self.initial_depth = 0
        self.additional = set()

    def get_page(self, url):
        """ loads a webpage into a string """
        soup = ''
        try:
            req = urllib.request.Request(url)
            req.add_header(key='User-Agent', val=self.user_agent)
            f = urllib.request.urlopen(req)
            soup = BeautifulSoup(f, 'html.parser')
            f.close()
        except IOError:
            print("Error opening {}".format(url))
        except http.client.InvalidURL as e:
            print("{} caused an Invalid URL error.".format(url))
            if hasattr(e, 'reason'):
                print('We failed to reach a server.')
                print('Reason: ', e.reason)
            elif hasattr(e, 'code'):
                print('The server couldn\'t fulfill the request.')
                print('Error code: ', e.code)
        return soup

    def save_all_links_on_page(self, soup):
        if soup == '':
            print('No page got')
            return
        for a in soup.find_all('a', href=True):
            link = a['href']
            if len(link) == 0:
                continue
            if link[0] == '/':
                link = urllib.parse.urljoin(self.url, link)
            if check_filters(link, self.url):
                if link not in self.links:
                    # adding link to global dictionary
                    self.additional.add(link)

    def crawl(self):
        print("Get {}".format(self.url))

        soup = self.get_page(coerce_url(self.url))

        # GET RSS
        if soup != '':
            self.save_all_links_on_page(soup)
            rss = self.feed_finder.find_feeds(soup, self.url, list(self.rss_set))
            self.rss_set.update(rss)


if __name__ == '__main__':
    crawler = WebCrawlerWraper(list(chunks_list)[0], 2, 10)
    crawler.crawl()