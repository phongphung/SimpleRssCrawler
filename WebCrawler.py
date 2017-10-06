import urllib.request
import http.client
import tldextract
from bs4 import BeautifulSoup
from Feedfinder import FeedFinder
from PageInfoFinder import PageInfoFinder
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
    def __init__(self, _url_list, max_depth, min_wait, backup_name):
        self.control = pd.DataFrame(columns=['domain', 'crawled', 'level'])
        self.data = pd.DataFrame(columns=['rss_sets', 'links_sets', 'time_crawled', 'next_available', 'title',
                                          'description', 'meta_twitter', 'locale', 'meta_title', 'meta_url',
                                          'meta_site_name', 'twitter_list'], dtype=object)
        self.feed_finder = FeedFinder()
        self.url_list = _url_list
        self.max_depth = max_depth
        self.min_wait = min_wait
        self.backup_name = backup_name
        self.count_to_backup = 0

        for url in list(_url_list):
            self.control.loc[url] = [get_domain(url), 0, 1]
            self.data.loc[get_domain(url)] = [set(), {url}, 0, 0, '', '', '', '', '', '', '', []]
        self.data['twitter_list'] = self.data['twitter_list'].astype(object)
        self.page_info_finder = PageInfoFinder()
        self.debug = ''

    def decide_url(self):
        min_next_available = 0
        control = self.control.loc[((self.control['crawled'] == 0) & (self.control['level'] <= self.max_depth))]
        if control.empty:
            return 'done'
        now = time.time()
        for url in control.index:
            domain = self.control.loc[url, 'domain']
            next_available = self.data.loc[domain, 'next_available']
            min_next_available = min(min_next_available, next_available)
            if next_available < now:
                self.data.loc[domain, 'time_crawled'] = now
                self.data.loc[domain, 'next_available'] = now + self.min_wait
                self.control.loc[url, 'crawled'] = 1
                return url, None
        return None, (min_next_available + 1 - now)

    def update_control(self, level, links):
        self.count_to_backup += 1
        for link in links:
            self.control.loc[link] = [get_domain(link), 0, level + 1]
        if self.count_to_backup == 2:
            self.count_to_backup = 0
            self.backup()
        return

    def update_page_info(self, domain, page_info):
        self.data.loc[domain, ['title', 'description', 'meta_twitter', 'locale', 'meta_title', 'meta_url',
                            'meta_site_name']] = page_info[:-1]
        self.data.set_value(domain, 'twitter_list', page_info[-1])


    def backup(self):
        self.data.to_csv('Crawled_' + str(self.backup_name) + '.csv')

    def crawl(self):
        while True:
            url, min_wait_time = self.decide_url()
            while url is None:
                print('waiting: ' + str(min_wait_time))
                if min_wait_time < 0:
                    break
                else:
                    time.sleep(min_wait_time + 1 - time.time())
                url = self.decide_url(), min_wait_time

            if url == 'done':
                break

            current_level = self.control.loc[url, 'level']

            domain = get_domain(url)
            web_crawler = WebCrawler(self.data.loc[domain, 'rss_sets'], self.data.loc[domain, 'links_sets'],
                                     self.feed_finder, url, self.page_info_finder, current_level)
            web_crawler.crawl()
            additional_links = web_crawler.additional
            self.data.loc[domain, 'rss_sets'].update(web_crawler.rss_set)
            self.data.loc[domain, 'links_sets'].update(additional_links)

            if current_level == 1:
                page_info = web_crawler.page_info
                self.debug = web_crawler.page_info
                self.update_page_info(domain, page_info)

            # update level
            self.update_control(current_level, additional_links)


class WebCrawler:
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)' \
                 ' Chrome/61.0.3163.100 Safari/537.36'
    timeout = 10

    def __init__(self, rss_set, links, feed_finder, url, page_info_finder, current_level):
        self.links = links
        self.feed_finder = feed_finder
        self.rss_set = rss_set
        self.url = url
        self.initial_depth = 0
        self.additional = set()
        self.page_info_finder = page_info_finder
        self.current_level = current_level
        self.page_info = ['', '', '', '', '', '', '', '']

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
        except http.client.IncompleteRead as e:
            f = e.partial
            soup = BeautifulSoup(f, 'html.parser')
        except Exception as e:
            print('Error in opening: ' + str(url))
            print('Error: ' + str(e))
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

        # GET info
        if soup != '':
            self.save_all_links_on_page(soup)
            rss = self.feed_finder.find_feeds(soup, self.url, list(self.rss_set))
            if self.current_level == 1:
                self.page_info = self.page_info_finder.find_info(soup)
            self.rss_set.update(rss)


if __name__ == '__main__':
    backup = 'tue_RSS'
    url_list = list(pd.read_csv('tue_RSS.csv')['url'])[:3]

    crawler = WebCrawlerWraper(url_list, 2, 0, backup)
    crawler.crawl()
