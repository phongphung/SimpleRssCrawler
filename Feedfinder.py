#!/usr/bin/env python
# -*- coding: utf-8 -*-

from six.moves.urllib import parse as urlparse
import requests
import logging


class FeedFinder(object):
    user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)' \
                 ' Chrome/61.0.3163.100 Safari/537.36'
    timeout = 10

    def __init__(self):
        self.rss_list = []

    def update(self, rss):
        self.rss_list = rss

    def check_duplicates(self, url):
        if url in self.rss_list:
            return False

    def is_feed(self, url):
        text = self.get_feed(url)
        if text is None:
            return False
        return self.is_feed_data(text)

    def get_feed(self, url):
        try:
            r = requests.get(url, headers={"User-Agent": self.user_agent}, timeout=self.timeout)
        except Exception as e:
            logging.warning("Error while getting '{0}'".format(url))
            logging.warning("{0}".format(e))
            return None
        return r.text

    def is_feed_data(self, text):
        data = text.lower()
        if data.count("<html"):
            return False
        return data.count("<rss")+data.count("<rdf")+data.count("<feed")

    def is_feed_url(self, url):
        return any(map(url.lower().endswith,
                       [".rss", ".rdf", ".xml", ".atom"]))

    def is_feedlike_url(self, url):
        return any(map(url.lower().count,
                       ["rss", "rdf", "xml", "atom", "feed"]))

    def find_feeds(self, tree, url, rss_list):

        self.update(rss_list)

        links = []
        for link in tree.find_all("link"):
            if link.get("type") in ["application/rss+xml",
                                    "text/xml",
                                    "application/atom+xml",
                                    "application/x.atom+xml",
                                    "application/x-atom+xml"]:
                links.append(urlparse.urljoin(url, link.get("href", "")))
        #         print('href: ' + link.get("href", ""))
        # print('1: ' + str(links))
        # Look for <a> tags.
        local, remote = [], []
        for a in tree.find_all("a"):
            href = a.get("href", None)
            if href is None:
                continue
            if "://" not in href and self.is_feed_url(href):
                local.append(href)
            if self.is_feedlike_url(href):
                remote.append(href)

        local = [urlparse.urljoin(url, l) for l in local]
        local = list(filter(self.check_duplicates, local))
        links += list(filter(self.is_feed, local))
        logging.info("Found {0} local <a> links to feeds.".format(len(links)))

        # print('2: ' + str(links))
        # Check the remote URLs.
        remote = [urlparse.urljoin(url, l) for l in remote]
        remote = list(filter(self.check_duplicates, remote))
        links += list(filter(self.is_feed, remote))
        logging.info("Found {0} remote <a> links to feeds.".format(len(links)))
        # print('3: ' + str(links))
        # Guessing potential URLs.
        fns = ["atom.xml", "index.atom", "index.rdf", "rss.xml", "index.xml",
               "index.rss"]
        additional = list(filter(self.check_duplicates, [urlparse.urljoin(url, f)
                                             for f in fns]))
        links += list(filter(self.is_feed, additional))
        # print('4: ' + str(links))
        return links

