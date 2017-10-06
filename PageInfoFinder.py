
class PageInfoFinder(object):
    def find_info(self, soup):
        title = ''
        description = ''
        meta_twitter = ''
        locale = ''
        meta_site_name = ''
        meta_title = ''
        meta_url = ''
        twitter_list = []

        temp = soup.find('title')
        if temp:
            title = temp.text

        temp = soup.find("meta", property="og:description") or soup.find("meta", attrs={"name": "description"})
        if temp:
            description = temp.get('content')

        temp = soup.find("meta", property="twitter:site") or soup.find("meta", attrs={"name": "twitter:site"})
        if temp:
            meta_twitter = temp.get('content')

        temp = soup.find("meta", property="og:locale") or soup.find("meta", attrs={"name": "og:locale"})
        if temp:
            locale = temp.get('content')

        temp = soup.find("meta", property="og:site_name") or soup.find("meta", attrs={"name": "keywords"})
        if temp:
            meta_site_name = temp.get('content')

        temp = soup.find("meta", property="og:title") or soup.find("meta", attrs={"name": "og:title"})
        if temp:
            meta_title = temp.get('content')

        temp = soup.find("meta", property="og:url") or soup.find("meta", attrs={"name": "og:url"})
        if temp:
            meta_url = temp.get('content')

        twitter_list = list(map(lambda x: x['href'], soup.select("a[href*=twitter.com/]")))
        return [title, description, meta_twitter, locale, meta_title, meta_url, meta_site_name, twitter_list]
