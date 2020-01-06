#coding=UTF-8

from spider import Spider
import urllib.request
import re
from urllib.parse import quote
import json
import time
import config


class toutiao(Spider):
    def __init__(self,db_name):
        super(toutiao, self).__init__()
        self.db = config.db_path(db_name)
        self.usrAgent = ["Mozilla/5.0 (Linux; Android 4.1.1; Nexus 7 Build/JRO03D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Safari/535.19",
                         "Mozilla/5.0 (Linux; U; Android 4.0.4; en-gb; GT-I9300 Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30",
                         "Mozilla/5.0 (Linux; U; Android 2.2; en-gb; GT-P1000 Build/FROYO) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
                         "Mozilla/5.0 (Windows NT 6.2; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0",
                         "Mozilla/5.0 (Android; Mobile; rv:14.0) Gecko/14.0 Firefox/14.0",
                         "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36",
                         "Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19",
                         "Mozilla/5.0 (iPad; CPU OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3",
                         "Mozilla/5.0 (iPod; U; CPU like Mac OS X; en) AppleWebKit/420.1 (KHTML, like Gecko) Version/3.0 Mobile/3A101a Safari/419.3"]
        self.header = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Host': 'www.toutiao.com',
            'Pragma': 'no-cache',
            'Accept': 'application/json,text/javascript',
            'Cache-Control': 'no-cache',
            'Accept-Language': 'zh-cn',
            'Connection': 'keep-alive',
            'X-Requested-With': 'XMLHttpRequest',
            'Cookie': '',
        }


    def search_toutiao(self,keyword, page_num = 3):
        '''
        get articles from 今日头条页面搜索
        :param keyword:
        :return:
        '''
        self.items = []

        for i in range(page_num):
            self.get_random_ip()
            self.url = 'https://www.toutiao.com/api/search/content/?aid=24&app_name=web_search&offset={}&format=json&keyword={}&autoload=true&count=20&en_qc=1&cur_tab=1&from=search_tab&pd=synthesis&timestamp={}'.format(str(i * 20), quote(keyword,'utf-8'), str(time.time()).split(".")[0])
            self.referer = 'https://www.toutiao.com/search/?keyword=' + quote(keyword,'utf-8')
            self.header["Referer"] = self.referer
            self.header["User-Agent"] = self.random_select_header(self.usrAgent)

            self.getWeb()
            try:
                self.items = self.items + self.html['data']
                self.content_acquire()
            except:
                pass

    def getWeb(self):
        proxy_support = urllib.request.ProxyHandler(self.proxy)
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        self.request = urllib.request.Request(url=self.url, headers=self.header)
        self.response = urllib.request.urlopen(self.request, timeout=20)
        html = self.response.read()
        html = str(html, 'utf-8')
        self.html = json.loads(html)

    def content_acquire(self):
        for i in self.items:
            if i != []:
                try:
                    self.title = i["title"]
                    self.time = i["datetime"][:10]
                    self.link = i["article_url"]
                    self.name_ch = i["keyword"]
                    self.name = i["media_name"]
                    data = {
                        "title":self.title,
                        "date":self.time,
                        "coll_date":self.today,
                        "sorouce":self.name,
                        "link":self.link
                    }
                    self.save_data('lishui_toutiao',data)
                except:
                    pass
            else:
                break

    def work_flow(self, keyword, page_num = 3):
        print("start collecting %s" %str(keyword))
        self.search_toutiao(str(keyword), page_num = page_num)

    def save_data(self, collection_name,data):
        exist = self.db[collection_name].find({"title":data["title"]}).count()
        if exist == 0:
            self.db[collection_name].insert(data)
            print('news {} is saved'.format(data["title"]))
        else:
            print('news {} exists'.format(data["title"]))

if __name__ == "__main__":
    T = toutiao('')
    T.work_flow('', )
