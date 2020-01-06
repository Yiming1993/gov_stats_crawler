from spider import Spider
import datetime
import re
from urllib.request import quote
import urllib.request
from bs4 import BeautifulSoup
import time
import json
import random
import config
import requests
from http import cookiejar

class taobao(Spider):
    def __init__(self, collection_name):
        super(taobao,self).__init__()
        # print(self.keywords)
        self.today = re.sub(r'-','',str(datetime.datetime.now())[:10])
        self.origin_count = 4
        self.origin_page = 0
        # print(self.url)
        self.header = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Cache-Control': 'max-age=0',
            'Accept-Language': 'zh-CN,zh;q=0.8,en;q=0.6,ja;q=0.4,zh-TW;q=0.2',
            'X-Requested-With': 'XMLHttpRequest',
            'Cookie':'',
            'Host':'s.taobao.com'}
        self.cookie_origin = ''
        self.usr_agent = [
            "Mozilla/5.0 (Linux; Android 4.1.1; Nexus 7 Build/JRO03D) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166 Safari/535.19",
            "Mozilla/5.0 (Linux; U; Android 4.0.4; en-gb; GT-I9300 Build/IMM76D) AppleWebKit/534.30 (KHTML, like Gecko) Version/4.0 Mobile Safari/534.30",
            "Mozilla/5.0 (Linux; U; Android 2.2; en-gb; GT-P1000 Build/FROYO) AppleWebKit/533.1 (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1",
            "Mozilla/5.0 (Windows NT 6.2; WOW64; rv:21.0) Gecko/20100101 Firefox/21.0",
            "Mozilla/5.0 (Android; Mobile; rv:14.0) Gecko/14.0 Firefox/14.0",
            "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/27.0.1453.94 Safari/537.36",
            "Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19",
            "Mozilla/5.0 (iPad; CPU OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3",
            "Mozilla/5.0 (iPod; U; CPU like Mac OS X; en) AppleWebKit/420.1 (KHTML, like Gecko) Version/3.0 Mobile/3A101a Safari/419.3"]

        self.product_collect = []
        self.proxy_pool = []
        self.db = config.db_path('taobao')
        self.collection_name = collection_name

    def make_data(self, keywords, range_num):
        if range_num > 0:
            ktsts = time.time()
            _ksTS = '%s_%s' % (int(ktsts * 1000), str(ktsts)[-3:])
            callback = "jsonp%s" % (int(str(ktsts)[-3:]) + 1)

            data = {
                "q": str(keywords),
                "imgfile": '',
                "js": 1,
                "stats_click": "search_radio_all:1",
                "initiative_id": "staobaoz_" + re.sub('-', '', str(self.today)),
                "ie": "utf8",
                "data-key":"s",
                "data-value":44*range_num,
                "ajax": True,
                "_ksTS":_ksTS,
                "callback":callback,
                "bcoffset": self.origin_count+(-3*range_num),
                "ntoffset": -3 * range_num,
                "p4ppushleft": "1,48",
                "s": self.origin_page+44*range_num
            }
        else:
            data = {
                "q":str(keywords),
                "imgfile":'',
                "js":1,
                "stats_click":"search_radio_all:1",
                "initiative_id":"staobaoz_" + re.sub('-','',str(self.today)),
                "ie":"utf8"
                }
        return data

    def url_construct(self,keywords, range_num):
        url_1 = 'https://s.taobao.com/search?q=' + quote(keywords, 'utf-8')
        url_2 = '&imgfile=&js=1&stats_click=search_radio_all%3A1&initiative_id=staobaoz_' + self.today
        url_3 = '&ie=utf8'
        if range_num > 0:
            url_4 = '&bcoffset=' + str(self.origin_count+(-3*range_num)) + '&ntoffset=' + str(
                -3 * range_num) + '&p4ppushleft=2%2C48&s=' + str(self.origin_page+44*range_num)
        else:
            url_4 = ''
        url = url_1 + url_2 + url_3 + url_4
        return url


    def get_web(self, keywords,  limit):
        def process_taobao(html):
            _raw_info = re.findall(r'g_page_config = (.*?) g_srp_loadCss', html, re.S)
            if len(_raw_info) > 0:
                raw_info = json.loads(_raw_info[0].strip()[:-1])
                try:
                    product_list = raw_info['mods']['itemlist']['data']['auctions']

                    for i in product_list:
                        data_dict = {}
                        sales = float(re.findall(r'[0-9]{1,}', str(i['view_sales']))[0])
                        link = i['detail_url']
                        name = re.sub(r'<[^>]+>', '', str(i['title']))
                        if 'http' not in link:
                            link = 'http:' + link
                        data_dict["name"] = name
                        data_dict["sales"] = int(sales)
                        data_dict["price"] = float(i["view_price"])
                        data_dict["link"] = link
                        location = i["item_loc"]
                        if ' ' in location:
                            province = location.split(' ')[0]
                            city = location.split(' ')[1]

                            data_dict["province"] = province
                            data_dict["city"] = city
                        else:
                            city = location
                            data_dict["province"] = city

                        data_dict["shop"] = i["nick"]
                        try:
                            similar_link = i["i2iTags"]["similar"]["url"]
                            if similar_link != '':
                                data_dict["similar"] = 'https://s.taobao.com' + similar_link
                        except:
                            pass
                        try:
                            comment_link = i["comment_url"]
                            if 'http' not in comment_link:
                                data_dict["comment"] = 'http:' + comment_link
                        except:
                            pass
                        try:
                            shop_link = i["shopLink"]
                            if 'http' not in shop_link:
                                shop_link = 'http:' + shop_link
                            data_dict["shop_link"] = shop_link
                        except:
                            pass

                        self.save_products(data_dict)
                except:
                    pass


        for i in range(0, limit):
            print('collecting data from page {}'.format(i))
            url = self.url_construct(keywords, i)
            self.get_random_ip()
            proxy_support = urllib.request.ProxyHandler(self.proxy)
            opener = urllib.request.build_opener(proxy_support)
            urllib.request.install_opener(opener)
            self.header['user-agent'] = self.random_select_header(self.usr_agent)

            if i != 0:
                self.header['Cookie'] = self.cookie
            else:
                pass
            # print(self.header['Cookie'])
            request = urllib.request.Request(url=url, headers=self.header)
            response = urllib.request.urlopen(request, timeout=40)
            cookie = self.get_cookie(url)
            cookie_collect = []
            for item in cookie:
                if item.name != 'cookieCheck':
                    iter = '{}={}'.format(item.name, item.value)
                    cookie_collect.append(iter)
            cookie_extend = ';'.join(cookie_collect)
            self.cookie = self.cookie_origin + cookie_extend + ';' + response.headers['Set-Cookie']
            html = str(response.read(), 'utf-8')
            process_taobao(html)
            time.sleep(20)


    def get_cookie(self, link):
        cookie = cookiejar.CookieJar()
        # 利用urllib.request库的HTTPCookieProcessor对象来创建cookie处理器,也就CookieHandler
        handler = urllib.request.HTTPCookieProcessor(cookie)
        # 通过CookieHandler创建opener
        opener = urllib.request.build_opener(handler)
        # 此处的open方法打开网页
        response = opener.open(link)
        # 打印cookie信息
        return cookie

    def get_jsessionid(self, link, referer, data):
        s = requests.session()
        s.headers.update({'referer': referer})
        r = s.get(link, data=data)
        jsession = r.headers['Set-Cookie']
        print(jsession)
        jsession2 = dict(r.cookies)['JSESSIONID']
        print(jsession2)
        return jsession2


    def format_control(self,target):
        result = re.sub(r'<[^>]+>','',str(target))
        return result

    def save_products(self, data_dict):
        exist = self.db[self.collection_name].find({'link':data_dict['link']}).count()
        if exist == 0:
            self.db[self.collection_name].insert(data_dict)

            print('the product %s is saved' %str(data_dict['name']))
        else:
            print('the product %s exists' %str(data_dict['name']))

    def work_flow(self, keywords, limits = 100):
        self.keywords_save = keywords
        self.get_web(keywords, limits)

if __name__ == '__main__':
    taobao = taobao('')
    taobao.work_flow('')