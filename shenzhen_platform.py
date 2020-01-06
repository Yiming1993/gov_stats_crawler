import config
from spider import Spider
import requests
import time
import re
import urllib.request
from bs4 import BeautifulSoup

class Shenzhen(Spider):
    def __init__(self):
        super(Shenzhen, self).__init__()
        self.link_json = 'http://opendata.sz.gov.cn/dataapi/queryDataApi'
        self.link = 'http://opendata.sz.gov.cn/dataapi/toDataApi?resType=1&type=2&catagoryId={}'
        self.origin_path, self.db = config.origin_path()
        self.header = {
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Host":"opendata.sz.gov.cn",
            "Connection":"keep-alive",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7,zh-TW;q=0.6",
            "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": "http://opendata.sz.gov.cn",
            "DNT":"1",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Length": "50",
            "Accept-Encoding": "gzip,deflate"
        }

    def build_Cookie(self, link):
        set_cookie = urllib.request.urlopen(link).info()['Set-Cookie']
        json_id = set_cookie.split(';')[0]
        json_id = json_id.split('=')[-1]
        return json_id


    def data_request(self, topicId, pageNo):
        data = {
            "catagoryId": str(topicId),
            "type": "2",
            "order": "1",
            "resType": "1",
            "page": str(pageNo),
            "row": "5"
        }
        return data

    def get_OWASP(self):
        self.get_random_ip()
        link = 'http://opendata.sz.gov.cn/resources/JavaScriptServlet'
        header = {
            "Accept": "*/*",
            "Host": "opendata.sz.gov.cn",
            "Connection": "keep-alive",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7,zh-TW;q=0.6",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "FETCH-CSRF-TOKEN": "1",
            "Origin": "http://opendata.sz.gov.cn",

        }
        header["Referer"] = self.url
        header["User-Agent"] = self.random_select_header(self.usrAgent)
        self.Cookie = "JSESSIONID={}; uid=112".format(self.build_Cookie(link))
        header["Cookie"] = self.Cookie
        response = requests.post(url=link, headers=header, proxies=self.proxy)
        OWASP = response.content.decode('utf-8').split(':')[1]
        return OWASP

    def shenzhen(self):
        def process_shenzhen(data):
            data_list = data.get("result")
            if data_list != []:
                for i in data_list:
                    title = i.get("name")
                    org = i.get("govName")
                    date = i.get("releaseDateForm")
                    id = i.get("id")
                    link = 'http://opendata.sz.gov.cn/dataapi/toApiDetail/{}/1'.format(id)
                    description = i.get("simple")
                    group_name = i.get("categoryName")
                    self.save_dataset(title, link, org, description, date, group_name)
                return 1
            else:
                return 0

        for i in range(1, 15):
            self.url = self.link.format(i)
            for j in range(1, 100):
                owasp = self.get_OWASP()
                data = self.data_request(i, j)
                self.header["User-Agent"] = self.random_select_header(self.usrAgent)
                self.header["Referer"] = self.url
                self.header["Cookie"] = self.Cookie
                self.header["OWASP_CSRFTOKEN"] = owasp
                response = requests.post(url=self.link_json, data=data, headers=self.header, proxies=self.proxy)
                flag = process_shenzhen(response.json())
                if flag == 0:
                    break
                else:
                    pass

    def save_dataset(self, title, link, org, description, date, group_name):
        exists = self.db.shenzhen.find({"link": link}).count()
        if exists == 0:
            self.db.shenzhen.insert({
                "title": title,
                "link": link,
                "org_name": org,
                "description": description,
                "coll_date": self.today,
                "date": date,
                "group": group_name,
            })
            print('dataset {} is saved'.format(title))
        else:
            print('dataset {} exists'.format(title))

    def shenzhen_detail(self, _id, link):
        def process_shenzhen(html):
            soup = BeautifulSoup(html, 'lxml')
            data = soup.select('div[class="operate-list"] div')
            for i in data:
                info = re.sub(r'<[^>]+>', '', str(i))
                info_ = re.sub(r'\s','',info)

                if "：" in info_:
                    key = info_.split("：")[0]
                    value = info_.split("：")[1]

                    if '数据主题' in key:
                        key = info.split("：")[0]
                        value = info.split("：")[1]
                        group = re.split(r'\s',value)
                        group = [m for m in group if m != '']
                        self.update_info('shenzhen', _id, 'group', group)

                    if '关键字' in key:
                        value = re.sub(r'\s','',value)
                        key_words = re.split(r'[，, 、\s]',value)
                        self.update_info('shenzhen', _id, 'key_words', key_words)

                    if '更新时间' in key:
                        date = re.sub(r'\s','',value)
                        self.update_info('shenzhen', _id, 'date', date)

                else:
                    pass

        self.get_random_ip()
        self.header = {}
        self.header["User-Agent"] = self.random_select_header(self.usrAgent)
        self.header["Cookie"] = self.build_Cookie(link)
        proxy_support = urllib.request.ProxyHandler(self.proxy)
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        request = urllib.request.Request(url=link, headers=self.header)
        response = urllib.request.urlopen(request, timeout=60)
        html = str(response.read(), 'utf-8')
        process_shenzhen(html)

if __name__ == '__main__':
    S = Shenzhen()
    S.shenzhen()