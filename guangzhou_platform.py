import config
from spider import Spider
import requests
import urllib.request
from bs4 import BeautifulSoup
import re

class Guangzhou(Spider):
    def __init__(self):
        super(Guangzhou, self).__init__()
        self.link_json = 'http://www.datagz.gov.cn/odweb/catalog/catalog.do?method=GetCatalog'
        self.header = {
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "Pragma": "no-cache",
            "Accept": "application/json,text/javascript,*/*;q=0.01",
            "Host": "www.datagz.gov.cn",
            "Cache-Control": "no-cache",
            "Accept-Language": "zh-cn",
            "Connection": "keep-alive",
            "Cookie":"",
            "X-Requested-With": "XMLHttpRequest",
            "Origin":"www.datagz.gov.cn"
        }
        self.origin_path, self.db = config.origin_path()

    def data_request(self, group_id, start_page_length):
        group_ = 'sub-' + str(group_id)
        start_page = start_page_length * 6
        data = {"group_id":group_,
        # "org_code":"",
        # "ind_id":"",
        "start":start_page,
        "length": 6,
        "pageLength": 6,
        # "cata_type":"",
        # "keywords":"",
        "_order": "1:b"}
        return data

    def format_control(self, data):
        if data == "" or data == []:
            return None
        else:
            return data

    def guangzhou_platform(self):
        def process_guangzhou(data):
            data_list = data.get("data")
            if data_list != []:
                for i in data_list:
                    title = i.get("cata_title")
                    link = 'http://www.datagz.gov.cn/odweb/catalog/catalogDetail.htm?cata_id=' + i.get("cata_id")
                    org_name = i.get("org_name")
                    description = i.get("description")
                    date = i.get("released_time")
                    if date != []:
                        date = date[:10]
                    else:
                        date = None
                    key_words = i.get("cata_tags").split(",")
                    group_name = i.get("cataLogGroups")
                    if group_name != []:
                        group_name = group_name[0].get("group_name")
                    else:
                        group_name = None
                    industry_name = i.get("cataLogIndustrys")
                    if industry_name != []:
                        industry_name = industry_name[0].get("group_name")
                    else:
                        industry_name = None
                    data_num = i.get("catalogStatistic").get("data_count")
                    file_num = i.get("catalogStatistic").get("file_count")
                    public_type = i.get("open_type")
                    self.save_dataset(title, link, org_name, description, date, key_words, industry_name, group_name,
                                      data_num, file_num, public_type)
                return 1
            else:
                return 0

        for i in range(1,17):
            for j in range(0, 100):
                self.get_random_ip()
                data = self.data_request(i,j)
                self.link = 'http://www.datagz.gov.cn/odweb/catalog/index.htm?subjectId=sub-{}'.format(str(i))
                self.header["User-Agent"] = self.random_select_header(self.usrAgent)
                self.header["Referer"] = self.link
                response = requests.post(url = self.link_json, data = data, headers = self.header, proxies = self.proxy)
                data = response.json()
                flag = process_guangzhou(data)
                if flag == 1:
                    pass
                else:
                    break

    def save_dataset(self, title, link, org, description, date, key_words, industry_name, group_name, data_num, file_num, public_type):
        exists = self.db.guangzhou.find({"link":link}).count()
        if exists == 0:
            self.db.guangzhou.insert({
                "title":title,
                "link":link,
                "org_name":org,
                "description":description,
                "coll_date":self.today,
                "date":date,
                "group":group_name,
                "industry":industry_name,
                "data_num":int(data_num),
                "file_num":int(file_num),
                "key_words":key_words,
                "public_type":public_type
            })
            print('dataset {} is saved'.format(title))
        else:
            print('dataset {} exists'.format(title))

    def guangzhou_detail(self, _id, link):
        def process_guangzhou(html):
            soup = BeautifulSoup(html, 'lxml')
            data = soup.select('div[class="detail-base-list"] li')
            for i in data:
                key = i.select('div')[0]
                key = re.sub(r'<[^>]+>','',str(key))
                value = i.select('div')[1]
                value = re.sub(r'<[^>]+>','',str(value))
                value = re.sub(r'\s','',str(value))

                if '最后更新' in key:
                    date = value[:10]
                    month = date.split('-')[1]
                    day = date.split('-')[2]
                    if len(month) == 1:
                        month = '0' + month
                    if len(day) != 2:
                        if int(day[0]) >= 3:
                            day = '0' + day[0]
                        else:
                            day = day[:2]
                    date = date.split('-')[0] + '-' + month + '-' + day
                    self.update_info('guangzhou', _id, 'date', date)

                if '数据格式' in key:
                    source_type = value.split("，")
                    self.update_info('guangzhou', _id, 'source_type', source_type)

        self.get_random_ip()
        self.header["User-Agent"] = self.random_select_header(self.usrAgent)
        proxy_support = urllib.request.ProxyHandler(self.proxy)
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        request = urllib.request.Request(url=link, headers=self.header)
        response = urllib.request.urlopen(request, timeout=60)
        html = str(response.read(), 'utf-8')
        process_guangzhou(html)

if __name__ == '__main__':
    G = Guangzhou()
    data = G.db.guangzhou.find({})
    for doc in data:
        _id = doc["_id"]
        link = doc["link"]
        G.guangzhou_detail(_id, link)