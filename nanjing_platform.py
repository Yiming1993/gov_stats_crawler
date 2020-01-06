import config
from spider import Spider
import requests
import re
import json
import urllib.request
from bs4 import BeautifulSoup

class Nanjing(Spider):
    def __init__(self):
        super(Nanjing, self).__init__()
        self.link = 'http://data.nanjing.gov.cn/dataSearch/szf/condition?callback=jQuery112409031495765581881_1548220232015'
        self.origin_path, self.db = config.origin_path()
        self.header = {
            "Accept": "text/javascript, */*; q=0.01",
            "Host":"data.nanjing.gov.cn",
            "Connection":"keep-alive",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7,zh-TW;q=0.6",
            "DNT":"1",
            "Cookie":"",
            "Origin":"http://data.nanjing.gov.cn",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest"
        }
        self.type_list = ['信用','交通','公安','医疗','卫生','就业','社保','文化','教育','科技',
                          '资源','农业','环境','安监','金融','质量','统计','企业登记监管']

    def data_request(self, pageNo, type_):
        data = {
            "page": str(pageNo),
            "size": "5",
            "chnlId":"76146",
            "type":type_
        }
        return data

    def save_dataset(self, title, link, org, description, date, group_name):
        exists = self.db.nanjing.find({"link":link}).count()
        if exists == 0:
            self.db.nanjing.insert({
                "title":title,
                "link":link,
                "org_name":org,
                "description":description,
                "coll_date":self.today,
                "date":date,
                "group":group_name,
            })
            print('dataset {} is saved'.format(title))
        else:
            print('dataset {} exists'.format(title))

    def nanjing(self):
        def process_nanjing(data):
            data = re.sub('\(','', data)
            data = data[0:-1]
            data = json.loads(data, encoding='utf-8')
            data_list = data.get("data")
            if data_list != []:
                for i in data_list:
                    org_name = i.get("ShuJuTiGongFang")
                    description = re.sub(r'<[^>]+>','',str(i.get("zaiYao")))
                    title = i.get("name")
                    group_name = i.get("fenLei")
                    date = i.get("crttime")[:10]
                    link = i.get("docPubUrl")
                    self.save_dataset(title, link, org_name, description, date, group_name)
                return 1
            else:
                return 0
        for j in self.type_list:
            for i in range(1, 6):
                data = self.data_request(i,str(j))
                self.url = 'http://data.nanjing.gov.cn/sj/?type=21&chnlId=76146'
                self.get_random_ip()
                self.header["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36"
                self.header["Referer"] = self.url
                response = requests.post(url = self.link, data = data, headers = self.header, proxies = self.proxy)
                data = str(response.content.decode('utf-8'))
                flag = process_nanjing(data)
                if flag == 0:
                    break
                else:
                    pass

    def nanjing_detail(self, _id, link):
        def process_nanjing(html):
            soup = BeautifulSoup(html, 'lxml')
            key_list = soup.select('tr th')
            value_list = soup.select('tr td')
            info_list = list(zip(key_list, value_list))

            for key, value in info_list:
                key = re.sub(r'<[^>]+>', '', str(key))
                value = re.sub(r'<[^>]+>', '', str(value))
                value = re.sub(r'&gt;=', '', str(value))

                if '关键字' in key:
                    if value != '':
                        key_words = re.split(r'[、，, \s]',value)
                        self.update_info('nanjing', _id, 'key_words', key_words)

                if '数据提供方' in key:
                    value = re.sub(r'\s', '', str(value))
                    if value != '':
                        org = value
                        self.update_info('nanjing', _id, 'org', org)

        self.get_random_ip()
        self.header["User-Agent"] = self.random_select_header(self.usrAgent)
        proxy_support = urllib.request.ProxyHandler(self.proxy)
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        request = urllib.request.Request(url=link, headers=self.header)
        response = urllib.request.urlopen(request, timeout=60)
        html = str(response.read(), 'utf-8')
        try:
            process_nanjing(html)
        except:
            pass

if __name__ == '__main__':
    N = Nanjing()
    data = N.db.nanjing.find({})
    for doc in data:
        link = doc["link"]
        _id = doc["_id"]
        N.nanjing_detail(_id, link)
