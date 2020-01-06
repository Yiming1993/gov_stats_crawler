import config
from spider import Spider
import requests
import time
import re
import json

class Guizhou(Spider):
    def __init__(self):
        super(Guizhou, self).__init__()
        self.link = 'http://www.gzdata.gov.cn/dataopen/api/dataset?pageNo={}&pageSize=10&order=0&topicId={}&orgId=&name=&_=1547796437251'
        self.origin_path, self.db = config.origin_path()
        self.header = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Host":"www.gzdata.gov.cn",
            "Connection":"keep-alive",
            "Cache-Control": "max-age=0",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,ja;q=0.7,zh-TW;q=0.6",
            "Upgrade-Insecure-Requests":"1",
            "DNT":"1"
        }

    def data_request(self, topicId, pageNo):
        data = {
            "pageNo": str(pageNo),
            "pageSize": "10",
            "order": "0",
            "topicId": topicId,
            "_":"1547796437251"
        }
        return data

    def cookie_maker(self):
        Cookie = "|{}|{}"
        current_time = time.time()
        current_time = str(current_time)
        return Cookie.format(current_time[:10], current_time[:10])

    def save_dataset(self, title, link, org, description, date, industry_name):
        exists = self.db.guizhou.find({"link":link}).count()
        if exists == 0:
            self.db.guizhou.insert({
                "title":title,
                "link":link,
                "org_name":org,
                "description":description,
                "coll_date":self.today,
                "date":date,
                "group":industry_name,
            })
            print('dataset {} is saved'.format(title))
        else:
            print('dataset {} exists'.format(title))

    def guizhou_platform(self):
        def process_guizhou(data):
            data_list = data.get("data").get("datasetlist")
            if data_list != []:
                for i in data_list:
                    org_name = i.get("orgName")
                    description = re.sub(r'<[^>]+>','',str(i.get("description")))
                    title = i.get("name")
                    industry = i.get("topicName")
                    date = i.get("updTime")[:10]
                    link = 'http://www.gzdata.gov.cn/detail.html?id={}'.format(i.get("id"))
                    self.save_dataset(title, link, org_name, description, date, industry)
                return 1
            else:
                return 0

        for i in range(77, 98):
            topicId = '14742551606' + str(i)
            for j in range(1, 100):
                data = self.data_request(topicId, j)
                self.url = self.link.format(str(j), topicId)
                self.get_random_ip()
                self.header["User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36"
                self.header["Cookie"] = self.cookie_maker()
                response = requests.get(url = self.url, data = data, headers = self.header, proxies = self.proxy)
                data= response.json()
                flag = process_guizhou(data)
                if flag == 0:
                    break
                else:
                    pass

    def guizhou_detail(self, _id, link):
        def make_data():
            data = {
                "callback": "",
                "_": ""
            }
            return data

        def make_header():
            self.header[
                "User-Agent"] = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36"
            self.header[
                "Cookie"] = ""
            self.header["Referer"] = link

        def process_guizhou(data):
            data = re.sub(r'/\*\*/jQuery1113022761563901064974_1548311069111\(', '', str(data))
            data = re.sub(r'\);','',str(data))
            data = json.loads(data, encoding='utf-8')
            data = data.get("data")
            date = data.get("updTime")[:10]
            self.update_info('guizhou', _id, 'date', date)

        dataset_id = link.split('id=')[1]
        self.link = 'http://www.gzdata.gov.cn/dataopen/api/dataset/{}?callback='
        data = make_data()
        self.url = self.link.format(dataset_id)
        self.get_random_ip()
        make_header()
        response = requests.get(url=self.url, data=data, headers=self.header, proxies=self.proxy)
        data = response.content.decode('utf-8')
        process_guizhou(data)

if __name__ == '__main__':
    G = Guizhou()
    links = G.db.guizhou.find({})
    for i in links:
        link = i["link"]
        _id = i["_id"]
        G.guizhou_detail(_id, link)