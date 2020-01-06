import config
from spider import Spider
import re
import urllib.request
import json
import requests

class Xiamen(Spider):
    def __init__(self):
        super(Xiamen, self).__init__()
        self.db = config.db_path('govdata')
        self.link = 'http://www.xmdata.gov.cn/sj/datasource.js'
        self.link_new = 'http://data.xm.gov.cn/datas/?type=subject&value={}'
        self.header = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Pragma": "no-cache",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Host": "data.xm.gov.cn",
            "Cache-Control": "no-cache",
            "Accept-Language": "zh-cn",
            "Origin": "http://data.xm.gov.cn",
            "Connection": "keep-alive",
            "X-Requested-With": "XMLHttpRequest"
        }


    def xiamen(self):
        def process_xiamen(html):
            data = html.split('var')
            data = data[1]
            data = re.sub(r'dataSource = ','',str(data))

            data = re.sub(r';', '', str(data))
            data = '{"data_list":' + data
            data = data + '}'
            data = json.loads(data, encoding='utf-8')
            data = re.sub(r'\s', '', str(data))
            data_list = data.get("data_list")
            for i in data_list:
                try:
                    title = i.get("data_name")
                    link = i.get("url")
                    description = re.sub(r'\n', '', str(i.get("abstract")))
                    date = i.get("publish_date")
                    tag = [i.get("label")]
                    group_name = i.get("data_area")
                    org_name = i.get("provider_org")
                    attachments = i.get("attachments")
                    public_type = i.get("public_type")
                    data_num = i.get("dataCount")
                    self.save_dataset(title, link, org_name, description, date,
                                      tag, group_name, data_num, attachments, public_type)
                except:
                    pass


        self.url = self.link
        self.get_random_ip()
        self.header["User-Agent"] = self.random_select_header(self.usrAgent)
        proxy_support = urllib.request.ProxyHandler(self.proxy)
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        request = urllib.request.Request(url=self.url, headers=self.header)
        response = urllib.request.urlopen(request, timeout=60)
        html = str(response.read(), 'utf-8')
        process_xiamen(html)

    def build_Cookie(self, link):
        set_cookie = urllib.request.urlopen(link).info()['Set-Cookie']
        json_id = set_cookie.split(';')[0]
        json_id = json_id.split('=')[-1]
        return json_id

    def build_data(self, page):
        data = {
        "params": "",
        "pageNo": page,
        "pageSize": 5
        }
        return data

    def xiamen_new(self):
        def process_xiamen(data):
            data = json.loads(data, encoding='utf-8')
            data_list = data.get("rows")
            if data_list != []:
                for i in data_list:
                    save_data = {}
                    title = i["DOCTITLE"]
                    link = i["DOCPUBURL"]
                    org = i["METADATA"].get("BA_ORG")
                    description = i["METADATA"].get("BA_INTRO")
                    date = i["DOCRELTIME"][:10]
                    tag = i["METADATA"].get("BA_INDUS")
                    if tag != None:
                        tag = tag.split(",")
                    if type(tag) == str:
                        tag = [tag]
                    group = i["METADATA"].get("BA_THEME")
                    if type(group) == str:
                        group = [group]
                    public_type = i["METADATA"].get("BA_STATUS")

                    save_data = {
                            "title": title,
                            "link": link,
                            "org_name": org,
                            "description": description,
                            "coll_date": self.today,
                            "date": date,
                            "tags": tag,
                            "group": group,
                            "public_type": public_type,
                    }
                    save_data = {i[0]:i[1] for i in save_data.items() if i[1] != None}
                    exists = self.db['xiamen'].find({"title":title}).count()
                    if exists == 0:
                        self.db['xiamen'].insert(save_data)
                        print('dataset {} is saved'.format(title))
                    else:
                        print('dataset {} exists'.format(title))
                return 0
            else:
                return 1

        for i in range(452, 474):
            for j in range(0, 50):
                self.link_json = 'http://data.xm.gov.cn/portal/api/data/list.xhtml'
                self.link = self.link_new.format(i)
                Cookie = 'HttpOnly=true; HttpOnly=true; JSESSIONID={}; HttpOnly=true'.format(self.build_Cookie(self.link_json))
                self.header["Cookie"] = Cookie
                self.header["Referrer"] = self.link
                self.header["User-Agent"] = self.random_select_header(self.usrAgent)
                self.data = self.build_data(j + 1)
                self.get_random_ip()
                response = requests.get(url=self.link_json, data=self.data, headers=self.header, proxies=self.proxy)
                data = response.content.decode('utf-8')
                flag = process_xiamen(data)
                if flag == 1:
                    break
                else:
                    pass


    def save_dataset(self, title, link, org, description, date, tag, group_name, data_num, attachments, public_type):
        exists = self.db.xiamen.find({"link": link}).count()
        if exists == 0:
            self.db.xiamen.insert({
                "title": title,
                "link": link,
                "org_name": org,
                "description": description,
                "coll_date": self.today,
                "date": date,
                "tags":tag,
                "group": group_name,
                "attachments": attachments,
                "public_type":public_type,
                "data_num":int(data_num)
            })
            print('dataset {} is saved'.format(title))
        else:
            print('dataset {} exists'.format(title))

if __name__ == '__main__':
    X = Xiamen()
    X.xiamen_new()