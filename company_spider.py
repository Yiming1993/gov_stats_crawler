#coding='utf-8'
from spider import Spider
import config
import urllib.request
from urllib.parse import quote
from bs4 import BeautifulSoup
import re
from bson import ObjectId
import argparse

class Company_spider(Spider):
    def __init__(self):
        super(Company_spider, self).__init__()
        self.db = config.db_path('company')
        self.page_range = 100000
        self.page = 0
        self.link = 'https://gongshang.mingluji.com/{}/{}?page={}'
        self.link_company = 'https://gongshang.mingluji.com/{}/{}'
        self.header = {
            "Cookie": "__utma=152261551.1924106988.1551684684.1551753961.1551758571.6; __utmb=152261551.4.10.1551758571; __utmc=152261551; __utmz=152261551.1551684684.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; Hm_lpvt_f733651f7f7c9cfc0c1c62ebc1f6388e=1551758752; Hm_lvt_f733651f7f7c9cfc0c1c62ebc1f6388e=1551684684,1551684753,1551685134,1551685760; has_js=1; __utmt=1; __utma=2324140.502488889.1551685134.1551685134.1551685134.1; __utmc=2324140; __utmz=2324140.1551685134.1.1.utmcsr=baidu|utmccn=(organic)|utmcmd=organic"
        }
        self.region_list = [
            # 'beijing',
            # 'tianjin',
            'hebei',
            'neimenggu',
            'shanxi',
            'shanghai',
            'anhui',
            'jiangsu',
            'zhejiang',
            'shandong',
            'jiangxi',
            'fujian',
            'guangdong',
            'guangxi',
            # 'hainan',
            # 'henan',
            # 'hubei',
            # 'hunan',
            # 'heilongjiang',
            # 'jilin',
            # 'liaoning',
            # 'shaanxi',
            # 'gansu',
            # 'ningxia',
            # 'qinghai',
            # 'xinjiang',
            # 'chongqing',
            # 'sichuan',
            # 'yunnan',
            # 'guizhou',
            # 'xizang'
        ]
        self.industry_list = [
            '五金',
            '仪器',
            '保险',
            '信贷',
            '储运',
            '农业',
            '冶金',
            '出版',
            '包装',
            '化工',
            '器材',
            '土产',
            '展览',
            '工程',
            '广告',
            '建筑',
            '房地产',
            '服装',
            '机械',
            '机电',
            '林业',
            '水产',
            '水利',
            '汽车',
            '烟草',
            '煤气',
            '煤炭',
            '电力',
            '电器',
            '电子',
            '畜产',
            '盐业',
            '石油',
            '矿产',
            '租赁',
            '纺织',
            '经济贸易',
            '自来水',
            '航空',
            '证券',
            '贸易',
            '轻工业',
            '运输',
            '造船',
            '金属',
            '铁路',
            '食品'
        ]
        self.label_transfer = {
            "注册资金":"register_capital",
            "法定代表人":"legal_representative",
            "注册日期":"foundation_date",
            "注册地址":"address",
            "地区":"city",
            "统一社会信用代码":"company_ID",
            "经营范围":"industry",
            "企业名称":"company_name"
        }
        parser = argparse.ArgumentParser(description='import breakpoint data to restart')
        parser.add_argument('-region', type=str, default='beijing', help='provide region_name')
        parser.add_argument('-industry', type=str, default='五金', help='must be CN')
        parser.add_argument('-page', type=int, default=0, help='provide page num')
        args = parser.parse_args()
        self.region_name = args.region
        self.industry_name = args.industry
        self.page = args.page

    def get_company_list(self, region_name, industry_name):
        def process_company(html):
            soup = BeautifulSoup(html,'lxml')
            companies = soup.select('span[class="field-content"] a')
            if companies != []:
                for i in companies:
                    company_name = re.sub(r'<[^>]+>','',str(i))
                    company_name = re.sub(r'\s','',str(company_name))
                    company_link = self.link_company.format(region_name, quote('name/' + company_name))
                    self.save_company(region_name, company_name, company_link, industry_name)
                return 0
            else:
                return 1

        def process_industry_list(html):
            soup = BeautifulSoup(html, 'lxml')
            industries = soup.select('div[class="block block-gongshang-mingluji-com"] div a')
            info_list = [re.sub(r'<[^>]+>','',str(i)) for i in industries]
            print(info_list[:31])
            print(info_list[31:])

        for i in range(self.page, self.page_range):
            link = self.link.format(region_name, quote(industry_name, 'utf-8'), i)
            print('region {} industry {} on page {}'.format(region_name, industry_name, i+1))
            try:
                self.get_random_ip()
                self.header["User-Agent"] = self.random_select_header(self.usrAgent)
                proxy_support = urllib.request.ProxyHandler(self.proxy)
                opener = urllib.request.build_opener(proxy_support)
                urllib.request.install_opener(opener)
                request = urllib.request.Request(url=link, headers=self.header)
                response = urllib.request.urlopen(request, timeout=60)
            except:
                try:
                    self.get_random_ip()
                    self.header["User-Agent"] = self.random_select_header(self.usrAgent)
                    proxy_support = urllib.request.ProxyHandler(self.proxy)
                    opener = urllib.request.build_opener(proxy_support)
                    urllib.request.install_opener(opener)
                    request = urllib.request.Request(url=link, headers=self.header)
                    response = urllib.request.urlopen(request, timeout=60)
                except:
                    try:
                        self.get_random_ip()
                        self.header["User-Agent"] = self.random_select_header(self.usrAgent)
                        proxy_support = urllib.request.ProxyHandler(self.proxy)
                        opener = urllib.request.build_opener(proxy_support)
                        urllib.request.install_opener(opener)
                        request = urllib.request.Request(url=link, headers=self.header)
                        response = urllib.request.urlopen(request, timeout=60)
                    except:
                        response = []

            html = str(response.read(), 'utf-8')

            flag = process_company(html)
            if flag == 1:
                return flag
            else:
                pass


    def get_company(self, collection_name, _id, company_link):
        def process_company(html):
            soup = BeautifulSoup(html,'lxml')
            labels = soup.select('ul span[class="field-label"]')
            values = soup.select('ul span[class="field-item"]')
            if values != []:
                labels = [re.sub(r'<[^>]+>', '', str(i)) for i in labels]
                values = [re.sub(r'<[^>]+>', '', str(i)) for i in values]
                labels = [i for i in labels if '付费' not in i]
                values = [i for i in values if '付费' not in i]
                limit = len(labels)
                values = values[:limit]
                values = [re.sub(r'\([^>]+\)','',str(i)) for i in values]
                values = [re.sub(r'\s', '', str(i)) for i in values]
                labels = [self.label_transfer[i] for i in labels]
                data = list(zip(labels, values))
                data = {i[0]:i[1] for i in data}
                self.update_company(collection_name, _id, data)
            else:
                return 1

        self.get_random_ip()
        self.header["User-Agent"] = self.random_select_header(self.usrAgent)
        proxy_support = urllib.request.ProxyHandler(self.proxy)
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        request = urllib.request.Request(url=company_link, headers=self.header)
        response = urllib.request.urlopen(request, timeout=60)
        html = str(response.read(), 'utf-8')
        flag = process_company(html)
        if flag == 1:
            return 1
        else:
            pass

    def save_company(self, collection_name, company_name, company_link, industry):
        _db = self.db[collection_name]
        exists = _db.find({"company_name":company_name}).count()
        if exists == 0:
            _db.insert({
                "company_name":company_name,
                "company_link":company_link,
                "industry_parent":industry,
            })
            print('company {} is saved in collection {}'.format(company_name, collection_name))
        else:
            print('company {} exists in collection {}'.format(company_name, collection_name))

    def work_flow(self, collection_name):
        _db = self.db[collection_name]
        companies = _db.find({"industry":{"$exists":False}})
        for i in companies:
            _id = i["_id"]
            link = i["company_link"]
            try:
                self.get_company(collection_name, _id, link)
            except:
                pass

    def update_company(self, collection_name,  _id, data):
        _db = self.db[collection_name]
        _db.update({"_id": ObjectId(_id)}, {"$set": data}, True, True)
        print('company {} is update'.format(data["company_name"]))

    def work_flow_link(self):
        region_name = self.region_name
        industry_name = self.industry_name
        self.page = self.page
        flag = self.get_company_list(region_name, industry_name)
        if flag == 1:
            progress = self.industry_list.index(industry_name) + 1
            undo_list = self.industry_list[progress:]
            self.page = 0
            for m in undo_list:
                self.get_company_list(region_name, m)
            region_progress = self.region_list.index(region_name) + 1
            undo_region_list = self.region_list[region_progress:]
            for x in undo_region_list:
                for y in self.industry_list:
                    self.page = 0
                    self.get_company_list(x, y)

    def save_progress(self, region_name, industry_name, page):
        f = open('progress.txt','w')
        f.write(region_name + '\n' + industry_name + '\n' + str(page))
        f.close()

    def read_progress(self):
        f = open('progress.txt').readlines()
        data_list = [re.sub(r'\n','',str(i)) for i in f]
        return data_list

    def save_industry_progress(self, industry_name):
        f = open('industry.txt','a')
        f.write(str('\n'+industry_name+'\n'))
        f.close()

    def read_industry_progress(self):
        f = open('industry.txt').readlines()
        industry_list = [re.sub(r'\n','',str(i)) for i in f]
        return industry_list

if __name__ == '__main__':
    C = Company_spider()
    C.work_flow_link()