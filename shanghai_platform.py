import config
from spider import Spider
from bs4 import BeautifulSoup
import re
import urllib.request
from bson import ObjectId

class Shanghai(Spider):
    def __init__(self):
        super(Shanghai, self).__init__()
        self.origin_path, self.db = config.origin_path()
        self.link = 'http://datashanghai.gov.cn/query!queryDataByField.action?currentPage={}&dataField={}'
        self.industry = ""
        self.industry_cata = {
            "1":'经济建设',
            "2":'资源环境',
            "3":'教育科技',
            "4":'道路交通',
            "5":'社会发展',
            "6":'公共安全',
            "7":'文化休闲',
            "8":'卫生健康',
            "9":'民生服务',
            "10":'机构团体',
            "11":'城市建设',
            "12":'信用服务'
        }

    def shanghai_platform(self):
        def process_shanghai(html):
            soup = BeautifulSoup(html, 'lxml')
            datasets = soup.select('div[class="container clear"] dt a')
            descriptions = soup.select('div[class="container clear"] dd p')
            if datasets == []:
                return 0
            else:

                full_datasets = list(zip(datasets,descriptions))
                for i in full_datasets:
                    title = re.sub(r'<[^>]+>','',str(i[0]))
                    title = re.sub(r'\s+','',title)
                    link_info = re.findall(r'\([^>]+\)',str(i[0]))[0]
                    link_info = re.findall(r"'[^>]+'",link_info)[0]
                    link_ = link_info.split(",")
                    dataId = re.sub(r"'",'',str(link_[0]))
                    type_ = re.sub(r"'",'',str(link_[-1]))
                    link = 'http://datashanghai.gov.cn/query!queryGdsDataInfoById.action?type={}&dataId={}'.format(
                        type_,
                        dataId
                    )

                    description = re.sub(r'<[^>]+>','',str(i[1]))
                    description = re.sub(r'\s+','',description)
                    self.save_dataset(title, link, description)
                return 1

        for i in range(1, 13):
            self.industry = self.industry_cata[str(i)]
            for j in range(1, 50):
                self.url = self.link.format(j, i)
                self.get_random_ip()
                self.header["User-Agent"] = self.random_select_header(self.usrAgent)
                proxy_support = urllib.request.ProxyHandler(self.proxy)
                opener = urllib.request.build_opener(proxy_support)
                urllib.request.install_opener(opener)
                request = urllib.request.Request(url=self.url, headers=self.header)
                response = urllib.request.urlopen(request, timeout=60)
                html = str(response.read(), 'utf-8')
                flag = process_shanghai(html)
                if flag == 0:
                    break
                else:
                    pass


    def save_dataset(self, title, link, description):
        exists = self.db.shanghai.find({"link": link}).count()
        if exists == 0:
            self.db.shanghai.insert({
                "title": title,
                "link": link,
                "description": description,
                "coll_date": self.today,
                "group": self.industry
            })
            print('dataset {} is saved'.format(title))
        else:
            print('dataset {} exists'.format(title))

    def shanghai_detail(self, _id, link):
        def process_shanghai(html):
            soup = BeautifulSoup(html, 'lxml')
            key_list = soup.select('tr th')
            value_list = soup.select('tr td')
            info_list = list(zip(key_list, value_list))
            for key, value in info_list:
                key = re.sub(r'<[^>]+>', '', str(key))
                value = re.sub(r'<[^>]+>', '', str(value))
                value = re.sub(r'\s', '', str(value))

                if '关键字' in key:
                    key_words = re.split(r'[,，、]', value)
                    self.update_info('shanghai', _id, 'key_words', key_words)
                if '公开' in key:
                    public_type = value
                    self.update_info('shanghai', _id, 'public_type', public_type)
                if '数据提供方' in key:
                    org = value
                    self.update_info('shanghai', _id, 'org', org)
                if '发布日期' in key:
                    date = value[:10]
                    self.update_info('shanghai', _id, 'date', date)

        self.get_random_ip()
        self.header["User-Agent"] = self.random_select_header(self.usrAgent)
        proxy_support = urllib.request.ProxyHandler(self.proxy)
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        request = urllib.request.Request(url=link, headers=self.header)
        response = urllib.request.urlopen(request, timeout=60)
        html = str(response.read(), 'utf-8')
        try:
            process_shanghai(html)
        except:
            pass

if __name__ == '__main__':
    S = Shanghai()
    links = S.db.shanghai.find({})
    for i in links:
        link = i["link"]
        _id = i["_id"]
        try:
            S.shanghai_detail(_id, link)
        except:
            pass