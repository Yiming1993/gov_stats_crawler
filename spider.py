import config
from pymongo import MongoClient
import datetime
import Levenshtein as lst
import re
import random
import urllib.request
from bs4 import BeautifulSoup
from bson import ObjectId

class Spider(object):
    def __init__(self):
        # print(self.db)
        self.origin_path = config.origin_path()
        self.usrAgent = [
            "Mozilla/5.0 (iPad; CPU OS 5_0 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9A334 Safari/7534.48.3",
            "Mozilla/5.0 (iPod; U; CPU like Mac OS X; en) AppleWebKit/420.1 (KHTML, like Gecko) Version/3.0 Mobile/3A101a Safari/419.3",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.1 Safari/605.1.15",
        ]
        self.proxy_path = self.origin_path + '/References/proxy.txt'
        self.today = str(datetime.datetime.now())[:10]
        self.header = {}
        self.db = config.db_path('xiamen')

    def cruiser_to_list(self, cruiser, keyname):
        result_list = [doc[str(keyname)] for doc in cruiser]
        return (result_list)

    def id_generator(self, list, value_keyname):
        valuelist = []
        for i in list:
            id = self.cruiser_to_list(self.db.NEWS.find({"title": str(i)}), value_keyname)
            valuelist.append(id[0])
        return valuelist

    def lst_result_generator(self, available, target_news):
        if lst.ratio(str(available.split()), str(target_news.split())) > 0.58:
            return str(available)
        else:
            return None

    def duplicate_id_generator(self, target_news, collection):
        simarticles = []
        for i in collection:
            result = self.lst_result_generator(str(i), target_news)
            if result != None:
                simarticles.append(result)

        if simarticles != []:
            simId = self.id_generator(simarticles, '_id')
            return simId
        else:
            return None

    def exist_detecter(self, newstitle):
        exist = self.db.NEWS.find({"title": newstitle}).count()
        if exist != 0:
            return 0
        else:
            return 1

    def random_select_header(self,list):
        agent = random.choice(list)
        return agent

    def get_ip_list(self,obj):
        '''
        从网页数据中提取代理地址
        :param obj: 免费代理地址的网页爬取response
        :return: 代理IP列表
        '''
        ip_text = obj.findAll('tr', {'class': 'odd'}) #在tr的tag中，class=odd
        ip_list = []
        for i in range(len(ip_text)):
            ip_tag = ip_text[i].findAll('td')
            ip_port = ip_tag[1].get_text() + ':' + ip_tag[2].get_text() #把IP和port联合起来
            ip_list.append(ip_port)
        # print("共收集到了{}个代理IP".format(len(ip_list)))
        # print(ip_list)
        return ip_list #爬取到的代理IP列表

    def get_random_ip(self):
        '''
        随机从代理IP文本中提取一个
        :return: 一个随机的代理IP
        '''
        ip_ = open(self.proxy_path, 'r').readlines() #打开代理IP文件，在项目的Reference文件夹中
        ip_list = [re.sub(r'\n','',str(i)) for i in ip_] #重构列表
        random_ip = 'http://' + random.choice(ip_list) #随机选择一个
        self.proxy = {'http:': random_ip} #构造成字典的形式，给urllib使用
        # print('check point: get_proxy')

    def get_proxy(self):
        '''
        only run once for a day, save a self.bsObjct for proxy pool
        用于从代理ip网页爬取代理ip
        :return:
        '''
        url = 'http://www.xicidaili.com/nn'
        headers = {}
        headers["User-Agent"] = self.random_select_header(self.usrAgent)
        headers["Upgrade-Insecure-Requests"] = 1
        headers["Accept-Language"] = 'zh-cn'
        headers["Connection"] = 'keep-alive'
        headers["Accept"] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8' #构建伪浏览器请求头
        headers["Host"] = 'www.xicidaili.com'
        headers["Referer"] = 'www.xicidaili.com'
        proxy_support = urllib.request.ProxyHandler(self.proxy)
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        request = urllib.request.Request(url,headers=headers)
        response = urllib.request.urlopen(request)
        bsObj = BeautifulSoup(response, 'lxml')
        return bsObj #网页数据

    def save_proxy(self):
        '''
        用于保存代理ip文件，这个程序可以定期启动一次，更新ip
        :return:
        '''
        self.get_random_ip()
        bsObj = self.get_proxy()
        ip_list = self.get_ip_list(bsObj)
        for i in ip_list:
            f = open(self.origin_path + '/References/proxy.txt','a')
            f.write(str(i) + '\n')
            f.close()
        print('proxy is saved')

    def save_article(self, title, time, link, source, read_count, class_):
        '''
        用于保存新闻数据
        :param title:
        :param time:
        :param link:
        :param source:
        :param read_count: 来自头条的阅读量统计数据，没有数据的则没有该键值
        :param class_: 来自的新闻渠道
        :return:
        '''
        if self.exist_detecter(title) == 1:
            # 遍历已有新闻
            self.availables = self.cruiser_to_list(self.db.NEWS.find({'collect': True}), "title")
            simId = self.duplicate_id_generator(title, self.availables) # 统计重复新闻
            del self.availables
            # 如果没有重复新闻，则不存sim_news
            if simId == None:
                if read_count == 0:
                    # 如果没有阅读量数据，则不存该键
                    self.db.NEWS.insert({"title":title,
                                         "link":link,
                                         "source":source,
                                         "date":time,
                                         "coll_date":self.today,
                                         "screen":False,
                                         "collect":False,
                                         "class":class_})
                else:
                    self.db.NEWS.insert({"title": title,
                                         "link": link,
                                         "source": source,
                                         "date": time,
                                         "coll_date": self.today,
                                         "read_count": read_count,
                                         "screen": False,
                                         "collect": False,
                                         "class": class_})
                print('news {} is saved'.format(title))
            else:
                if read_count == 0:
                    self.db.NEWS.insert({"title":title,
                                         "link":link,
                                         "source":source,
                                         "date":time,
                                         "coll_date":self.today,
                                         "screen":False,
                                         "collect":False,
                                         "class":class_,
                                         "sim_news":simId})
                else:
                    self.db.NEWS.insert({"title": title,
                                         "link": link,
                                         "source": source,
                                         "date": time,
                                         "coll_date": self.today,
                                         "read_count": read_count,
                                         "screen": False,
                                         "collect": False,
                                         "class": class_,
                                         "sim_news": simId})
                print('news {} has a similar news'.format(title))
        else:
            print('news {} exists'.format(title))

    def update_article(self, link, content):
        article = self.db.POLICY.find({"$and":[{"content":{"$exists":False}},{"link":link}]})
        _id = [i["_id"] for i in article][0]
        self.db.POLICY.update({"_id":ObjectId(_id)},{"$set":{"content":content}},True,True)
        print('article {} is updated'.format(link))

    def key_name_change(self, collection_name, old_keyname, new_keyname):
        data = self.db[str(collection_name)].find({str(old_keyname):{"$exists":True}})
        for doc in data:
            Id = doc["_id"]
            self.db[str(collection_name)].update({"_id":ObjectId(Id)},{"$rename":{old_keyname:new_keyname}})
            print('data {} changed old key {} to {}'.format(Id, old_keyname, new_keyname))

    def update_info(self, collection_name, _id, tag_name, data):
        self.db[str(collection_name)].update({"_id": ObjectId(_id)}, {"$set": {str(tag_name): data}}, True, True)
        print('Object {} is updated with tag {} and data {}'.format(_id, tag_name, data))

    def build_Cookie(self, link):
        set_cookie = urllib.request.urlopen(link).info()['Set-Cookie']
        print(set_cookie)
        json_id = set_cookie.split(';')[0]
        json_id = json_id.split('=')[-1]
        return json_id

    def format_control(self, sentence):
        sentence = re.sub(r'\s', '', str(sentence))
        return re.sub(r'<[^>]+>', '', str(sentence))

if __name__ == '__main__':
    S = Spider()
    S.save_proxy()