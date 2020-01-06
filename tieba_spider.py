# coding=utf-8
from spider import Spider
import config
import re
from bs4 import BeautifulSoup
import requests
from urllib.parse import unquote

class Tieba(Spider):
    def __init__(self,tieba_name):
        super(Tieba, self).__init__()
        self.link ="https://tieba.baidu.com/f?kw=" + tieba_name + "&ie=utf-8&pn={}"
        self.header = {

        }

    def format_control(self, str):
        string = re.sub(r'"','',str)
        return string

    def tieba(self):
        def process_tieba(html):
            titles = re.findall(r'<a rel="noreferrer" [^>]+>',html)

            for i in titles:
                data_dict = {}
                if "j_th_tit" in i:
                    title = re.findall(r'title="[^>]+"', str(i))[0]
                    link = re.findall(r'href="[^>]+"', str(i))[0]
                    title = title.split(' ')[0]
                    link = link.split(' ')[0]
                    title = re.sub(r'title=','',str(title))
                    link = re.sub(r'href=','',str(link))
                    title = self.format_control(title)
                    link = self.format_control(link)
                    link = 'http://tieba.baidu.com' + link

                elif "frs-author-name" in i:
                    author = re.findall(r'href="[^>]+"', str(i))[0]
                    author_main_page = author.split(' ')[0]
                    author_name = re.findall(r'un=[^>]+&ie', str(author_main_page))
                    if author_name != []:
                        author_name = author_name[0]
                        author_name = re.sub(r'un=','',str(author_name))
                        author_name = re.sub(r'&ie','',str(author_name))
                        author_name = unquote(author_name, encoding='utf-8')
                    author_main_page = 'http://tieba.baidu.com' + self.format_control(author_main_page)
                try:
                    data_dict["title"] = title
                    data_dict["link"] = link
                    data_dict["author_main_page"] = author_main_page
                    data_dict["author"] = author_name
                    self.save_thread('tieba',data_dict)
                except:
                    pass

        for i in range(0, 100):
            self.get_random_ip()
            self.header["User-Agent"] = self.random_select_header(self.usrAgent)
            response = requests.get(self.link.format(i*50), headers=self.header, proxies = self.proxy)
            html = str(response.content.decode('utf-8'))
            process_tieba(html)
            break

    def save_thread(self, collection_name, data):
        exist = self.db[collection_name].find({"title": data["title"]}).count()
        if exist == 0:
            self.db[collection_name].insert(data)
            print('news {} is saved'.format(data["title"]))
        else:
            print('news {} exists'.format(data["title"]))

if __name__ == '__main__':
    tieba = Tieba('')
    tieba.tieba()