import config
from spider import Spider
from bs4 import BeautifulSoup
import urllib.request
import re
from urllib.request import urlretrieve
import os

class Statistic_download(Spider):
    def __init__(self):
        super(Statistic_download, self).__init__()
        self.origin_path, _ = config.origin_path()
        self.save_path = self.origin_path + '/files'
        self.header["Cookie"] = ''
        self.header["Connection"] = 'keep-alive'

    def get_mainpage(self, link, save_dir_name):
        def downloader(html):
            soup = BeautifulSoup(html, 'html5lib')
            files = soup.select('ul ul li a')

            os.makedirs(self.save_path + '/' + save_dir_name, exist_ok=True)
            for file in files:
                file_name = re.sub(r'<[^>]+>', '', str(file))
                if ' ' in file_name:
                    file_index = file_name.split(' ')[0]
                    file_name = file_name.split(' ')[1]
                    file_path = re.findall(r'href="[^>]+"',str(file))[0]
                    file_path = re.sub(r'href="','',str(file_path))
                    file_path = re.sub(r'"','',str(file_path))
                    file_path = '/'.join(link.split('/')[0:-1]) + '/' + file_path
                    if 'xls' in file_path:

                        urlretrieve(file_path, self.save_path + '/' + save_dir_name + '/' + file_path.split('/')[-1])
                        print('file {} is saved'.format(file_name))

        self.get_random_ip()
        self.header["User-Agent"] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/12.0.2 Safari/605.1.15'
        proxy_support = urllib.request.ProxyHandler(self.proxy)
        opener = urllib.request.build_opener(proxy_support)
        urllib.request.install_opener(opener)
        request = urllib.request.Request(url=link, headers=self.header)
        response = urllib.request.urlopen(request, timeout=60)
        html = str(response.read(), 'gbk')
        downloader(html)

if __name__ == '__main__':
    S = Statistic_download()
    S.get_mainpage()