import urllib.request as urllibreq
import re
import socket
from bs4 import BeautifulSoup
import pandas as pd
import os
import time


class News:

    def get_html(self, url):
        try:
            html = urllibreq.request.urlopen(url).read()
            html = html.decode('gbk')
            return html
        except Exception as e:
            return ""


    def get_html2(self, url):
        try:
            head = {}
            head['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0'
            req = urllibreq.Request(url, headers=head)
            response = urllibreq.urlopen(req)
            html = response.read()
            html = html.decode('utf-8')
            return html
        except Exception as e:
            print(e)
            return ''

    def get_news(self, start_date, end_date):
        result_df = pd.DataFrame()
        try:
            gsxw = self.get_gsxw_news(start_date, end_date)
            result_df = pd.concat([result_df, gsxw])
        except Exception as e:
            print("获取证券之星新闻出错。")
        return result_df

    def hexun_test(self, start_date, end_date):
        page = 1
        url = time.strftime('http://open.tool.hexun.com/MongodbNewsService/newsListPageByJson.jsp?id=100235849&s=30&cp={0}&priority=0&callback=hx_json31585212241405'.format(page))
        html = self.get_html2(url)
        print(html)

    def cfi_test(self, start_date, end_date):
        page = 1
        url = time.strftime('http://stock.cfi.cn/index.aspx?catid=A0A4127A4346A4439&dycatid=&pagepara='.format(page))
        html = self.get_html2(url)
        print("html:" + html)

    def stcn_test(self, start_date, end_date):
        page = 1
        url = time.strftime('http://company.stcn.com/gsdt/index.html'.format(page))
        html = self.get_html2(url)
        print("html:" + html)


if __name__ == '__main__':
    obj = News()
    obj.stcn_test('2019-01-17 20:58:32', '2022-01-17 20:58:32')
    time.sleep(5)
