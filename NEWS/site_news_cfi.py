import urllib
import re
from bs4 import BeautifulSoup
import pandas as pd
import os
import time


class News:

    def get_html(self, url):
        try:
            head = {}
            head['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0'
            req = urllib.request.Request(url, headers=head)
            response = urllib.request.urlopen(req)
            html = response.read()
            html = html.decode('utf-8')
            return html
        except Exception as e:
            return ""

    def get_news(self, start_date, end_date):
        result_df = pd.DataFrame()
        try:
            gskd = self.get_gskd_news(start_date, end_date)
            if len(gskd) == 0:
                print("未获取到公司快递。")
            result_df = pd.concat([result_df, gskd])
        except Exception as e:
            print("获取新闻出错。")
        return result_df

    # 公司快递
    def get_gskd_news(self, start_date, end_date):
        result_df = pd.DataFrame()
        start_time = time.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_time = time.strptime(end_date, "%Y-%m-%d %H:%M:%S")
        pages = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]
        url = 'http://stock.cfi.cn/index.aspx?catid=A0A4127A4346A4439&dycatid=&pagepara='
        for page in pages:
            html = self.get_html(url)
            if html == '':
                break
            soup = BeautifulSoup(html, 'html.parser')
            links = soup.select('div[class="xinwen"] > a')
            for link in links:
                span = link.previous_sibling.previous_sibling
                news = {}
                news["content"] = news["title"] = link.getText()
                datetime = span.getText()
                today = time.strftime('%Y-%m-%d')
                year = time.strftime('%Y')
                if ":" in datetime:
                    datetime = today + " " + datetime + ":00"
                else:
                    datetime = year + datetime.replace("/", "-") + " 24:00:00"
                news["datetime"] = datetime
                news["channels"] = ""
                # print(news)
                if news["title"] == "":
                    continue
                if start_date <= news["datetime"] <= end_date:
                    result_df = result_df.append(news, ignore_index=True)
        return result_df


if __name__ == '__main__':
    obj = News()
    obj.get_news('2019-01-17 20:58:32', '2022-01-17 20:58:32')
    time.sleep(5)
