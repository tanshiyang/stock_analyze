import urllib
import re
from bs4 import BeautifulSoup
import pandas as pd
import os
import time


class News:

    def get_html(self, url):
        try:
            html = urllib.request.urlopen(url).read()
            html = html.decode('gbk')
            return html
        except Exception as e:
            return ""

    def get_news(self, start_date, end_date):
        result_df = pd.DataFrame()
        try:
            gsxw = self.get_gsxw_news(start_date, end_date)
            result_df = pd.concat([result_df, gsxw])
        except Exception as e:
            print("获取证券之星新闻出错。")
        return result_df

    def get_gsxw_news(self, start_date, end_date):
        result_df = pd.DataFrame()
        start_time = time.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_time = time.strptime(end_date, "%Y-%m-%d %H:%M:%S")
        pages = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]
        for page in pages:
            url = time.strftime('http://stock.stockstar.com/list/10_{0}.shtml'.format(page))
            html = self.get_html(url)
            if html == '':
                break
            soup = BeautifulSoup(html, 'html.parser')
            ul = soup.select('div[class="listnews"] > ul > li')
            for child in ul:
                if len(child.select("a")) == 0:
                    continue
                news = {}
                news["content"] = news["title"] = child.select("a")[0].getText()
                news["datetime"] = child.select("span")[0].getText().replace("  ", " ")
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
