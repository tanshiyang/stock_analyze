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
            ssgs = self.get_ssgs_news(start_date, end_date)
            gszx = self.get_gszx_news(start_date, end_date)
            result_df = pd.concat([result_df, ssgs, gszx])
        except Exception as e:
            print("获取金融界新闻出错。")
        return result_df

    def get_ssgs_news(self, start_date, end_date):
        result_df = pd.DataFrame()
        start_time = time.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_time = time.strptime(end_date, "%Y-%m-%d %H:%M:%S")
        pages = ["", "2", "3", "4", "5", "6", "7", "8", "9", "10"]
        for page in pages:
            url = time.strftime('http://stock.jrj.com.cn/xwk/%Y%m/%Y%m%d_{0}.shtml'.format(page))
            if page != "":
                page = "-" + page
            url = time.strftime('http://stock.jrj.com.cn/list/stockssgs{0}.shtml'.format(page))
            html = self.get_html(url)
            if html == '':
                break
            soup = BeautifulSoup(html, 'html.parser')
            ul = soup.select('div[class="list-main"] > ul > li')
            for child in ul:
                if len(child.select("a")) == 0:
                    continue
                news = {}
                news["content"] = news["title"] = child.select("a")[0].getText()
                news["datetime"] = child.select("span")[0].getText().replace("  ", " ") + ":00"
                news["channels"] = ""
                # print(news)
                if news["title"] == "":
                    continue
                if start_date <= news["datetime"] <= end_date:
                    result_df = result_df.append(news, ignore_index=True)
        return result_df

    def get_gszx_news(self, start_date, end_date):
        result_df = pd.DataFrame()
        start_time = time.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_time = time.strptime(end_date, "%Y-%m-%d %H:%M:%S")
        pages = ["", "2", "3", "4", "5", "6", "7", "8", "9", "10"]
        for page in pages:
            if page != "":
                page = "-" + page
            url = time.strftime('http://stock.jrj.com.cn/list/stockgszx{0}.shtml'.format(page))
            html = self.get_html(url)
            if html == '':
                break
            soup = BeautifulSoup(html, 'html.parser')
            ul = soup.select('div[class="list-main"] > ul > li')
            for child in ul:
                if len(child.select("a")) == 0:
                    continue
                news = {}
                news["content"] = news["title"] = child.select("a")[0].getText()
                news["datetime"] = child.select("span")[0].getText().replace("  ", " ") + ":00"
                news["channels"] = ""
                # print(news)
                if news["title"] == "":
                    continue
                if start_date <= news["datetime"] <= end_date:
                    result_df = result_df.append(news, ignore_index=True)
        return result_df


if __name__ == '__main__':
    jrj = News()
    jrj.get_news('2019-01-17 20:58:32', '2022-01-17 20:58:32')
    time.sleep(5)
