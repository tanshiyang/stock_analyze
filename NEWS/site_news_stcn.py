import urllib.request as urllibreq
import re
from bs4 import BeautifulSoup
import pandas as pd
import os
import time
from util.dispacher import Dispacher


class News:

    def get_html(self, url):
        try:
            head = {}
            head['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0'
            req = urllibreq.Request(url, headers=head)
            response = urllibreq.urlopen(req)
            html = response.read()
            html = html.decode('utf-8')
            return html
        except Exception as e:
            print(url)
            print(e)
            return ""

    def get_html_with_timeout(self, url):
        c = Dispacher(self.get_html, url)
        c.join(5)

        if c.isAlive():
            return ""
        elif c.error:
            return c.error[1]
        return c.result

    def get_news(self, start_date, end_date):
        result_df = pd.DataFrame()
        try:
            gsdt = self.get_gsdt_news(start_date, end_date)
            gsxw = self.get_gsxw_news(start_date, end_date)
            result_df = pd.concat([result_df, gsdt, gsxw])
        except Exception as e:
            print("获取新闻出错。")
        return result_df

    # 公司动态
    def get_gsdt_news(self, start_date, end_date):
        result_df = pd.DataFrame()
        start_time = time.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_time = time.strptime(end_date, "%Y-%m-%d %H:%M:%S")
        pages = ["1"]
        pages = ["", "1", "2", "3", "4", "5"]
        found_page_data = False
        for page in pages:
            if page != "":
                page = "_" + page
            url = time.strftime('https://company.stcn.com/gsdt/index{0}.html'.format(page))
            html = self.get_html_with_timeout(url)
            if html == '':
                break
            soup = BeautifulSoup(html, 'html.parser')
            li = soup.select('div[class="content clearfix"]>div>ul>li')
            for child in li:
                if len(child.select("a")) == 0:
                    continue
                news = {}
                news["content"] = news["title"] = child.select("a")[1].getText()
                date_time_text = child.select("span")[0].getText()
                date_time_text = date_time_text.replace("\n","").replace("\t","")
                time_text = child.select("span>i")[0].getText()
                date_text = date_time_text.replace(time_text,"")
                news["datetime"] = "{0} {1}:00".format(date_text, time_text)
                news["channels"] = ""
                # print(news)
                if news["title"] == "":
                    continue
                found_page_data = True
                if start_date <= news["datetime"] <= end_date:
                    result_df = result_df.append(news, ignore_index=True)
        if not found_page_data:
            print("未获取到公司动态。")
        return result_df

    # 公司新闻
    def get_gsxw_news(self, start_date, end_date):
        result_df = pd.DataFrame()
        start_time = time.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_time = time.strptime(end_date, "%Y-%m-%d %H:%M:%S")
        pages = ["1"]
        pages = ["", "1", "2", "3", "4", "5"]
        found_page_data = False
        for page in pages:
            if page != "":
                page = "_" + page
            url = time.strftime('https://company.stcn.com/gsxw/index{0}.html'.format(page))
            html = self.get_html_with_timeout(url)
            if html == '':
                break
            soup = BeautifulSoup(html, 'html.parser')
            li = soup.select('div[class="content clearfix"]>div>ul>li')
            for child in li:
                if len(child.select("a")) == 0:
                    continue
                news = {}
                news["content"] = news["title"] = child.select("a")[1].getText()
                date_time_text = child.select("span")[0].getText()
                date_time_text = date_time_text.replace("\n", "").replace("\t", "")
                time_text = child.select("span>i")[0].getText()
                date_text = date_time_text.replace(time_text, "")
                news["datetime"] = "{0} {1}:00".format(date_text, time_text)
                news["channels"] = ""
                # print(news)
                if news["title"] == "":
                    continue
                found_page_data = True
                if start_date <= news["datetime"] <= end_date:
                    result_df = result_df.append(news, ignore_index=True)
        if not found_page_data:
            print("未获取到公司新闻。")
        return result_df


if __name__ == '__main__':
    obj = News()
    df = obj.get_news('2019-01-17 20:58:32', '2022-01-17 20:58:32')
    print(df)
