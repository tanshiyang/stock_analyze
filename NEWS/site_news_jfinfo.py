import urllib.request as urllibreq
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
            req = urllibreq.Request(url, headers=head)
            response = urllibreq.urlopen(req)
            html = response.read()
            html = html.decode('utf-8')
            return html
        except Exception as e:
            print(url)
            print(e)
            return ""

    def get_news(self, start_date, end_date):
        result_df = pd.DataFrame()
        try:
            jgyc = self.get_jgyc(start_date, end_date)
            result_df = pd.concat([result_df, jgyc])
        except Exception as e:
            print("获取巨丰资讯出错。")
        return result_df

    # 金股预测
    def get_jgyc(self, start_date, end_date):
        result_df = pd.DataFrame()
        start_time = time.strptime(start_date, "%Y-%m-%d %H:%M:%S")
        end_time = time.strptime(end_date, "%Y-%m-%d %H:%M:%S")
        found_page_data = False
        url = time.strftime('http://www.jfinfo.com/reference/jgyc')
        html = self.get_html(url)
        soup = BeautifulSoup(html, 'html.parser')
        childrens = soup.select('div[class="p-content"]>div>div>div:nth-of-type(1)>dl')
        for child in childrens:
            news = {}
            href = child.select("a")[0]["href"]
            title = child.select("a")[0].text
            detail_html = self.get_html(href)
            detail_soap = BeautifulSoup(detail_html, 'html.parser')
            div_context = detail_soap.select('div[class="t-context f16 picture"]')
            if len(div_context) > 0:
                news["content"] = div_context[0].prettify()
            div_date_time = detail_soap.select('div[class="t-tit"]>span')
            if len(div_date_time) > 0:
                news["datetime"] = "{0}:00".format(div_date_time[0].text)
            news["title"] = title
            news["channels"] = ""
            # print(news)
            if news["title"] == "":
                continue
            found_page_data = True
            if start_date <= news["datetime"] <= end_date:
                result_df = result_df.append(news, ignore_index=True)
            print(result_df)
        if not found_page_data:
            print("未获取到金股预测。")
        return result_df


if __name__ == '__main__':
    obj = News()
    obj.get_news('2019-01-17 20:58:32', '2022-01-17 20:58:32')
    time.sleep(5)
