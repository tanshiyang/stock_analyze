import urllib
import re
import socket
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import time


class News:

    def get_html(self, url):
        try:
            socket.setdefaulttimeout(10)  # socket访问时延
            send_headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:74.0) Gecko/20100101 Firefox/74.0",
                "Connection": "keep-alive",
                "Cookie":"HexunTrack=SID=202003071638161465aaaef84cec446a59632dc71fa2c86a4&CITY=0&TOWN=0; UM_distinctid=170b4251f6862-0552e4dfa18dd8-4c302879-e1000-170b4251f69674; ADVC=38427cea694e8f; ASL=18347,000zq,df4a93b7df683f16; ADVS=385169e0639b02; __jsluid_h=380597701a8ff20d3a6d27ba36a2baa6; __utma=194262068.1731910817.1585211408.1585211408.1585211408.1; __utmb=194262068.5.10.1585211408; __utmc=194262068; __utmz=194262068.1585211408.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); cn_1263247791_dplus=%7B%22distinct_id%22%3A%20%22170b4251f6862-0552e4dfa18dd8-4c302879-e1000-170b4251f69674%22%2C%22userFirstDate%22%3A%20%2220200307%22%2C%22userID%22%3A%20%22%22%2C%22userName%22%3A%20%22%22%2C%22userType%22%3A%20%22nologinuser%22%2C%22userLoginDate%22%3A%20%2220200326%22%2C%22%24_sessionid%22%3A%200%2C%22%24_sessionTime%22%3A%201585213183%2C%22%24dp%22%3A%200%2C%22%24_sessionPVTime%22%3A%201585213183%2C%22initial_view_time%22%3A%20%221583565073%22%2C%22initial_referrer%22%3A%20%22https%3A%2F%2Fwww.baidu.com%2Flink%3Furl%3Dei2b65fm5zoRK1UY9myTzJdRT6OwQwFfad_fII1zPhDerhjv6cHvPUBY6ImSEP6TZ4opAhXKAw718Qcik1bhy_%26wd%3D%26eqid%3D94a5e5fd0000423c000000065e635d46%22%2C%22initial_referrer_domain%22%3A%20%22www.baidu.com%22%2C%22%24recent_outside_referrer%22%3A%20%22www.baidu.com%22%7D",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2"
            }  # 伪装成浏览器
            response = requests.get(url, send_headers)  # 访问
            response.close()  # 访问后就关闭访问
            html = response.text
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
            url = time.strftime('http://open.tool.hexun.com/MongodbNewsService/newsListPageByJson.jsp?id=100235849&s=30&cp={0}&priority=0&callback=hx_json31585212241405'.format(page))
            html = self.get_html(url)
            print(html)
            continue
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
    while True:
        obj = News()
        obj.get_news('2019-01-17 20:58:32', '2022-01-17 20:58:32')
        time.sleep(5)
