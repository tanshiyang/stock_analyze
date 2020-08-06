import os
import sys

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

import difflib
import re, time, datetime
import pandas as pd
import mytusharepro
import my_email.sendmail as sendmail
from collections import deque
import NEWS.site_news_jrj as jrj
import NEWS.site_news_stockstar as stockstar
import NEWS.site_news_cfi as cfi
import NEWS.site_news_stcn as stcn
import NEWS.site_news_jfinfo as jfinfo
from NEWS.model import News, Newskeyword
from NEWS.news_db import save_news_to_db, get_news_keywords

pro = mytusharepro.MyTusharePro()
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

data_path = os.path.join(curPath, 'data')


def my_decode(keyword):
    return keyword.replace("%2C", ",")


def mark_content_replace(match_obj):
    return "<b>" + match_obj.group(0) + "</b>"


def mark_content(content=str, keywords_list=[]):
    for keywords in keywords_list:
        for keyword in keywords.split(","):
            content = re.sub(my_decode(keyword), mark_content_replace, content)
            # content = content.replace(keyword, "<b>" + keyword + "</b>")
    return content


def send_news_mail(result_df=pd.DataFrame):
    mail_content = ""
    for news_index, news_row in result_df.iterrows():
        mail_content += "时间：{0}<br/>".format(news_row["date_time"])
        mail_content += "关键字：{0}<br/>".format(news_row["keywords"])
        mail_content += "内容：{0}<br/>".format(news_row["content"])
        mail_content += "来源：{0}<br/>".format(news_row["src"])
        mail_content += "<p/>"
    sendmail.send_news_mail(mail_content)


def save_to_csv(temp_df=pd.DataFrame):
    today = time.strftime('%Y%m%d')
    output_file_name = os.path.join(data_path, "result", "focus_news_{0}.csv".format(today))
    temp_df.to_csv(output_file_name, mode='a', header=False, encoding="utf_8_sig")


def check_contains_keywords(content, keywords):
    for keyword in keywords.split(","):
        try:
            if keyword.startswith("^"):
                if re.search(my_decode(keyword.replace("^", "")), content):
                    return False
            else:
                if not re.search(my_decode(keyword), content):  # keyword not in content:
                    return False
        except Exception as e:
            print('正则匹配错误,{0}'.format(e))
            print(content)
            print(keyword)
            return False

    return True


def get_last_news_time():
    now_time = datetime.datetime.now()
    now_hour = time.strftime('%H')
    last_time = (now_time + datetime.timedelta(days=-1)).strftime("%Y-%m-%d 15:00:00")
    if now_hour >= '15':
        last_time = now_time.strftime("%Y-%m-%d 15:00:00")

    today = time.strftime('%Y%m%d')
    output_file_name = os.path.join(data_path, "result", "focus_news_{0}.csv".format(today))
    if os.path.exists(output_file_name):
        columns = ["date_time", "keywords", "keywords_group", "content", "src"]
        df = pd.read_csv(output_file_name, encoding="utf_8_sig", names=columns)
        if len(df) > 0:
            return str(pd.to_datetime(df.date_time).max())
    return last_time


class NewsMonitor:
    def __init__(self):
        self.recent_news_deque = deque(maxlen=50)
        last_news_time = get_last_news_time()
        self.last_save_time = time.perf_counter()
        self.news_src = {'sina': last_news_time, 'wallstreetcn': last_news_time,
                         '10jqka': last_news_time, 'eastmoney': last_news_time,
                         'yuncaijing': last_news_time, 'jrj': last_news_time,
                         'stockstar': last_news_time, 'cfi': last_news_time,
                         'stcn': last_news_time, 'stcn': last_news_time,
                         'jfinfo': last_news_time, 'jfinfo': last_news_time,
                         }
        self.result_df = pd.DataFrame(columns=["date_time", "keywords", "keywords_group", "content", "src"])
        self.focus_keywords: list[Newskeyword] = pd.DataFrame()

    def monitor(self):
        while True:
            self.focus_keywords = get_news_keywords()  # pd.read_csv(file_name, encoding="gbk")
            for src in self.news_src.keys():
                start_date = self.news_src[src]
                end_date = time.strftime('%Y-%m-%d %H:%M:%S')
                print("news: {0}，{1},{2}".format(start_date, end_date, src))
                if src == "jrj":
                    news_df = jrj.News().get_news(start_date, end_date)
                elif src == "stockstar":
                    news_df = stockstar.News().get_news(start_date, end_date)
                elif src == "cfi":
                    news_df = cfi.News().get_news(start_date, end_date)
                elif src == "stcn":
                    news_df = stcn.News().get_news(start_date, end_date)
                elif src == "jfinfo":
                    news_df = jfinfo.News().get_news(start_date, end_date)
                else:
                    news_df = pro.news(src=src, start_date=start_date, end_date=end_date)
                self.check_news(src, news_df)

            now_hour = time.strftime('%H')
            time_span = 60
            if '15' <= now_hour <= '24':
                time_span = 60 * 60 * 2
            if '00' <= now_hour <= '07':
                time_span = 60 * 60 * 8

            if len(self.result_df) > 0 and time.perf_counter() - self.last_save_time > time_span:
                try:
                    send_news_mail(self.result_df)
                    self.result_df.drop(index=self.result_df.index, inplace=True)
                    self.last_save_time = time.perf_counter()
                except Exception as e:
                    print('str(e):\t', str(e))
            time.sleep(30)

    def check_news(self, src=str, news_df=pd.DataFrame):
        for news_index, news_row in news_df.iterrows():
            date_time = news_row["datetime"]
            content = news_row["content"]

            matched_keywords = []
            matched_keywords_group = []
            matched_keywords_level = []
            for row in self.focus_keywords:
                keywords = row.KeywordRule
                keywords_group = row.KeywordGroup
                keywords_level = row.KeywordLevel
                if check_contains_keywords(content, keywords):
                    matched_keywords.append(keywords)
                    matched_keywords_group.append(keywords_group)
                    matched_keywords_level.append(keywords_level)
            if len(matched_keywords) > 0:
                next_time = datetime.datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S') + \
                            datetime.timedelta(seconds=1)
                if self.news_src[src] < next_time.strftime('%Y-%m-%d %H:%M:%S'):
                    self.news_src[src] = next_time.strftime('%Y-%m-%d %H:%M:%S')
                # 判断重复性
                if self.check_duplicate(content):
                    continue
                self.recent_news_deque.append(content)
                dict = {}
                dict["date_time"] = date_time
                dict["keywords"] = matched_keywords
                dict["keywords_group"] = matched_keywords_group
                dict["content"] = mark_content(content, matched_keywords)
                dict["src"] = src
                print(dict)
                temp_df = pd.DataFrame(columns=["date_time", "keywords", "keywords_group", "content", "src"])
                temp_df = temp_df.append(dict, ignore_index=True)
                save_news_to_db(temp_df)
                save_to_csv(temp_df)
                if 1 in matched_keywords_level:
                    self.result_df = self.result_df.append(dict, ignore_index=True)

    def check_duplicate(self, content: str):
        for recent_news in self.recent_news_deque:
            if difflib.SequenceMatcher(None, recent_news, content).quick_ratio() >= 0.7:
                print("skip:{0}".format(content))
                return True
        return False


if __name__ == '__main__':
    monitor = NewsMonitor()
    monitor.monitor()
    print()
