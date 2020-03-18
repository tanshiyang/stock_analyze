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

pro = mytusharepro.MyTusharePro()
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

data_path = os.path.join(curPath, 'data')


def monitor(file_name):
    recent_news_deque = deque(maxlen=20)
    last_news_time = get_last_news_time()
    last_save_time = time.perf_counter()
    news_src = {'sina': last_news_time, 'wallstreetcn': last_news_time,
                '10jqka': last_news_time, 'eastmoney': last_news_time,
                'yuncaijing': last_news_time}
    result_df = pd.DataFrame(columns=["date_time", "keywords", "content", "src"])

    while True:
        focus_keyword_df = pd.read_csv(file_name, encoding="gbk")
        for src in news_src.keys():
            start_date = news_src[src]
            end_date = time.strftime('%Y-%m-%d %H:%M:%S')
            print("news: {0}，{1},{2}".format(start_date, end_date, src))
            news_df = pro.news(src=src, start_date=start_date, end_date=end_date)
            for news_index, news_row in news_df.iterrows():
                date_time = news_row["datetime"]
                content = news_row["content"]

                matched_keywords = []
                for keywords_index, keywords_row in focus_keyword_df.iterrows():
                    keywords = keywords_row["keywords"]
                    if check_contains_keywords(content, keywords):
                        matched_keywords.append(keywords)
                if len(matched_keywords) > 0:
                    next_time = datetime.datetime.strptime(date_time, '%Y-%m-%d %H:%M:%S') + \
                                datetime.timedelta(seconds=1)
                    if news_src[src] < next_time.strftime('%Y-%m-%d %H:%M:%S'):
                        news_src[src] = next_time.strftime('%Y-%m-%d %H:%M:%S')
                    # 判断重复性
                    if check_duplicate(recent_news_deque, content):
                        continue
                    recent_news_deque.append(content)
                    dict = {}
                    dict["date_time"] = date_time
                    dict["keywords"] = matched_keywords
                    dict["content"] = mark_content(content, matched_keywords)
                    dict["src"] = src
                    print(dict)
                    result_df = result_df.append(dict, ignore_index=True)

        today = time.strftime('%Y%m%d')
        output_file_name = os.path.join(data_path, "result", "focus_news_{0}.csv".format(today))
        if len(result_df) > 0 and time.perf_counter() - last_save_time > 60:
            try:
                save_and_send(result_df, output_file_name)
                # result_df.to_csv(output_file_name, mode='a', header=False, encoding="utf_8_sig")
                result_df = pd.DataFrame(columns=["date_time", "keywords", "content", "src"])
            except Exception as e:
                print('str(e):\t', str(e))

        time.sleep(30)


def save_and_send(result_df=pd.DataFrame, output_file_name=str):
    mail_content = ""
    for news_index, news_row in result_df.iterrows():
        mail_content += "时间：{0}<br/>".format(news_row["date_time"])
        mail_content += "关键字：{0}<br/>".format(news_row["keywords"])
        mail_content += "内容：{0}<br/>".format(news_row["content"])
        mail_content += "来源：{0}<br/>".format(news_row["src"])
        mail_content += "<p/>"
    sendmail.send_news_mail(mail_content)
    result_df.to_csv(output_file_name, mode='a', header=False, encoding="utf_8_sig")


def check_contains_keywords(content, keywords):
    for keyword in keywords.split(","):
        if keyword not in content:
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
        columns = ["date_time", "keywords", "content", "src"]
        df = pd.read_csv(output_file_name, encoding="utf_8_sig", names=columns)
        if len(df) > 0:
            return str(pd.to_datetime(df.date_time).max())
    return last_time


def mark_content(content=str, keywords_list=[]):
    for keywords in keywords_list:
        for keyword in keywords.split(","):
            content = content.replace(keyword, "<b>" + keyword + "</b>")
    return content


def check_duplicate(recent_news_deque: deque, content: str):
    for recent_news in recent_news_deque:
        if difflib.SequenceMatcher(None, recent_news, content).quick_ratio() >= 0.8:
            print("skip:{0}".format(content))
            return True
    return False


if __name__ == '__main__':
    monitor(os.path.join(data_path, "news_focus_keywords.csv"))
    print()
