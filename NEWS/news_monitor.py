import os
import sys

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)

import re, time, datetime
import mydate
import pandas as pd
import mydb
import mytusharepro
from sqlalchemy import Table, Column, String, Float, MetaData
from concurrent.futures import ThreadPoolExecutor
import my_email.sendmail as sendmail

pro = mytusharepro.MyTusharePro()


def monitor(file_name):
    today = time.strftime('%Y%m%d')
    now_time = datetime.datetime.now()
    now_hour = time.strftime('%H')
    last_time = (now_time + datetime.timedelta(days=-1)).strftime("%Y-%m-%d 15:00:00")
    if now_hour >= '15':
        last_time = now_time.strftime("%Y-%m-%d 15:00:00")

    news_src = ('sina', 'wallstreetcn', '10jqka', 'eastmoney', 'yuncaijing')

    while True:
        focus_keyword_df = pd.read_csv(file_name, encoding="gbk")
        start_date = last_time
        end_date = time.strftime('%Y-%m-%d %H:%M:%S')
        for src in news_src:
            print("news: {0}，{1},{2}".format(start_date, end_date, src))
            news_df = pro.news(src=src, start_date=start_date, end_date=end_date)
            for news_index, news_row in news_df.iterrows():
                date_time = news_row["datetime"]
                content = news_row["content"]
                for keywords_index, keywords_row in focus_keyword_df.iterrows():
                    keywords = keywords_row["keywords"]
                    if check_contains_keywords(content, keywords):
                        result_df = pd.DataFrame(columns=["date_time", "keywords", "content", "src"])
                        dict = {}
                        dict["date_time"] = date_time
                        dict["keywords"] = keywords
                        dict["content"] = content
                        dict["src"] = src
                        print(dict)
                        mail_content = "时间：{0}<br/>".format(date_time)
                        mail_content += "关键字：{0}<br/>".format(keywords)
                        mail_content += "内容：{0}<br/>".format(content)
                        mail_content += "来源：{0}<br/>".format(src)
                        sendmail.send_news_mail(mail_content)
                        result_df = result_df.append(dict, ignore_index=True)
                        output_file_name = "d:/temp/focus_news_{0}.csv".format(today)
                        try:
                            result_df.to_csv(output_file_name, mode='a', header=False, encoding="utf_8_sig")
                        except Exception as e:
                            print('str(e):\t', str(e))
        last_time = end_date
        time.sleep(30)


def check_contains_keywords(content, keywords):
    for keyword in keywords.split(","):
        if keyword not in content:
            return False
    return True


if __name__ == '__main__':
    monitor("D:\\Temp\\news_focus_keywords.csv")
    print('')