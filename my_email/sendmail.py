# !/usr/bin/python
# -*- coding: UTF-8 -*-

import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr

def send_rps_mail(mail_content):
    my_sender = '43309251@qq.com'  # 发件人邮箱账号
    my_pass = 'jpbquivmommlbjci'  # 发件人邮箱密码
    my_user = ['43309251@qq.com','19136678@qq.com']  # 收件人邮箱账号
    # my_user = ['43309251@qq.com']

    ret = True
    try:
        msg = MIMEText(mail_content, 'html', 'utf-8')
        msg['From'] = formataddr(["谭世阳", my_sender])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
        # msg['To'] = formataddr(["FK", my_user])  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
        msg['Subject'] = "RPS分析结果"  # 邮件的主题，也可以说是标题

        server = smtplib.SMTP_SSL("smtp.qq.com", 465)  # 发件人邮箱中的SMTP服务器，端口是25
        server.login(my_sender, my_pass)  # 括号中对应的是发件人邮箱账号、邮箱密码
        server.sendmail(my_sender, my_user, msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
        server.quit()  # 关闭连接
    except Exception as e:
        print('str(e):\t', str(e))
        ret = False
    return ret


def send_news_mail(mail_content):
    my_sender = '43309251@qq.com'  # 发件人邮箱账号
    my_pass = 'jpbquivmommlbjci'  # 发件人邮箱密码
    my_user = ['43309251@qq.com']

    ret = True
    try:
        msg = MIMEText(mail_content, 'html', 'utf-8')
        msg['From'] = formataddr(["谭世阳", my_sender])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
        # msg['To'] = formataddr(["FK", my_user])  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
        msg['Subject'] = "关注资讯"  # 邮件的主题，也可以说是标题

        server = smtplib.SMTP_SSL("smtp.qq.com", 465)  # 发件人邮箱中的SMTP服务器，端口是25
        server.login(my_sender, my_pass)  # 括号中对应的是发件人邮箱账号、邮箱密码
        server.sendmail(my_sender, my_user, msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
        server.quit()  # 关闭连接
    except Exception as e:
        print('str(e):\t', str(e))
        ret = False
    return ret