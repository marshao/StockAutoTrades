#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

from src import C_GlobalVariable as glb
import smtplib


class C_Email(object):
    '''
    Change: 2017-06-22 in _send_trading_command: modified the received signal to 1 and -1 to match the signal calculation result.
            2017-06-22 in _MACD_Singal_calcualtion_cross use MACD_short = 999 to represent real life stock operation and save them in DB.
    '''

    def __init__(self):
        gv = glb.C_GlobalVariable()
        self._sender = 'guan.hao@inovageo.com'
        self._receiver = ['guan.hao@inovageo.com']
        # self._username = '1814241918'
        self._username = 'ghao@inovageo.com'
        # self._pwd = 'New_2014'
        self._pwd = 'New_2013'

    def send_email(self, body):
        message_header = "\r\n".join(["From: From Guan Hao <guan.hao@inovageo.com>",
                                      "To: To Guan Hao <guan.hao@foxmail.com>",
                                      "Subject: Stock Notification Email",
                                      "",
                                      body
                                      ])

        message = message_header + body

        server = smtplib.SMTP('cal1cas01.inovageo.com:25')
        server.ehlo()
        server.starttls()
        server.login(self._username, self._pwd)
        server.sendmail(self._sender, self._receiver, message)
        server.quit()


def main():
    email = C_Email()
    email.send_email(body="The signal is verified")


if __name__ == '__main__':
    main()
