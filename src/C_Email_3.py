#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import smtplib
from email.mime.text import MIMEText


class C_Email(object):
    '''
    Change: 2017-06-22 in _send_trading_command: modified the received signal to 1 and -1 to match the signal calculation result.
            2017-06-22 in _MACD_Singal_calcualtion_cross use MACD_short = 999 to represent real life stock operation and save them in DB.
    '''

    def __init__(self):
        self._sender = 'meng.qiang01@mail.com'
        self._receiver = ['mars.guan@gmail.com']
        # self._username = '1814241918'
        self._username = 'meng.qiang01@mail.com'
        # self._pwd = 'New_2014'
        self._pwd = 'New_2008'

    def send_email(self, subject, body):
        message = "\r\n".join(["From: From meng qiang <meng.qiang01@mail.com>",
                                      "To: INOVA",
                               "Subject:" + subject,
                                      "",
                               body.as_string()
                               ])

        #server = smtplib.SMTP_SSL('smtp.gmail.com:465')
        server = smtplib.SMTP('smtp.mail.com:587')
        #server.ehlo()
        #server.starttls()

        server.login(self._username, self._pwd)
        server.sendmail(self._sender, self._receiver, message)
        server.quit()


def main():
    email = C_Email()
    subject = "This is a testing"
    body = MIMEText("This is a testing")
    email.send_email(subject, body)


if __name__ == '__main__':
    main()
