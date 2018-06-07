#!/usr/local/bin/python
# coding: utf-8

__metclass__ = type

import mechanize

br = mechanize.Browser()

to = 'guan.hao@inovageo.com'
subject = 'email testing'
message = 'This is a testing email'

url = "http://anonymouse.org/anonemail.html"
headers = "Mozilla/4.0(compatible: MSIE 5.0; Windows95; AOL 4.0; c_athome)"
br.addheaders = [('User-agent', headers)]
br.open(url)
br.set_handle_equiv(True)
br.set_handle_gzip(True)
br.set_handle_redirect(True)
br.set_handle_referer(True)
br.set_handle_robots(False)
br.set_debug_http(False)
br.set_debug_redirects(False)

br.select_form(nr=0)

br.form['to'] = to
br.form['subject'] = subject
br.form['text'] = message

result = br.submit()

repsonse = br.response().read()

#print repsonse

if "The e-mail has been sent anonymously" in repsonse:
    print "Successed"
else:
    print 'Failed'