#!/usr/local/bin/python
# coding: utf-8

import socket

s = socket.socket()

host = socket.gethostname()
port = 32768

l = "Buy 300226 1000 at 50.13 "

s.connect(('ghuan02-d.inovageo.com', port))

s.send(l)

print s.recv(1024)