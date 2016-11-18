#!/usr/local/bin/python
# coding: utf-8

import socket

s = socket.socket()

host = socket.gethostname()
port = 32768

s.connect(('ghuan02-d.inovageo.com', port))

s.send("This is a message from client")

print s.recv(1024)