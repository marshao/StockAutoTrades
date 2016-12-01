#!/usr/local/bin/python
# coding: UTF-8

from pycallgraph import PyCallGraph
from pycallgraph.output import GraphvizOutput

from LinuxBackEnd.BackEndMain import *
from LinuxBackEnd.C_Algorithms_BestMACDPattern import C_BestMACDPattern

graphviz = GraphvizOutput(output_file='BackEndMain.png')

with PyCallGraph(output=graphviz):
    main()
