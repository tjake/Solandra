#!/usr/bin/python
import sys

def tokens(nodes):
    for i in range(0, nodes):
        print (i * (2 ** 127 - 1) / nodes)

tokens(int(sys.argv[1]))
