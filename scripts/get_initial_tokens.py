#!/usr/bin/python
import sys

def tokens(nodes):
    for i in range(1, nodes + 1):
        print (i * (2 ** 127 - 1) / nodes)

tokens(int(sys.argv[1]))
