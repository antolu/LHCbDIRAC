#! /usr/bin/env python

import sys

version = sys.version.split()[0]
v = version.split('.')
if version[0:3] == "2.4" and len(v)>2: v[2] = "2"
if "-2" in sys.argv:
    n = 2
else:
    n = len(v)
print ".".join(v[:n])
