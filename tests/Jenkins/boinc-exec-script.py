#!/usr/bin/env python
'''Create a file whose name and content is dictated by the first parameter'''


import sys
from os import system
import time

if __name__ == '__main__':
  arg = sys.argv[1]
  with open('%s_toto.txt'%arg, 'w') as f:
    f.write("%s"%arg)
  sys.exit(0)
