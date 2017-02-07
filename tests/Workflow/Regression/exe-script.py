#!/usr/bin/env python
'''Script to run Executable application'''

import os
import sys

# Main
if __name__ == '__main__':
  sys.exit(os.system('''echo Hello World''')/256)
