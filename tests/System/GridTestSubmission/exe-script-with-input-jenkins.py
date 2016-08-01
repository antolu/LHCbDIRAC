#!/usr/bin/env python
'''Script to run Executable application'''

import sys
from os import system, environ, pathsep, getcwd

# Main
if __name__ == '__main__':

  environ['PATH'] = getcwd() + (pathsep + environ['PATH'])
  sys.exit( system( '''cat jenkinsInputTestFile.txt''' ) / 256 )
