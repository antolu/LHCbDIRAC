#!/usr/bin/env python
'''Script to run Executable application'''

from os import system
import sys

# Main
if __name__ == '__main__':

    sys.exit( system( '''ls 081616_0000000213.raw''' ) / 256 )
