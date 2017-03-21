#!/usr/bin/env python
'''Script to run Executable application'''

from os import system
import sys
import random
import string
# Main
if __name__ == '__main__':

    sys.exit( system( '''echo Hello World %s''' % ( ''.join( random.choice( string.ascii_letters ) for _ in range( 10 ) ) ) ) / 256 )
