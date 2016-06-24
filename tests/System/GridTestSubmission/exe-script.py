#!/usr/bin/env python
'''Script to run Executable application'''

from os import system, environ, pathsep, getcwd
import sys
import random
import string
# Main
if __name__ == '__main__':

    environ['PATH'] = getcwd() + (pathsep + environ['PATH'])        
    sys.exit( system( '''echo Hello World %s''' % ( ''.join( random.choice( string.ascii_letters ) for _ in range( 10 ) ) ) ) / 256 )
  
