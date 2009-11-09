########################################################################
# $Id: Dummy.py 18064 2009-11-05 19:40:01Z acasajus $
########################################################################
""" Gaudi Application Class """

__RCSID__ = "$Id: Dummy.py 18064 2009-11-05 19:40:01Z acasajus $"
from DIRAC import *

import shutil, re, string, os, sys

class Dummy(object):

  #############################################################################
  def __init__(self):
     pass
  #############################################################################
  def execute(self):
    cwd = os.getcwd()
    print cwd
    return S_OK

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#