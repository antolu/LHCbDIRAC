########################################################################
# $Id$
########################################################################
""" Gaudi Application Class """

__RCSID__ = "$Id$"
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