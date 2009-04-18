########################################################################
# $Id: Dummy.py,v 1.2 2009/04/18 18:26:56 rgracian Exp $
########################################################################
""" Gaudi Application Class """

__RCSID__ = "$Id: Dummy.py,v 1.2 2009/04/18 18:26:56 rgracian Exp $"
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