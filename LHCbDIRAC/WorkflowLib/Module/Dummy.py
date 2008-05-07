########################################################################
# $Id: Dummy.py,v 1.1 2008/05/07 05:36:35 joel Exp $
########################################################################
""" Gaudi Application Class """

__RCSID__ = "$Id: Dummy.py,v 1.1 2008/05/07 05:36:35 joel Exp $"
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