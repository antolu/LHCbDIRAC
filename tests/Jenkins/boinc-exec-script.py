#!/usr/bin/env python
###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
'''Create a file whose name and content is dictated by the first parameter'''


import sys
from os import system
import time

if __name__ == '__main__':
  arg = sys.argv[1]
  with open('%s_toto.txt'%arg, 'w') as f:
    f.write("%s"%arg)
  sys.exit(0)
