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
"""
  Command to invoke the LHCb Bookkeeping Database graphical user interface
"""

__RCSID__ = "$Id$"


""" Here we simply invoke dirac-bookkeeping-gui from a v9r2pX version. Reasons are:

    - v9r3 uses lcgBundle instead of LHCbGrid, and lcgBundle does not include Qt4
    - DIRACOS will come and we don't want to make too much effort with this temporary solution of lcgBundle
    - dirac-bookkeeping-gui is anyway set to disappear in favor of the web version
"""

import os

os.system("lb-run -c best LHCbDIRAC/v9r2p11 dirac-bookkeeping-gui")
