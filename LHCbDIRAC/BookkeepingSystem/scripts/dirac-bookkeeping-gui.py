#!/usr/bin/env python
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
