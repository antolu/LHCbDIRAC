#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-remove-lfn
# Author :  Stuart Paterson
########################################################################
"""
  Obsolete, use DMS scripts instead
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC import gLogger

gLogger.always( "This script is obsolete, please use dirac-dms-remove-files" )

import os
os.system( "dirac-dms-remove-files --help" )

DIRAC.exit( 2 )
