#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-add-file
# Author :  Stuart Paterson
########################################################################
"""
  Obsolete, use DMS scripts instead
"""
__RCSID__ = "$Id$"

import DIRAC
from DIRAC import gLogger

gLogger.always( "This script is obsolete, please use dirac-dms-add-files" )

import os
os.system( "dirac-dms-add-files --help" )

DIRAC.exit( 2 )
