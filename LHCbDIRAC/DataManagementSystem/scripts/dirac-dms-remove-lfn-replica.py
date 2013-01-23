#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-dms-remove-lfn-replica
# Author :  Stuart Paterson
########################################################################
"""
  Obsolete: should use the DataManagement scripts
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC import gLogger

gLogger.always( "This script is obsolete, please use dirac-dms-remove-replicas" )

import os
os.system( "dirac-dms-remove-replicas --help" )

DIRAC.exit( 2 )
