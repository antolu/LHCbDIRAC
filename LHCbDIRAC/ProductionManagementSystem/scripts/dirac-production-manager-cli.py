#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-production-manager-cli
# Author :  Gennady G. Kuznetsov
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.24 $"

import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.ProductionManagementSystem.Client.ProductionManagerCLI import ProductionManagerCLI


Script.localCfg.addDefaultEntry( "LogLevel", "DEBUG" )
Script.parseCommandLine()


ProductionManagerCLI().cmdloop()
