#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/DIRAC/ProductionManagementSystem/scripts/dirac-production-manager-cli.py,v 1.24 2008/10/17 13:15:46 rgracian Exp $
# File :   dirac-production-manager-cli
# Author :  Gennady G. Kuznetsov
########################################################################
__RCSID__   = "$Id: dirac-production-manager-cli.py,v 1.24 2008/10/17 13:15:46 rgracian Exp $"
__VERSION__ = "$Revision: 1.24 $"

from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.ProductionManagementSystem.Client.ProductionManagerCLI import ProductionManagerCLI


Script.localCfg.addDefaultEntry( "LogLevel", "DEBUG" )
Script.parseCommandLine()


ProductionManagerCLI().cmdloop()
