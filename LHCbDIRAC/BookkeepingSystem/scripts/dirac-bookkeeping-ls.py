#!/usr/bin/env python
########################################################################
# $Id: dirac-bookkeeping-ls.py,v 1.2 2008/10/17 13:03:02 rgracian Exp $
########################################################################

from DIRACEnvironment import DIRAC
from DIRAC.Interfaces.API.Dirac                       import Dirac
from DIRAC.BookkeepingSystem.Client.LHCB_BKKDBClient  import LHCB_BKKDBClient
from DIRAC.Core.Base import Script

__RCSID__ = "$Id: dirac-bookkeeping-ls.py,v 1.2 2008/10/17 13:03:02 rgracian Exp $"

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()


cl = LHCB_BKKDBClient()
path = args[0]

print cl.list(path)

