#!/usr/bin/env python
########################################################################
# $Id: dirac-bookkeeping-ls.py,v 1.1 2008/07/31 08:18:49 zmathe Exp $
########################################################################

from DIRAC.Interfaces.API.Dirac                       import Dirac
from DIRAC.BookkeepingSystem.Client.LHCB_BKKDBClient  import LHCB_BKKDBClient
from DIRAC.Core.Base import Script

__RCSID__ = "$Id: dirac-bookkeeping-ls.py,v 1.1 2008/07/31 08:18:49 zmathe Exp $"

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()


cl = LHCB_BKKDBClient()
path = args[0]

print cl.list(path)

