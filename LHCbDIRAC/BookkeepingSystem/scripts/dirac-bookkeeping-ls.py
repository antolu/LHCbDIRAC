#!/usr/bin/env python
########################################################################
# $Id: dirac-bookkeeping-ls.py,v 1.3 2009/10/12 16:10:35 zmathe Exp $
########################################################################

from DIRACEnvironment import DIRAC
from DIRAC.Interfaces.API.Dirac                       import Dirac
from DIRAC.BookkeepingSystem.Client.LHCB_BKKDBClient  import LHCB_BKKDBClient
from DIRAC.Core.Base import Script

__RCSID__ = "$Id: dirac-bookkeeping-ls.py,v 1.3 2009/10/12 16:10:35 zmathe Exp $"

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()


cl = LHCB_BKKDBClient()
path = ''
if len(args) > 0:
  path = args[0]
if len(args) > 1:
  for i in range(1, len(args)):
    path += args[i]

print cl.list(path)

