#!/usr/bin/env python
########################################################################
# $HeadURL:  $
# File :    dirac-bookkeeping-CLI.py
# Author :  Zoltan Mathe
########################################################################

from DIRAC.Core.Base                                      import Script
Script.parseCommandLine( ignoreErrors = True )

from LHCbDIRAC.BookkeepingSystem.Client.LHCbBookkeepingCLI import LHCbBookkeepingCLI

#############################################################################
if __name__ == '__main__':
  bk = LHCbBookkeepingCLI()
  bk.cmdloop()
