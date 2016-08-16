"""
  Extends DIRAC EmailAgent
"""

import os
import sqlite3
from DIRAC                                              import gConfig, S_OK
from DIRAC.ResourceStatusSystem.Agent.EmailAgent        import EmailAgent as DiracEmAgent

__RCSID__ = '$Id: $'

AGENT_NAME = 'ResourceStatus/EmailAgent'


class EmailAgent( DiracEmAgent ):

  def __init__( self, *args, **kwargs ):

    super( EmailAgent, self ).__init__( *args, **kwargs )

    if 'DIRAC' in os.environ:
      self.cacheFile = os.path.join( os.getenv('DIRAC'), 'work/ResourceStatus/cache.db' )
    else:
      self.cacheFile = os.path.realpath('cache.db')

  def execute( self ):

    defaultSites = ['CERN', 'CNAF', 'GridKa', 'IN2P3', 'NIKHEF', 'PIC', 'RAL', 'RRCKI', 'SARA', 'HLTFarm', 'Tier2-Ds',
                    'CBPF', 'CPPM', 'CSCS', 'IHEP', 'LAL', 'LPNHE', 'Manchester', 'NCBJ', 'NIPNE-07', 'RAL-HEP', 'UKI-LT2-IC-HEP']

    sites = self.am_getOption( 'Sites', defaultSites )

    elogUsername = self.am_getOption( 'Elog_Username' )
    elogPassword = self.am_getOption( 'Elog_Password' )

    if os.path.isfile(self.cacheFile):
      with sqlite3.connect(self.cacheFile) as conn:

        result = conn.execute("SELECT DISTINCT SiteName from ResourceStatusCache;")
        for site in result:
          cursor = conn.execute("SELECT StatusType, ResourceName, Status, Time, PreviousStatus from ResourceStatusCache WHERE SiteName='"+ site[0] +"';")

          DryRun = self.am_getOption( 'DryRun', False )

          if DryRun:
           self.log.info("Running in DryRun mode...")
           return S_OK()
          else:
            elements = ""
            substring = site[0].split('.', 1)[0]

            if substring in sites:
              for StatusType, ResourceName, Status, Time, PreviousStatus in cursor:
                elements += StatusType + " of " + ResourceName + " has been " + Status + " since " + \
                            Time + " (Previous status: " + PreviousStatus + ")\n"

              os.system('./elog -h lblogbook.cern.ch -p 8080 -l Operations -u ' + elogUsername + ' ' + elogPassword + ' -a System="Site Downtime" '
                               '-a author="LHCb RSS" -a Subject="RSS Actions Taken for ' + site[0] + '" "' + elements + '"')

    super( EmailAgent, self ).execute()

    return S_OK()
