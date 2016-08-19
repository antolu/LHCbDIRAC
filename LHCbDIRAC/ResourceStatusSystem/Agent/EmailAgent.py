"""
  Extends DIRAC EmailAgent

  This agent extends the DIRAC EmailAgent which is used to aggregate status changes,
  in this case LHCbDIRAC EmailAgent adds the additional functionality of automatically posting these
  status changes in the LHCb logbook ("lblogbook.cern.ch").

  This is done by invoking a client program named elog (1) which is used to post the data to the LHCb logbook.
  The authentication is done by providing a valid username and password in the configuration file of dirac.

  By default this agent will only post notifications that match a list of sites
  (the list can be changed in the configuration file)

  (1) The elog program can be downloaded from here: http://midas.psi.ch/elog/download.html
  Upon the first execution if you get any error that includes "libssl.so.10" make sure that you have
  "libssl1.0.0" and "libssl-dev" installed and make a link:

  "cd /lib/x86_64-linux-gnu"
  "sudo ln -s libssl.so.1.0.0 libssl.so.10"
  "sudo ln -s libcrypto.so.1.0.0 libcrypto.so.10"
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
