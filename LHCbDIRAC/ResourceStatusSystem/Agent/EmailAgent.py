"""
  Extends DIRAC EmailAgent

  This agent extends the DIRAC EmailAgent which is used to aggregate status changes,
  in this case LHCbDIRAC EmailAgent adds the additional functionality of automatically posting these
  status changes in the LHCb logbook ("lblogbook.cern.ch").

  This is done by invoking a client program named elog (1) which is used to post the data to the LHCb logbook.
  The authentication is done by providing a valid username and password in the configuration file of dirac.
"""

import os
import sqlite3
import requests
from DIRAC                                              import S_OK, S_ERROR
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

              try:
                response = requests.post('https://lblogbook.cern.ch:5050/log',
                  json = {
                    "user": elogUsername,
                    "password": elogPassword,
                    "logbook": "Operations",
                    "system": "Site Downtime",
                    "text": elements,
                    "subject": "RSS Actions Taken for " + site[0]
                  }).json()

                response.raise_for_status()
              except requests.exceptions.RequestException as e:
                return S_ERROR("Error %s" % e)

    super( EmailAgent, self ).execute()

    return S_OK()
