"""
  Extends DIRAC EmailAgent

  This agent extends the DIRAC EmailAgent which is used to aggregate status changes,
  in this case LHCbDIRAC EmailAgent adds the additional functionality of automatically posting these
  status changes in the LHCb logbook ("lblogbook.cern.ch").

  This is done by sending a request to a restful API which is used to post the data to the LHCb logbook.
  The authentication is done by providing a valid username and password in the configuration file of dirac.
"""

import os
import sqlite3
import requests
import errno
from DIRAC                                              import S_OK, S_ERROR
from DIRAC.Core.Utilities                               import DErrno
from DIRAC.ResourceStatusSystem.Agent.EmailAgent        import EmailAgent as DiracEmAgent

__RCSID__ = '$Id: $'

AGENT_NAME = 'ResourceStatus/EmailAgent'


def getName( name ):
  # Method that is used to get the site's name
    try:
        start = name.index( '.' ) + len( '.' )
        end = name.index( '.', start )
        return name[start:end]
    except ValueError:
        return S_ERROR('Site name %s can not be parsed' % name)

class EmailAgent( DiracEmAgent ):

  def __init__( self, *args, **kwargs ):

    super( EmailAgent, self ).__init__( *args, **kwargs )

    if 'DIRAC' in os.environ:
      self.cacheFile = os.path.join( os.getenv('DIRAC'), 'work/ResourceStatus/cache.db' )
    else:
      self.cacheFile = os.path.realpath('cache.db')

  def execute( self ):

    DryRun = self.am_getOption( 'DryRun', True )

    if DryRun:
     self.log.info("Running in DryRun mode...")
     super( EmailAgent, self ).execute()
     return S_OK()

    elogUsername = self.am_getOption( 'Elog_Username' )
    elogPassword = self.am_getOption( 'Elog_Password' )

    if not elogUsername or not elogPassword:
      super( EmailAgent, self ).execute()
      return S_ERROR(DErrno.ECONF, "Elog credentials not provided")

    try:
      response = requests.post('https://lblogbook.cern.ch:5050/config/option',
                                json = {
                                  "user": elogUsername,
                                  "password": elogPassword,
                                  "logbook": "Operations",
                                  "param": "Site"
                                }).json()

    except requests.exceptions.RequestException as e:
      super( EmailAgent, self ).execute()
      return S_ERROR(errno.ECONNABORTED, "Error %s" % e)

    sites = set()

    for sitesName in response['Result'].replace(" ", "").split(","):
      sites.add(sitesName)

    if os.path.isfile(self.cacheFile):
      with sqlite3.connect(self.cacheFile) as conn:

        result = conn.execute("SELECT DISTINCT SiteName from ResourceStatusCache;")
        for site in result:
          cursor = conn.execute("SELECT StatusType, ResourceName, Status, Time, PreviousStatus from ResourceStatusCache WHERE SiteName='"+ site[0] +"';")

          elements = ""
          if site[0] != 'Unassigned Resources':
            name = getName( site[0] )

            if name in sites:
              for StatusType,ResourceName, Status, Time, PreviousStatus in cursor:
                elements += StatusType + " of " + ResourceName + " has been " + Status + " since " + \
                            Time + " (Previous status: " + PreviousStatus + ")\n"

              try:
                requests.post('https://lblogbook.cern.ch:5050/log',
                              json = {
                                "user": elogUsername,
                                "password": elogPassword,
                                "logbook": "Operations",
                                "system": "Site Downtime",
                                "text": elements,
                                "subject": "RSS Actions Taken for " + site[0]
                              }).json()

              except requests.exceptions.RequestException as e:
                super( EmailAgent, self ).execute()
                return S_ERROR(errno.ECONNABORTED, "Error %s" % e)

      conn.close()

    super( EmailAgent, self ).execute()

    return S_OK()
