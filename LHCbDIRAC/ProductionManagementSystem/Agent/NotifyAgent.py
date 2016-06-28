''' NotifyAgent
  This agent reads a cache file ( cache.db ) which contains the aggregated information
  of what happened to each production request. After reading the cache file
  ( by default every 30 minutes ) it sends an email for every site and then clears it.
'''

import os
import sqlite3
from DIRAC                                                       import gConfig, S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                                 import AgentModule
from DIRAC.FrameworkSystem.Client.NotificationClient             import NotificationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Registry           import getUserOption, getUsersInGroup

__RCSID__ = '$Id: $'

AGENT_NAME = 'ProductionManagement/NotifyAgent'

class NotifyAgent( AgentModule ):

  def __init__( self, *args, **kwargs ):

    AgentModule.__init__( self, *args, **kwargs )

    self.notification = None

    if 'DIRAC' in os.environ:
      self.cacheFile = os.path.join( os.getenv('DIRAC'), 'work/ProductionManagement/cache.db' )
    else:
      self.cacheFile = os.path.realpath('cache.db')

  def initialize( self ):
    ''' NotifyAgent initialization
    '''

    self.notification = NotificationClient()

    return S_OK()

  def execute( self ):

    if os.path.isfile(self.cacheFile):
      with sqlite3.connect(self.cacheFile) as conn:

        result = conn.execute("SELECT DISTINCT thegroup from ProductionManagementCache;")

        for group in result:

          aggregate = ""

          print group[0]
          cursor = conn.execute("SELECT subject, body, fromAddress from ProductionManagementCache WHERE thegroup = ?", (group[0],) )

          for subject, body, fromAddress in cursor:
            print subject, body, fromAddress

            aggregate += subject + body + "\n------------------------------------------------------\n"

          for man in self._getMemberMails( group[0] ):

            notification = NotificationClient()
            res = notification.sendMail( man, "Notifications for production requests", aggregate, fromAddress, True )

            if not res['OK']:
              self.log.error( "_inform_people: can't send email: %s" % res['Message'] )

    return S_OK()

  def _getMemberMails( self, group ):
    """ get members mails
    """
    members = getUsersInGroup( group )
    if members:
      emails = []
      for user in members:
        email = getUserOption( user, 'Email' )
        if email:
          emails.append( email )
      return emails

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
