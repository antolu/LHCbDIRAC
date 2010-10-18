########################################################################
# $Id$
########################################################################
"""

"""

__RCSID__ = "$Id$"

from LHCbDIRAC.BookkeepingSystem.DB.IBookkeepingDB                   import IBookkeepingDB
from types                                                           import *
from DIRAC                                                           import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config                         import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder                     import getDatabaseSection
from DIRAC.Core.Utilities.OracleDB                                   import OracleDB
import datetime
import types
global ALLOWED_ALL 
ALLOWED_ALL = 2
class OracleBookkeepingDB(IBookkeepingDB):
  
  #############################################################################
  def __init__(self):
    """
    """
    super(OracleBookkeepingDB, self).__init__()
    self.cs_path = getDatabaseSection('Bookkeeping/BookkeepingDB')
    
    self.dbHost = ''
    result = gConfig.getOption( self.cs_path+'/LHCbDIRACBookkeepingTNS')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: Host')
      return
    self.dbHost = result['Value']
    
    self.dbUser = ''
    result = gConfig.getOption( self.cs_path+'/LHCbDIRACBookkeepingUser')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: User')
      return
    self.dbUser = result['Value']
    
    self.dbPass = ''
    result = gConfig.getOption( self.cs_path+'/LHCbDIRACBookkeepingPassword')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: User')
      return
    self.dbPass = result['Value']


    self.dbServer = ''
    result = gConfig.getOption( self.cs_path+'/LHCbDIRACBookkeepingServer')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: User')
      return
    self.dbServer = result['Value']

    self.dbW_ = OracleDB(self.dbServer, self.dbPass, self.dbHost)
    self.dbR_ = OracleDB(self.dbUser, self.dbPass, self.dbHost)
  
  #############################################################################
  def getAvailableSteps(self, dict = {}):      
    condition = None
    selection = 'stepid,stepname, applicationname,applicationversion,optionfiles,DDDB,CONDDB, extrapackages,VisibilityFlag'
    if len(dict) > 0:
      tables = 'steps'
      if dict.has_key('StartDate'):
        condition += ' steps.inserttimestamps >= TO_TIMESTAMP (\''+dict['StartDate']+'\',\'YYYY-MM-DD HH24:MI:SS\')'
      if dict.has_key('StepId'):
        tables += ',table(steps.filetypesids) ftypes, filetypes '
        if condition != None:
          condition+= ' and '      
        condition += 'ftypes.filetypeid=filetypes.filetypeid '
        selection = 'filetypes.name,ftypes.visibilityflag '
      command = 'select '+selection+' from '+tables+' where '+condition+' order by inserttimestamps'
      return self.dbR_._query(command)
    else:
      command = 'select '+selection+' from steps order by inserttimestamps'
      return self.dbR_._query(command)
