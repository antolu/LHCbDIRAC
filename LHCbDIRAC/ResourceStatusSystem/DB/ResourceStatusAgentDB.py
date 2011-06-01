""" 
The ResourcesStatusAgentDB module contains a couple of exception classes, and a 
class to interact with the ResourceStatusAgent DB.
"""

import datetime

from DIRAC.ResourceStatusSystem.Utilities.Utils import where
from DIRAC.ResourceStatusSystem.Utilities.Exceptions import RSSException

#############################################################################

class RSSAgentDBException( RSSException ):
  """ 
  DB exception 
  """
  
  def __init__( self, message = "" ):
    self.message = message
    RSSException.__init__( self, message )
  
  def __str__( self ):
    return "Exception in the RSS Agent DB: " + repr( self.message )

#############################################################################

class ResourceStatusAgentDB:
  """ 
  The ResourceStatusAgentDB class is a front-end to the Resource StatusAgent Database.
  
  The simplest way to instantiate an object of type :class:`ResourceStatusAgentDB` 
  is simply by calling 

   >>> rsaDB = ResourceStatusAgentDB()

  This way, it will use the standard :mod:`DIRAC.Core.Base.DB`. 
  But there's the possibility to use other DB classes. 
  For example, we could pass custom DB instantiations to it, 
  provided the interface is the same exposed by :mod:`DIRAC.Core.Base.DB`.

   >>> AnotherDB = AnotherDBClass()
   >>> rsaDB = ResourceStatusAgentDB(DBin = AnotherDB)

  Alternatively, for testing purposes, you could do:

   >>> from DIRAC.ResourceStatusSystem.Utilities.mock import Mock
   >>> mockDB = Mock()
   >>> rsaDB = ResourceStatusAgentDB(DBin = mockDB)

  Or, if you want to work with a local DB, providing it's mySQL:

   >>> rsaDB = ResourceStatusAgentDB(DBin = ['UserName', 'Password'])

  """

  
  def __init__( self, *args, **kwargs ):

    if len( args ) == 1:
      if isinstance( args[0], str ):
#        systemInstance=args[0]
        maxQueueSize=10
      if isinstance( args[0], int ):
        maxQueueSize=args[0]
#        systemInstance='Default'
    elif len( args ) == 2:
#      systemInstance=args[0]
      maxQueueSize=args[1]
    elif len( args ) == 0:
#      systemInstance='Default'
      maxQueueSize=10
    
    if 'DBin' in kwargs.keys():
      DBin = kwargs['DBin']
      if isinstance( DBin, list ):
        from DIRAC.Core.Utilities.MySQL import MySQL
        self.db = MySQL( 'localhost', DBin[0], DBin[1], 'ResourceStatusAgentDB' )
      else:
        self.db = DBin
    else:
      from DIRAC.Core.Base.DB import DB
      self.db = DB( 'ResourceStatusAgentDB', 'ResourceStatus/ResourceStatusAgentDB', maxQueueSize ) 
#      self.db = DB('ResourceStatusDB','ResourceStatus/ResourceStatusDB',maxQueueSize)


#############################################################################

#############################################################################
# HCAgent functions
#############################################################################

#############################################################################

  # ADD a(n empty) test to the DB.
  def addTest( self, siteName, submissionTime, agentStatus = '', resourceName = '' ):
    """ 
    Add a new test to the DB.
    
    :params:
      :attr:`siteName`: string - site where the test is submitted

      :attr:`submissionTime`: date time - test submission attempt (on time)
           
      :attr:`agentStatus`: string - additional comments
    """

    req = "INSERT INTO HCAgent (SiteName, SubmissionTime, AgentStatus, ResourceName )"
    req = req + "VALUES ('%s', '%s', '%s', '%s');" % ( siteName, submissionTime, agentStatus, resourceName )

    resUpdate = self.db._update( req )
    if not resUpdate['OK']:
      raise RSSAgentDBException, where( self, self.addTest ) + resUpdate['Message']

#############################################################################

  #Remove a test from the DB
  def removeTest( self, testID ):
    """ 
    Remove a test from the HCAgent table.
    
    :params:
      :attr:`testID`: integer - test ID
    """

    req = "DELETE from HCAgent WHERE TestID = %d;" % testID
    resDel = self.db._update( req )
    if not resDel['OK']:
      raise RSSAgentDBException, where( self, self.removeTest ) + resDel['Message']

#############################################################################
  
  def updateTest( self, submissionTime, testID = None, testStatus = None, startTime = None, endTime = None, counterTime = None, 
                  agentStatus = None, formerAgentStatus = None, counter = None ):
    """
    Update test parameters on the HCAgent table.
  
    :params:
      :attr: `submissionTime`: date time - test submission attempt (on time), key to find a test on the table
      
      :attr: `testID` : integer - (optional) assigned (by HC) test ID
      
      :attr: `testStatus` : string - (optional) assigned (by HC) test status
      
      :attr: `startTime` : date time - (optional) assigned (by HC) start time
       
      :attr: `endTime` : date time - (optional) assigned (by HC) end time
      
      :attr: `counterTime` : date time - (optional) error registered time
      
      :attr: `agentStatus` : string - (optional) additional comments
      
      :attr: `counter` : integer - number of times test has failed
    """
    
    paramsList = []
        
    if submissionTime is not None:
      paramsList.append( 'SubmissionTime = "%s"' % submissionTime )    
        
    if testID is not None:
      paramsList.append( 'TestID = %d' % testID )

    if testStatus is not None:
      paramsList.append( 'TestStatus = "%s"' % testStatus )
      
    if startTime is not None:
      paramsList.append( 'StartTime = "%s"' % startTime )
      
    if endTime is not None:
      paramsList.append( 'EndTime = "%s"' % endTime )
    
    if counterTime is not None:
      paramsList.append( 'CounterTime = "%s"' % counterTime )
          
    if agentStatus is not None:
      paramsList.append( 'AgentStatus = "%s"' % agentStatus )

    if formerAgentStatus is not None:
      paramsList.append( 'FormerAgentStatus = "%s"' % formerAgentStatus )
    
    if counter is not None:
      paramsList.append( 'Counter = %d' % counter )
    
    req = 'UPDATE HCAgent SET '
    req += ', '.join( paramsList )
    
    if submissionTime:
      req += ' WHERE SubmissionTime = "%s";' % submissionTime    
    elif testID:
      req += ' WHERE TestID = %d;' % testID
    else:
      raise RSSAgentDBException, where( self, self.updateTest ) + 'Submission time and TestID missing'  
        
    resUpdate = self.db._update( req )
    if not resUpdate['OK']:
      raise RSSAgentDBException, where( self, self.updateTest ) + resUpdate['Message']     
  
#############################################################################  
  
  def getTestList( self, submissionTime = None, testStatus = None, siteName = None, testID = None,
                   resourceName = None, agentStatus = None, formerAgentStatus = None, counter = None, 
                   last = False ):
    """ 
    Get list of tests, having as query filters the parameters.
    
    :params:
      :attr: `submissionTime` : datetime - test submission attempt (on time), select filter 
      
      :attr: `testStatus` : datetime - select filter
      
      :attr: `siteName` : datetime - site where the test is submitted, select filter
      
      :attr: `testID` : datetime - assigned (by HC) test ID, select filter
      
      :attr: `agentStatus` : string - either HCongoing, HCfinished or HCfailure.
      
      :attr: `counter` : number of failures.
      
    """

    paramsList = []
        
    if submissionTime is not None:
      paramsList.append( 'SubmissionTime = "%s"' % submissionTime )
      
    if testStatus is not None:
      paramsList.append( 'TestStatus = "%s"' % testStatus )
      
    if siteName is not None:
      paramsList.append( 'SiteName = "%s"' % siteName )

    if resourceName is not None:
      paramsList.append( 'ResourceName = "%s"' % resourceName )
          
    if testID is not None:
      paramsList.append( 'TestID = "%s"' % testID )

    if agentStatus is not None:
      paramsList.append( 'AgentStatus = "%s"' % agentStatus )

    if formerAgentStatus is not None:
      paramsList.append( 'FormerAgentStatus = "%s"' % formerAgentStatus )

    if counter is not None:
      paramsList.append( 'Counter > %d' % counter )

    req = 'SELECT TestID, SiteName, ResourceName, TestStatus, AgentStatus, FormerAgentStatus, SubmissionTime, StartTime, EndTime, Counter, CounterTime from HCAgent' 

    if paramsList:
      req += ' WHERE ' + ' AND '.join( paramsList )  
    req += ' ORDER BY SubmissionTime'  
    
    if last:
      req += ' DESC LIMIT 1'
      
    req += ';'   

    resQuery = self.db._query( req )
    if not resQuery['OK']:
      raise RSSAgentDBException, where( self, self.getTestList ) + resQuery['Message']
    if not resQuery['Value']:
      return []
    #l = [ x[0] for x in resQuery['Value']]
    return [res for res in resQuery['Value']]

#############################################################################