# $HeadURL:  $
''' AccountingCommand module
'''

from datetime                                                   import datetime, timedelta

from DIRAC                                                      import gLogger, S_OK, S_ERROR
from DIRAC.AccountingSystem.Client.ReportsClient                import ReportsClient
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.ResourceStatusSystem.Utilities                       import CSHelpers

from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

__RCSID__ = '$Id:  $'  

class AccountingCommand( Command ):
  '''
    Accounting "master" Command
  '''
  
  def __init__( self, args = None, clients = None ):
    
    super( AccountingCommand, self ).__init__( args, clients )
    
    if 'ReportsClient' in self.apis:
      self.rClient = self.apis[ 'ReportsClient' ]
    else:
      self.rClient = ReportsClient() 

    if 'ReportGenerator' in self.apis:
      self.rgClient = self.apis[ 'ReportGenerator' ]
    else:
      self.rgClient = RPCClient( 'Accounting/ReportGenerator' )       
    
    self.rClient.rpcClient = self.rgClient  

    if 'ResourceManagementClient' in self.apis:
      self.rmClient = self.apis[ 'ResourceManagementClient' ]
    else:
      self.rmClient = ResourceManagementClient()  
  
################################################################################  
  
class JobAccountingCommand( AccountingCommand ):
  '''
    Accounting command that gets information of the WMSHistory type.
  '''
  
  def _storeCommand( self, results ):
    '''
      Stores the results of doNew method on the database.    
    '''

    for result in results:

      resQuery = self.rmClient.addOrModifyJobAccountingCache( result[ 'Name' ], 
                                                              result[ 'Checking' ],
                                                              result[ 'Completed' ],
                                                              result[ 'Done' ],
                                                              result[ 'Failed' ],
                                                              result[ 'Killed' ],
                                                              result[ 'Matched' ],
                                                              result[ 'Running' ],
                                                              result[ 'Stalled' ]
                                                              )
      if not resQuery[ 'OK' ]:
        return resQuery
    return S_OK()  
  
  def _prepareCommand( self ):
    '''
      AccountingCommand requires two arguments:
      - hours  : <int>
      - name   : <str>    
    '''

    if not 'hours' in self.args:
      return S_ERROR( 'Number of hours not specified' )
    hours = self.args[ 'hours' ]

    if not 'name' in self.args:
      return S_ERROR( '"name" is missing' )
    name = self.args[ 'name' ]

    return S_OK( ( name, hours ) )
  
  def doNew( self, masterParams = None ):

    if masterParams is not None:
      site, hours = masterParams
      
    else:
      params = self._prepareCommand()
      if not params[ 'OK' ]:
        return params
      site, hours = params[ 'Value' ] 
 
    typeName   = 'WMSHistory'
    reportName = 'NumberOfJobs'
    condDict   = { 'Site' : site }
    grouping   = 'Status'
    
    end   = datetime.utcnow()
    start = end - timedelta( hours = hours )
    
    results = self.rClient.getReport( typeName, reportName, start, end, condDict, grouping )
    if not results[ 'OK' ]:
      return results
    results = results[ 'Value' ]
    
    if not 'data' in results:
      return S_ERROR( 'Missing data key' )
    results = results[ 'data' ]

    uniformResult = { 'Name' : site }

    # possible statuses: Checking, Completed, Failed, Running, Done, Stalled, 
    # Killed, Matched
    for status, statusPoints in results.items():        
      
      mean = 0
      
      try:
        mean = statusPoints.values() / len( statusPoints.values() )
      except ZeroDivisionError:
        pass         

      uniformResult[ status ] = mean            

    storeRes = self._storeCommand( [ uniformResult ] )
    if not storeRes[ 'OK' ]:
      return storeRes

    return S_OK( uniformResult )

  def doCache( self ):
    '''
      Method that reads the cache table and tries to read from it. It will 
      return a list of dictionaries if there are results.
    '''
    
    params = self._prepareCommand()
    if not params[ 'OK' ]:
      return params
    site, _hours = params[ 'Value' ] 
         
    result = self.rmClient.selectJobAccountingCache( site )  
    if not result[ 'OK' ]:
      return result
    
    result = [ dict( zip( result[ 'Columns' ], res ) ) for res in result[ 'Value' ] ]        
           
    return S_OK( result )  

  def doMaster( self ):
    '''
    '''

    sites = CSHelpers.getSites()
    if not sites[ 'OK' ]:
      return sites
    sites = sites[ 'Value' ]

    gLogger.info( 'Processing %s' % ', '.join( sites ) )
 
    for site in sites:
      # time window of 1 hour
      result = self.doNew( ( site, 1 )  ) 
      if not result[ 'OK' ]:
        self.metrics[ 'failed' ].append( result )
       
    return S_OK( self.metrics )

################################################################################

class PilotAccountingCommand( AccountingCommand ):
  '''
    Accounting command that gets information of the Pilot type.
  '''

  def _storeCommand( self, results ):
    '''
      Stores the results of doNew method on the database.    
    '''

    for result in results:
      
      resQuery = self.rmClient.addOrModifyPilotAccountingCache( result[ 'Name' ], 
                                                                result[ 'Aborted' ], 
                                                                result[ 'Deleted' ], 
                                                                result[ 'Done' ],
                                                                result[ 'Failed' ] )
      if not resQuery[ 'OK' ]:
        return resQuery
    return S_OK()  

  def _prepareCommand( self ):
    '''
      AccountingCommand requires four arguments:
      - hours       : <int>
      - name : <str>  
      - elementType : <str>  
    '''

    if not 'hours' in self.args:
      return S_ERROR( 'Number of hours not specified' )
    hours = self.args[ 'hours' ]

    if not 'name' in self.args:
      return S_ERROR( '"name" is missing' )
    name = self.args[ 'name' ]

#    if not 'status' in self.args:
#      return S_ERROR( '"status" is missing' )
#    status = self.args[ 'status' ]

    if not 'elementType' in self.args:
      return S_ERROR( '"elementType" is missing' )
    elementType = self.args[ 'elementType' ]
    
    if not elementType in [ 'Site', 'Resource' ]:
      return S_ERROR( 'elementType %s not in Site, Resource' % elementType )
    
    site, ce = None, None
    
    if elementType == 'Site':
      site = name
    else:
      ce = name  

    return S_OK( ( site, ce, hours ) )#, status ) )
  
  def doNew( self, masterParams = None ):

    if masterParams is not None:
      site, ce, hours = masterParams
    else:
      params = self._prepareCommand()
      if not params[ 'OK' ]:
        return params
      site, ce, hours = params[ 'Value' ] 
 
    typeName   = 'Pilot'
    reportName = 'NumberOfPilots'
    
    condDict = {}
    if site is not None:
      condDict[ 'Site' ] = site
    if ce is not None:
      condDict[ 'GridResourceBroker' ] = ce
      
    grouping   = 'GridStatus'
    
    end   = datetime.utcnow()
    start = end - timedelta( hours = hours )
    
    results = self.rClient.getReport( typeName, reportName, start, end, condDict, grouping )
    if not results[ 'OK' ]:
      return results
    results = results[ 'Value' ]
    
    if not 'data' in results:
      return S_ERROR( 'data' )
    results = results[ 'data' ]

    uniformResult = { 'Name' : site }

    # possible statuses: Aborted, Deleted, Done, Failed
    for status, statusPoints in results.items():        
      
      mean = 0
      
      try:
        mean = statusPoints.values() / len( statusPoints.values() )
      except ZeroDivisionError:
        pass         

      uniformResult[ status ] = mean            

    storeRes = self._storeCommand( [ uniformResult ] )
    if not storeRes[ 'OK' ]:
      return storeRes

    return S_OK( uniformResult )  

  def doCache( self ):
    '''
      Method that reads the cache table and tries to read from it. It will 
      return a list of dictionaries if there are results.
    '''
    
    params = self._prepareCommand()
    if not params[ 'OK' ]:
      return params
    site, ce, _hours = params[ 'Value' ] 
         
    if site is not None:
      name = site
    else:
      name = ce
           
    result = self.rmClient.selectPilotAccountingCache( name )  
    if not result[ 'OK' ]:
      return result
    
    result = [ dict( zip( result[ 'Columns' ], res ) ) for res in result[ 'Value' ] ]        
           
    return S_OK( result )  

  def doMaster( self ):
    
    siteNames = CSHelpers.getSites()
    if not siteNames[ 'OK' ]:
      return siteNames
    siteNames = siteNames[ 'Value' ]
    
    ces = CSHelpers.getComputingElements()
    if not ces[ 'OK' ]:
      return ces
    ces = ces[ 'Value' ]
    
    for site in siteNames:
      # 2 hours of window
      result = self.doNew( ( site, None, 1 )  ) 
      if not result[ 'OK' ]:
        self.metrics[ 'failed' ].append( result )
       
    for ce in ces:
      # 2 hours of window
      result = self.doNew( ( None, ce, 1 )  ) 
      if not result[ 'OK' ]:
        self.metrics[ 'failed' ].append( result )

    return S_OK( self.metrics )  
  
#...............................................................................
#EOF
