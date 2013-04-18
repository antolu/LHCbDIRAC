''' LHCbDIRAC.ResourceStatusSystem.Service.PublisherHandler

   initializePublisherHandler
   PublisherHandler.__bases__:
     DIRAC.Core.DISET.RequestHandler.RequestHandler

'''

from types                                                  import NoneType

from DIRAC                                                  import gLogger, S_OK
from DIRAC.Core.DISET.RequestHandler                        import RequestHandler
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Utilities                   import CSHelpers, RssConfiguration 

from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

__RCSID__ = '$Id$'
rsClient  = None
rmClient  = None

def initializePublisherHandler( _serviceInfo ):
  '''
    Handler initialization in the usual horrible way.
  '''
  global rsClient 
  rsClient = ResourceStatusClient()
  
  global rmClient 
  rmClient = ResourceManagementClient()
  
  return S_OK()
  
class PublisherHandler( RequestHandler ):
  '''
    RPCServer used to deliver data to the web portal.
      
    So far it contains only examples, probably some of them will be used by the 
    web portal, but not all of them.
  '''  
  
  ## CSHelpers methods #########################################################
  
  types_getSites = []
  def export_getSites( self ):  
    '''
      Returns list of all sites considered by RSS
    '''
    gLogger.info( 'getSites' )
    return CSHelpers.getSites()

  types_getElementStatuses = [ str, ( str, list, NoneType ), ( str, list, NoneType ), 
                            ( str, list, NoneType ), ( str, list, NoneType ),
                            ( str, list, NoneType ) ]
  def export_getElementStatuses( self, element, name, elementType, statusType, status, tokenOwner ):
      return rsClient.selectStatusElement( element, 'Status', name = name, elementType = elementType,
                                           statusType = statusType, status = status,
                                           tokenOwner = tokenOwner ) 

  types_getElementHistory = [ str, ( str, list, NoneType ), ( str, list, NoneType ), 
                            ( str, list, NoneType ) ]
  def export_getElementHistory( self, element, name, elementType, statusType ):
      return rsClient.selectStatusElement( element, 'History', name = name, elementType = elementType,
                                           statusType = statusType,
                                           meta = { 'columns' : [ 'Status', 'DateEffective', 'Reason' ] } ) 

  types_getElementPolicies = [ str, ( str, list, NoneType ), ( str, list, NoneType ) ]
  def export_getElementPolicies( self, element, name, statusType ):
    return rmClient.selectPolicyResult( element = element, name = name, 
                                        statusType = statusType, 
                                        meta = { 'columns' : [ 'Status', 'PolicyName', 'DateEffective',
                                                               'LastCheckTime', 'Reason' ]} )

  types_getNodeStatuses = []
  def export_getNodeStatuses( self ):
      return rsClient.selectStatusElement( 'Node', 'Status' ) 

  types_getResourceStatuses = []
  def export_getResourceStatuses( self ):
      return rsClient.selectStatusElement( 'Resource', 'Status' ) 

  types_getResources = []
  def export_getResources( self ):  
    '''
      Returns list of all resources considered by RSS
    '''
    gLogger.info( 'getResources' )
    return CSHelpers.getResources()

  types_getNodes = []
  def export_getNodes( self ):  
    '''
      Returns list of all nodes considered by RSS
    '''
    gLogger.info( 'getNodes' )
    return CSHelpers.getNodes()
  
  ## RssConfiguration methods ##################################################
  
  types_getPolicies = []
  def export_getPolicies( self ):  
    '''
      Returns list of all nodes considered by RSS
    '''
    gLogger.info( 'getPolicies' )
    return RssConfiguration.getPolicies()
  
  types_getValidStatus = []
  def export_getValidStatus( self ):
    #TODO: return it from RssConfiguration
    pass
  
  #TODO: set the types as in the previous method
  def export_getValidStatusTypes( self ):
    #TODO: return in from RssConfiguration().getConfig...
    pass
  
  ## Status methods ############################################################
  
  # Element must be a string, names must be either a list, a string or None
  types_selectStatusElement = [ str, ( str, list, NoneType )]
  def export_selectStatusElement( self, element, name ):
    '''
      Given an element ( Site, Resource, Node ) and a name, we query the database
      through the client.
      
    '''
    
    gLogger.info( 'selectStatusElement ( %s, %s )' % ( element, name ) )

#    We can validate the element, if it is wrong, this query will not go to the
#    database. Needed ?
    
#    validElements = RssConfiguration.getValidElements()
#    if not element in validElements:
#      message = '"%s" not in validElements' % element
#      gLogger.error( message )
#      return S_ERROR( message )
    
    #selectStatusElement( self, element, tableType, name = None, statusType = None, 
    #                         status = None, elementType = None, reason = None, 
    #                         dateEffective = None, lastCheckTime = None, 
    #                         tokenOwner = None, tokenExpiration = None, meta = None ):
    return rsClient.selectStatusElement( element, 'Status', name )   

  types_selectStatusElementExtended = [ str, ( str, list, NoneType ), ( str, list, NoneType ),
                                        ( str, list, NoneType ), ( str, list, NoneType ) ]
  def export_selectStatusElementExtended( self, element, name, statusType, status,
                                          elementType ):
    '''
      Given an element ( Site, Resource, Node ) and a name, we query the database
      through the client.
      
    '''
    
    gLogger.info( 'selectStatusElementExtended ( %s, %s )' % ( element, name ) )
    
    #selectStatusElement( self, element, tableType, name = None, statusType = None, 
    #                         status = None, elementType = None, reason = None, 
    #                         dateEffective = None, lastCheckTime = None, 
    #                         tokenOwner = None, tokenExpiration = None, meta = None ):
    return rsClient.selectStatusElement( element, 'Status', name, statusType, status,
                                         elementType )   

    
  #TODO: Andrew, this is all yours ;)  
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF 