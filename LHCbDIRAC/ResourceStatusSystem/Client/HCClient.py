#''' HCClient 
#
#  Client to connect and interact with HammerCloud.
#
#'''
#
#import xmlrpclib
#
#from datetime import datetime
#
#__RCSID__ = "$Id$"
#
#def checkInt( name, value ):
#  '''
#  If value is not of type `int`, raise Exception
#  '''
#  
#  if not isinstance( value, int ):
#    raise Exception( 'Wrong %s, expected int.' % name )
#  return value
#
#def checkBool( name, value ):
#  '''
#  If value is not of type `bool`, raise Exception
#  '''
#  
#  if not isinstance( value, bool ):
#    raise Exception( 'Wrong %s, expected bool.' % name )
#  return value
#
#def checkDatetime( name, value ):
#  '''
#  If value is not of type `datetime`, raise Exception
#  '''
#    
#  if not isinstance( value, datetime ):
#    raise Exception( 'Wrong %s, expected datetime.' % name )
#  return value.strftime( "%Y-%m-%d %H:%M:%S" )
#
#def checkDict( name, value ):
#  '''
#  If value is not of type `dict`, raise Exception
#  '''
#  
#  if not isinstance( value, dict ):
#    raise Exception( 'Wrong %s, expected dict.' % name )
#  return value
#
#################################################################################
## END TYPE checkers
#################################################################################
#
#class HCClient:
#  '''
#  Class HCClient. It creates a connection to the HammerCloud XMLRPC server.
#  
#  The available methods are:
#  
#  - getTemplate
#  - getResults
#  - getSummarizedResults
#  - getTest
#  - sendTest
#  
#  If something goes wrong on the XMLRPC server, but it returns an message, it is 
#  expected a message with the following format:
#  
#    ( False,{'type': xyz ,'response': xyz })
#    
#  Being the type one of the following:
#  
#  - FORMAT   : wrong type
#  - MISSING  : parameter missing
#  - UNKNOWN  : something else happened
#  
#  plus if there is a connection problem, the client returns the type
#  
#  - SUBMISSION : problem with the connection 
#  '''
#  
#################################################################################  
#  
#  # URL for the HC XML RPC server
#  __RPC_SERVER__ = "http://hammercloud.cern.ch/hc/xmlrpc"
#  
#################################################################################
#  
#  def __init__( self ):
#    ''' 
#    Initialize HCClient, creates ServerProxy to send XML-RPCs to the HC Server.
#    '''
#    
#    self.rpc_srv = xmlrpclib.ServerProxy( self.__RPC_SERVER__ )
#
#################################################################################
#   
#  def getTemplates( self, detailed = 0, verbose = 0 ):
#    '''
#    Returns a list of templates with basic information, 
#    of the given application - in this case lhcb.
#      
#    :params:
#      :attr: `detailed` : int - if 1 returns extra information per template ( optional )  
#      
#      :attr: `verbose` : int - if 1 prints result ( optional )
#      
#    :return:
#      [True, [{'template':Dict},..]] | [False, {'response':'msg'}]     
#    '''
#    
#    try:
#    
#      res = self.rpc_srv.getTemplates({ 'app' : 'lhcb', 'detailed' : detailed })
#      if verbose:
#        print res
#      return res
#    
#    except Exception, e:
#      return [ False, { 'type' : 'SUBMISSION', 'response' : e } ]
#   
#################################################################################   
#   
#  def getResults( self, test, detailed = 0, verbose = 0 ):
#    '''
#    Returns a list of results with basic information, of the given application 
#    -in this case lhcb, and a given test.
#    
#    :params:
#      :attr: `test` : int - test ID
#    
#      :attr: `detailed` : int - if 1 returns extra information per result ( optional )  
#      
#      :attr: `verbose` : int - if 1 prints result ( optional )
#      
#    :return:
#        [True, {'results':[{},..]}] | [False, {'response':'msg'}] 
#    '''
#    
#    try:
#      
#      res = self.rpc_srv.getResults({ 'app':'lhcb', 'test':test, 'detailed':detailed })
#      if verbose:  
#        print res
#      return res
#    
#    except Exception, e:
#      return [ False, { 'type' : 'SUBMISSION', 'response' : e } ]
#   
#################################################################################   
#   
#  def getSummarizedResults( self, test, detailed = 0, verbose = 0 ):
#    '''
#    Returns a list of summarized results with basic information, 
#    of the given application -in this case lhcb, and a 
#    given test.
#      
#    :params:
#      :attr: `test` : int - test ID
#    
#      :attr: `detailed` : int - if 1 returns extra information per summarized 
#                 result ( optional )  
#      
#      :attr: `verbose` : int - if 1 prints result ( optional )    
#      
#    :return:
#      [True, {'summary':[{}]}] | [False, {'response':'msg'}]  
#    '''
#    
#    try:
#      
#      res = self.rpc_srv.getSummarizedResults({ 'app':'lhcb', 'test':test, 'detailed':detailed })
#      if verbose:
#        print res 
#      return res
#    
#    except Exception, e:
#      return [ False, { 'type':'SUBMISSION', 'response':e }]
#    
#################################################################################  
#  
#  def getTest( self, test, verbose = 0 ):
#    '''
#    Returns test basic information.
#      
#    :params:
#      :attr: `test` : int - test ID
#
#      :attr: `verbose` : int - if 1 prints result ( optional )    
#                    
#    :return:
#      [True, {'test':{}}] | [False, {'response':'msg'}]    
#    '''
#    
#    try:
#    
#      res = self.rpc_srv.getTest({ 'app':'lhcb', 'test':test })
#      if verbose:   
#        print res
#      return res
#    
#    except Exception, e:
#      return [ False, { 'type':'SUBMISSION', 'response':e }]
#    
#################################################################################   
#   
#  def sendTest( self, site, *args, **kwargs ):
#    '''
#    Sends a test to a single site.
#            
#    :params:
#      :attr: `site` : string - site name where are we going to send the test.
#      
#      :attr: `*args`
#      
#      :attr: `**kwargs` : dict - accepts the following:
#                 template : int
#                 duration : int
#                 description : string
#                 starttime : datetime
#                 extraargs : string
#        
#    :return:
#      [True,{'id':ID}]|[False,{'response':'msg'}]
#      
#    '''
#    
#    site = HCSite( site )
#    
#    hctest = HCTest( site, **kwargs )
#          
#    print '  + sending %s' % hctest.toDict()
#    
#    try:   
#      res = self.rpc_srv.createTest( hctest.toDict() )
#      return res
#    except Exception, e:
#      return [ False, { 'type':'SUBMISSION', 'response':e }]
#    
#################################################################################
#
#class HCSite:
#  '''
#  Class HCSite. Its only purpose is to ensure the validity and format of the
#  parameters being sent to the XMLRPC server.
#  '''
#  
#################################################################################  
#  
#  def __init__( self,
#                siteName,
#                resubmit_enabled      = True,
#                resubmit_force        = False,
#                num_datasets_per_bulk = 1,
#                min_queue_depth       = 0,
#                max_running_jobs      = 1 ):
#    '''
#    Class constructor
#    
#    :params:
#      :attr: `siteName` : string - name of the site
#      :attr: `resubmit_enabled` : bool - HC paramter that enables resubmission
#      :attr: `resubmit_force` : bool - HC paramter that forces resubmission
#      :attr: `num_datasets_per_bulk` : int - nr. of jobs sent at time0
#      :attr: `min_queue_depth` : int - max number of submitted not running jobs
#      :attr: `max_running_jobs` : int - max number of running jobs         
#    '''  
#    
#    self.name                  = siteName
#    self.resubmit_enabled      = resubmit_enabled
#    self.resubmit_force        = resubmit_force
#    self.num_datasets_per_bulk = num_datasets_per_bulk
#    self.min_queue_depth       = min_queue_depth
#    self.max_running_jobs      = max_running_jobs
#    
#################################################################################
#
#  def toDict( self ):
#    '''
#    Default values at server are:
#    - resubmit_enabled      : True
#    - resubmit_force        : False
#    - num_datasets_per_bulk : 1
#    - min_queue_depth       : 0
#    - max_running_jobs      : 1
#    
#    If we are sending values by default, we omit this and
#    send only new/changed values.
#    
#    We also check types before creating the dict. 
#    '''
#    
#    dict = {
#            'name'        : self.name
#            }
#    
#    re = checkBool( 'resubmit_enabled', self.resubmit_enabled )
#    if re != True:
#      dict['resubmit_enabled'] = re
#
#    rf = checkBool( 'resubmit_force', self.resubmit_force )
#    if rf != False:
#      dict['resubmit_force'] = rf
#      
#    ndpb = checkInt( 'num_datasets_per_bulk', self.num_datasets_per_bulk )  
#    if ndpb != 1:
#      dict['num_datasets_per_bulk'] = ndpb
#      
#    mqd = checkInt( 'min_queue_depth', self.min_queue_depth )  
#    if mqd != 0:
#      dict['min_queue_depth'] = mqd
#      
#    mrj = checkInt( 'max_running_jobs', self.max_running_jobs )  
#    if mrj != 1:
#      dict['max_running_jobs'] = mrj  
#    
#    return dict
#
#################################################################################
#
#class HCTest:
#  '''
#  Class HCTest. Its only purpose is to ensure the validity and format of the
#  parameters being sent to the XMLRPC server.
#  '''
#
#################################################################################
#  
#  def __init__( self, sites, template = 1, duration = 3600, 
#                description = 'RSS test', starttime = None, extraargs = '' ):
#    '''
#    Initialize HC Test model
#      
#    :params:
#      :attr: `sites` : string / list(string) - site(s) where the test is sent
#    
#      :attr: `template` : int - HC template used to generate the test
#      
#      :attr: `duration` : int - test duration in seconds
#      
#      :attr: `description` : string - meaningful description for the test
#      
#      :attr: `starttime` : datetime - test starttime
#      
#      :attr: `extraargs` : string - extraargs for the HC algorithm
#    '''
#    
#    self.__app          = 'lhcb'  
#    self.description    = description
#    self.duration       = duration
#    self.template       = template
#    self.sites          = sites
#    self.starttime      = starttime
#    self.extraargs      = extraargs
#
#################################################################################
#
#  def toDict( self ):
#    '''
#    Generate dictionary with all information needed in the
#    server side to generate a test.
#    '''
#    
#    dict = {
#            'app'          : self.__app,
#            'description'  : str( self.description ),
#            'duration'     : checkInt( 'duration',self.duration ),
#            'template'     : checkInt( 'template',self.template ),
#            'starttime'    : checkDatetime( 'starttime', self.starttime ),
#            'extraargs'    : self.extraargs
#            }
#    
#    sites = self.checkSitesList( self.sites )
#    
#    dict['sites'] = []
#    for site in sites:
#      dict['sites'].append( site.toDict() )
#        
#    return dict
#
#################################################################################
#
#  def checkSitesList( self, value ):  
#    '''
#    Check if the sites is either a HCSite or a list of HCSites
#    '''
#    
#    if not isinstance( value, list ):
#      if not isinstance( value, HCSite ):
#        raise Exception( 'Wrong sites, expected list or HCSite' )
#      value = [ value ]
#    else:
#      if filter( lambda x: isinstance( x, HCSite ) == False, value ):  
#        raise Exception( 'Wrong sites, expected list of HCSites' )
#    
#    return value
#  
#################################################################################
##EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF  