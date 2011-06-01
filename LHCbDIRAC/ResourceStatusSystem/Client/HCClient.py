import xmlrpclib

from datetime import datetime

def checkInt( name, value ):
  if not isinstance( value, int ):
    raise Exception( 'Wrong %s, expected int.' % name )
  return value

def checkBool( name, value ):
  if not isinstance( value, bool ):
    raise Exception( 'Wrong %s, expected bool.' % name )
  return value

def checkDatetime( name, value ):
  if not isinstance( value, datetime ):
    raise Exception( 'Wrong %s, expected datetime.' % name )
  return value.strftime( "%Y-%m-%d %H:%M:%S" )

def checkDict( name, value ):
  if not isinstance( value, dict ):
    raise Exception( 'Wrong %s, expected dict.' % name )
  return value


class HCClient:
  
#############################################################################
  
  def __init__( self ):
    """ 
      Initialize HCClient, creates ServerProxy to send XML-RPCs to the HC Server.
    """
    self.rpc_srv = xmlrpclib.ServerProxy( "http://hammercloud.cern.ch/hc/xmlrpc" )

#############################################################################
   
  def getTemplates( self, detailed = 0, verbose = 0 ):
    """
      Returns a list of templates with basic information, 
      of the given application - in this case lhcb.
      
      :Parameters:
        `detailed`
          optional parameter, which will return extra information per template. 
      
      :return:
        [True, [{'template':Dict},..]] | [False, {'response':'msg'}] 
        
    """
    
    try:
      res = self.rpc_srv.getTemplates({ 'app':'lhcb', 'detailed':detailed })
      if verbose:
        print res
      return res
    except Exception, e:
      return [ False, { 'type':'SUBMISSION', 'response':e }]
   
#############################################################################   
   
  def getResults( self, test, detailed = 0, verbose = 0 ):
    """
      Returns a list of results with basic information, 
      of the given application -in this case lhcb, and a 
      given test.
      
      :Parameters:
        `test`
          test ID
              
        `detailed`
          optional parameter, which will return extra information per result. 
      
      :return:
        [True, {'results':[{},..]}] | [False, {'response':'msg'}] 
        
    """
    
    try:
      res = self.rpc_srv.getResults({ 'app':'lhcb', 'test':test, 'detailed':detailed })
      if verbose:  
        print res
      return res
    except Exception, e:
      return [ False, { 'type':'SUBMISSION', 'response':e }]
   
#############################################################################   
   
  def getSummarizedResults( self, test, detailed = 0, verbose = 0 ):
    """
      Returns a list of summarized results with basic information, 
      of the given application -in this case lhcb, and a 
      given test.
      
      :Parameters:
        `test`
          test ID
              
        `detailed`
          optional parameter, which will return extra information per summarized result. 
      
      :return:
        [True, {'summary':[{}]}] | [False, {'response':'msg'}] 
        
    """
    
    try:
      res = self.rpc_srv.getSummarizedResults({ 'app':'lhcb', 'test':test, 'detailed':detailed })
      if verbose:
        print res 
      return res
    except Exception, e:
      return [ False, { 'type':'SUBMISSION', 'response':e }]
    
#############################################################################  
  
  def getTest( self, test, verbose = 0 ):
    """
      Returns test basic information.
      
      :Parameters:
        `test`
          test ID
              
      :return:
        [True, {'test':{}}] | [False, {'response':'msg'}] 
        
    """
    try:
      res = self.rpc_srv.getTest({ 'app':'lhcb', 'test':test })
      if verbose:   
        print res
      return res
    except Exception, e:
      return [ False, { 'type':'SUBMISSION', 'response':e }]
    
#############################################################################   
   
  def sendTest( self, site, *args, **kwargs ):
    """
      Sends a test to a single site.
            
      :Parameters:
        `site`
          site name where are we going to send the test.
      
        `*args`
          TO BE IMPLEMENTED
      
      :return:
        [True,{'id':ID}]|[False,{'response':'msg'}]
      
    """
    
    site = HCSite( site )
    
    hctest = HCTest( site, **kwargs )
          
    print '  + sending %s' % hctest.toDict()
    
    try:   
      res = self.rpc_srv.createTest( hctest.toDict() )
      return res
    except Exception, e:
      return [ False, { 'type':'SUBMISSION', 'response':e }]
    
#############################################################################

#############################################################################


    
class HCSite:
  
#############################################################################  
  
  def __init__( self,
                siteName,
                resubmit_enabled      = True,
                resubmit_force        = False,
                num_datasets_per_bulk = 1,
                min_queue_depth       = 0,
                max_running_jobs      = 1 ):
    
    self.name                  = siteName
    self.resubmit_enabled      = resubmit_enabled
    self.resubmit_force        = resubmit_force
    self.num_datasets_per_bulk = num_datasets_per_bulk
    self.min_queue_depth       = min_queue_depth
    self.max_running_jobs      = max_running_jobs
    
#############################################################################

  def toDict( self ):
    
    """
      Default values at server are:
       - resubmit_enabled      : True
       - resubmit_force        : False
       - num_datasets_per_bulk : 1
       - min_queue_depth       : 0
       - max_running_jobs      : 1
    
      If we are sending values by default, we omit this and
      send only new/changed values.
    
      We also check types before creating the dict. 
    """
    
    dict = {
            'name'        : self.name
            }
    
    re = checkBool( 'resubmit_enabled', self.resubmit_enabled )
    if re != True:
      dict['resubmit_enabled'] = re

    rf = checkBool( 'resubmit_force', self.resubmit_force )
    if rf != False:
      dict['resubmit_force'] = rf
      
    ndpb = checkInt( 'num_datasets_per_bulk', self.num_datasets_per_bulk )  
    if ndpb != 1:
      dict['num_datasets_per_bulk'] = ndpb
      
    mqd = checkInt( 'min_queue_depth', self.min_queue_depth )  
    if mqd != 0:
      dict['min_queue_depth'] = mqd
      
    mrj = checkInt( 'max_running_jobs', self.max_running_jobs )  
    if mrj != 1:
      dict['max_running_jobs'] = mrj  
    
    return dict

#############################################################################


class HCTest:

#############################################################################
  
  def __init__( self, sites, template = 1, duration = 3600, description = 'RSS test', 
                starttime = None, extraargs = '' ):
    
    """
      Initialize HC Test model
      
      :Parameters:
        `template`
          template ID used to configure the test.
        
        `sites`
          HCSite or HCSites list, where the test is going to be submitted
        
        `duration`
          test duration (seconds)
          
        `description`
          description used on HC to describe test.
    """
    
    self.__app          = 'lhcb'  
    self.description    = description
    self.duration       = duration
    self.template       = template
    self.sites          = sites
    self.starttime      = starttime
    self.extraargs      = extraargs

#############################################################################

  def toDict( self ):
    
    """
      Generate dictionary with all information needed in the
      Server side to generate a test.
    """
    
    dict = {
            'app'          : self.__app,
            'description'  : str( self.description ),
            'duration'     : checkInt( 'duration',self.duration ),
            'template'     : checkInt( 'template',self.template ),
            'starttime'    : checkDatetime( 'starttime', self.starttime ),
            'extraargs'    : self.extraargs
            }
    
    sites = self.checkSitesList( self.sites )
    
    dict['sites'] = []
    for site in sites:
      dict['sites'].append( site.toDict() )
        
    return dict

#############################################################################

  def checkSitesList( self, value ):  
    
    """
      Check if the sites is either a HCSite or a list of HCSites
    """
    
    if not isinstance( value, list ):
      if not isinstance( value, HCSite ):
        raise Exception( 'Wrong sites, expected list or HCSite' )
      value = [ value ]
    else:
      if filter( lambda x: isinstance( x, HCSite ) == False, value ):  
        raise Exception( 'Wrong sites, expected list of HCSites' )
    
    return value
  
#############################################################################
  
  def rebootUniverse( self ):
    
    """
      Unless you know what are you doing, do not reboot the universe please.
    """
    
    return 42
    
#############################################################################  