""" lhcb_ci.chains

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
  This module provides an easy way to install/run the necessary backend to run
  a particular component. First, we need to make notation clear:
  
  ComponentX :
    o ComponentXClient.py
    o ComponentXHandler.py
    o ComponentXDB.py
  
  Connections should be made following this chain:
    Client -> Handler -> DB
  unfortunately, it is not always the case. Or maybe is followed, but links are 
  sometimes not one-to-one ( a client connecting to more than one service, or directly
  to the database ) or the naming convention is not well enforced, which means we
  have to hardcode those transitions. If the transitions are not in LINKS, we assume
  the regular chain.
  
"""


import lhcb_ci.component


# Chains........................................................................

LINKS = {

  'Client' :
    {
     'FTSClient'            : [ 'DataManagementSystem.Service.FTSManager', 
                                'RequestManagementSystem.Service.RequestManager' ],
     'TransformationClient' : 'TransformationSystem.Service.TransformationManager'
     },

  'Service' :
    {
     'TransformationManager' : 'TransformationSystem.DB.TransformationDB',
     'FTSManager'            : 'DataManagementSystem.DB.FTSDB' 
     }

}

#...............................................................................


class Link( object ):
  """ Link class
  
  This class implement recursive functions `build` and `destroy`, which given a 
  component will find out which other components it needs to be running, and load 
  / unload then in order.
  
  """
  
  # We keep a list of all components that we may need to be installed. This is used
  # to get defaults and few sanity checks. The format must be:
  # 'DB' : { 'System' : [ components ] }
  components = { 
                 'DB'      : {},
                 'Service' : {},
                 }
  
  # This class variable will be used to store the chain of links in order and 
  # delete then in reverse order.
  __chain = []
  
  
  def __init__( self, sut ):
    """ Constructor
    
    Accepts as input sut string ( Software Under Test ) which should be of the format
    System.Component.Name e.g. TransformationSystem.Client.TransformationManager
    
    """
    
    lhcb_ci.logger.debug( 'NEW %s' % sut )
    
    self.system, self.component, self.name = sut.split( '.' )
  
  
  @classmethod
  def chain( cls ):
    """ chain
    
    This classmethod returns a mutable list wich will be updated by all Links when
    loading themselves.
    
    """
    
    return cls.__chain
  
  
  def reset( self, dbs, services ):
    """ reset
    
    It allows us to make sure we are starting with a clean chain.
    
    """
    
    self.components[ 'DB'  ]     = dbs
    self.components[ 'Service' ] = services
    self.__class__.__chain = []

    
  def build( self ):
    """ build
    
    This method is the one actually building the chain. Finds the closest 
    descendants for the given SUT and iterates over them building them recursively.
    As a result, the last descendants are loaded first ( DBs will go first, then
    services, etc... ).
    
    """
    
    descendants = self.__getDescendants()
    if not isinstance( descendants, list ):
      descendants = [ descendants ]  
  
    lhcb_ci.logger.debug( 'Descendants : %s' % str( descendants ) )
  
    for descendant in descendants:
      link = Link( descendant )
      res = link.build()
      if not res[ 'OK' ]:
        return res
    
    return self.__load()
  
  
  def destroy( self ):
    """ destroy
    
    This method unloads all components stored in the chain.
    
    """
  
    lhcb_ci.logger.debug( 'DESTROY %s' % self.name )
  
    for link in self.chain():
      res = link.__unload()
      if not res[ 'OK' ]:
        return res 
    
    return self.__unload()  
  
  
  #.............................................................................
  # Please, do not mess the following methods
  
  
  def __getDescendants( self ):
    """ __getDescendants
    
    This method tries to find the closest descendants for the given SUT as follows:
    
    1) if SUT is on LINKS, returns its hardcoded descendants
    2) if not, follows the convention to get name of descendant ( guessName )
    2.1) if descendant exists, we are good
    2.2) if not, returns all components for the next component level ( if SUT is
         a client, returns all Services of given System ).
    
    """
    
    try:
      return LINKS[ self.component ][ self.name ]
    except KeyError:
      pass
    
    if self.component == 'Client':
      nextComponent = 'Service'
      guessName = self.name.replace( 'Client', 'Manager' )
    elif self.component == 'Service':
      nextComponent = 'DB'
      # Let's remove Manager from the Service name to avoid having to hardcode
      # tons of exceptions in LINKS
      # FIXME: hardcode the exceptions and slowly change names in code.
      guessName = self.name.replace( 'Manager', '' ) + 'DB'
    elif self.component == 'DB':
      return []  
    else:
      raise Exception( 'Unknown %s' % self.component )
    
    try:
      if guessName in self.components[ self.component ][ self.system ]:
        guessComponents = [ guessName ]
      else:
        guessComponents = self.components[ nextComponent ][ self.system ]
    except KeyError:
      guessComponents = []

    return [ '%s.%s.%s' % ( self.system, nextComponent, name ) for name in guessComponents ]
    

  def __load( self ):
    """ __load
    
    This method loads the given component if has not been loaded first. Each component
    is handled differently, so we have to make some little exceptions ( if-else ).
    Also, takes care of the threads.. sometimes are a bit problematic.
    
    """

    if self in self.chain():
      lhcb_ci.logger.warn( '%s already loaded' % self.name )
      return

    lhcb_ci.logger.debug( 'LOADED %s' % self.name )
    
    self.componentObj = lhcb_ci.component.Component( self.system, self.component, self.name ) 
    res = self.componentObj.run()

    self.chain().append( self )
    return res
    
  
  def __unload( self ):
    """ __unload
    
    This method undoes what __load does.
    
    """
    
    lhcb_ci.logger.debug( 'UNLOADED %s' % self.name )
    
    return self.componentObj.stop()


#...............................................................................
#EOF
