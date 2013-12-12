""" lhcb_ci.chains

  LHCb Continuous Integration system libraries.

  @author: mario.ubeda.garcia@cern.ch
  
  This is awful, awful, awful... Please, forgive me for doing this. We do not have
  standard conventions to know from a Client which Services / Databases will need,
  we can guess, but will not guess OK 100% of the times. So, we have to somehow
  hardcode the chains...
  
"""


import lhcb_ci.commons
import lhcb_ci.db
import lhcb_ci.service


# Client Chains.................................................................

LINKS = {

  'Client' :
    {
     'TransformationClient' : 'TransformationSystem.Service.TransformationManager'
     },

  'Service' :
    {
     'TransformationManager' : 'TransformationSystem.DB.TransformationDB'
     }

}

#...............................................................................

class Link( object ):
  
  
  components = { 
                 'DB'      : {},
                 'Service' : {},
                 }
  
  chain = []
  
  def __init__( self, sut ):
    
    lhcb_ci.logger.debug( 'NEW %s' % sut )
    
    self.system, self.component, self.name = sut.split( '.' )
  
  
  def reset( self, dbs, services ):
    
    self.components[ 'DB'  ]     = dbs
    self.components[ 'Service' ] = services
    self.chain = []
    
  def build( self ):
    
    descendants = self.getDescendants()
    if not isinstance( descendants, list ):
      descendants = [ descendants ]  
  
    lhcb_ci.logger.debug( str( descendants ) )
  
    for descendant in descendants:
      Link( descendant ).build()
    
    return self.load()
  
  def destroy( self ):
  
    lhcb_ci.logger.debug( 'DESTROY %s' % self.name )
    lhcb_ci.logger.debug( str( self.chain ) )
    lhcb_ci.logger.debug( str( self.chain[0].name ) )
  
    for link in self.chain:
      link.unload()
    
    return self.unload()    
  
  
  def getDescendants( self ):
    
    try:
      return LINKS[ self.component ][ self.name ]
    except KeyError:
      pass
    
    if self.component == 'Client':
      nextComponent = 'Service'
      replacement   = ( 'Client', 'Manager' )
    elif self.component == 'Service':
      nextComponent = 'DB'
      replacement   = ( 'Manager', 'DB' )
    elif self.component == 'DB':
      return []  
    else:
      raise Exception( 'Unknown %s' % self.component )
    
    guessName = self.name.replace( *replacement )
    
    try:
      _ = self.components[ self.component ][ self.system ][ guessName ]
      guessName = [ guessName ]
    except KeyError:
      try:
        guessName = self.components[ nextComponent ][ self.system ]
      except KeyError:
        guessName = []
    
    return [ '%s.%s.%s' % ( self.system, nextComponent, name ) for name in guessName ]
    

  def load( self ):

    if self in self.chain:
      lhcb_ci.logger.warn( '%s already loaded' % self.name )
      return

    lhcb_ci.logger.debug( 'LOADED %s' % self.name )

    currentThreads, activeThreads = lhcb_ci.commons.trackThreads()
    
    self.currentThreads = currentThreads
    self.activeThreads  = activeThreads
    
    if self.component == 'DB':
      lhcb_ci.db.installDB( self.name )
    elif self.component == 'Service':
      lhcb_ci.service.initializeServiceReactor( self.system.replace( 'System', '' ), self.name )
  
    self.chain.append( self )
  
  
  def unload( self ):
    
    if self.component == 'DB':
      lhcb_ci.db.dropDB( self.name )
    
    threadsAfterPurge = lhcb_ci.commons.killThreads( self.currentThreads )
    
    return threadsAfterPurge == self.activeThreads

#...............................................................................
#EOF