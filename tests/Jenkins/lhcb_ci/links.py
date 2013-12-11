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
  
  def __init__( self, system, component, name ):
    
    self.component = component
    self.system    = system
    self.name      = name
  
    
  def build( self ):
    
    descendants = self.getDescendants()
    if not isinstance( descendants, list ):
      descendants = list( descendants )  
  
    for descendant in descendants:
      Link( *descendant ).build()
    
    return self.load()
  
  def destroy( self ):
  
    for link in self.chain:
      link.unload()
    
    return self.unload()    
  
  
  def getDescendants( self ):
    
    try:
      return self.name in LINKS[ self.component ][ self.name ]
    except KeyError:
      pass       
    
    if self.component == 'Client':
      nextComponent = 'Service'
      replacement   = ( 'Client', 'Manager' )
    elif self.component == 'DB':
      nextComponent = 'DB'
      replacement   = ( 'Manager', 'DB' )
    else:
      raise Exception( 'Unknown' )
    
    guessName = self.name.replace( *replacement )
    
    try:
      _ = self.components[ self.component ][ guessName ]
      guessName = list( guessName )
    except KeyError:
      try:
        guessName = self.components[ nextComponent ]
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
      lhcb_ci.service.initializeServiceReactor( self.system, self.name )
  
    self.chain.append( self )
  
  
  def unload( self ):
    
    if self.component == 'DB':
      lhcb_ci.db.dropDB( self.name )
    
    threadsAfterPurge = lhcb_ci.commons.killThreads( self.currentThreads )
    
    return threadsAfterPurge == self.activeThreads

#...............................................................................
#EOF