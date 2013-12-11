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

Chains = {

# TransformationSystem..........................................................
  
  'TransformationClient' : { 
                            'db'      : [ 'TransformationDB' ],
                            'service' : [ 'Transformation/TransformationManager' ] 
                           },

}

#...............................................................................


class Chain( object ):
  """ Chain module
  
  Given a client name, gets the databases and services it needs to run. It installs
  fresh databases and run services when issuing load. Unload, undoes the load 
  action.
    
  """
  
  
  def __init__( self, componentName ):
    
    self.chain = Chains[ componentName ]
    
    currentThreads, activeThreads = lhcb_ci.commons.trackThreads()
    
    self.currentThreads = currentThreads
    self.activeThreads  = activeThreads


  @staticmethod
  def componentsList( component ):
    
    components = []
    
    if component in self.chain:
      components = chain[ component ]
      if not isinstance( components, list ):
        components = list( components )
    
    return components  
  
  
  def load( self ):
  
    for dbName in componentsList( 'db' ):
      res = lhcb_ci.db.installDB( dbName )
      if not res[ 'OK' ]:
        return res
  
    for serviceName in componentsList( 'service' ):
      res = lhcb_ci.service.initializeServiceReactor( *serviceName.split( '/' ) )
      if not res[ 'OK' ]:
        return res 
    
    return res   
  
  
  def unload( self ):

    for db in componentsList( 'db' ):
      lhcb_ci.db.dropDB( dbName )
    
    threadsAfterPurge = lhcb_ci.commons.killThreads( self.currentThreads )
    
    return threadsAfterPurge == self.activeThreads


#...............................................................................
#EOF