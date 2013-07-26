#""" lhcb_ci.test.test_configure
#
#  LHCb Continuous Integration system libraries.
#
#  @author: mario.ubeda.garcia@cern.ch
#  
#"""
#
#
#import collections
#import os
#
#import lhcb_ci.agent
#import lhcb_ci.basecase 
#import lhcb_ci.db
#import lhcb_ci.service
#
#
#class Configure_Test( lhcb_ci.basecase.Agent_TestCase ):
#  """ Configure_Test
#  
#  This class contains dirty & sticky tests. The configuration steps have been
#  transformed into simple unittests, which are run here. Dirty & sticky because
#  the tests will alter the CS structure, adding the necessary configuration 
#  parameters to be able to run the rest of the tests. 
#  
#  Disclaimer: do not change the name of the tests, as some of them need to
#  run in order.
#  
#  """
#
#  def test_configure_service( self ):
#    """ test_configure_service
#    
#    Test that we can configure services on an "empty CS".
#    """
#
#    self.logTestName()
#
#    for systemName, services in self.swServices.iteritems():
#      
#      # Master Configuration is already on place.
#      if systemName == 'Configuration':
#        self.log.debug( 'Skipping Master Configuration' )
#        continue 
#      
#      for serviceName in services:
#      
#        if self.isException( serviceName ):
#          continue
#      
#        res = lhcb_ci.service.configureService( systemName, serviceName )
#        self.assertDIRACEquals( res[ 'OK' ], True, res )         
#        
#
#  def test_configured_service_ports( self ):
#    """ test_configured_service_ports
#    
#    Tests that the services configuration does not overlap, namely ports.
#    
#    """
#    
#    self.logTestName()
#    
#    ports = {}
#    
#    for system, services in self.swServices.iteritems():
#      
#      for service in services:
#      
#        serviceName = '%s/%s' % ( system, service )
#      
#        if self.isException( service ):
#          try:
#            ports[ 'xxxx' ].append( serviceName )
#          except KeyError:
#            ports[ 'xxxx' ] = [ serviceName ]  
#          continue  
#      
#        port = lhcb_ci.service.getServicePort( system, service )
#        _msg = '%s:%s already taken by %s' % ( serviceName, port, ports.get( port,'' ) )
#        
#        # If false, it raises. 
#        self.assertTrue( port not in ports, _msg )
#        
#        ports[ port ] = serviceName
#
#    # Sort port numbers
#    sortedPorts = ports.keys()
#    sortedPorts.sort()
#
#    # Write ports report
#    with open( os.path.join( lhcb_ci.workspace, self.reportPath() ), 'w' ) as servFile:
#      for port in sortedPorts:
#        servFile.write( '%s : %s\n' % ( port, ports[ port ] ) )  
#
#
#  def test_configured_service_authorization( self ):
#    """ test_configured_service_authorization
#    
#    Tests that the services default configuration sets a minimum security level.
#    This means, any / all by Default is forbidden, all the security properties
#    must be valid ones and discourages from the usage of any / all in general.
#    
#    """
#
#    self.logTestName()
#    
#    securityProperties = set( lhcb_ci.service.getSecurityProperties() )
#    
#    authRules = collections.defaultdict( dict )
#    
#    for system, services in self.swServices.iteritems():
#      
#      for service in services:
#        
#        serviceName = '%s/%s' % ( system, service )
#        
#        if self.isException( service ):
#          authRules[ serviceName ] = { 'xxxx' : 'skipped' }
#          continue
#        
#        
#        self.log.debug( '%s authorization rules' % serviceName )
#        
#        res = lhcb_ci.service.getServiceAuthorization( system, service )
#        self.assertDIRACEquals( res[ 'OK' ], True, res )
#        authorization = res[ 'Value' ]
#        
#        self.assertTrue( authorization, 'Empty authorization rules not allowed %s' % serviceName )
#        for method, secProp in authorization.iteritems():
#          
#          if not isinstance( secProp, str ):
#            self.log.debug( 'Found non str authorization rule for %s.%s' % ( serviceName, method ) )
#            continue
#          
#          # lower case, just in case
#          method  = method.lower()
#          secProp = set( secProp.lower().replace( ' ','' ).split( ',' ) )
#          
#          if method == 'default':
#            self.assertFalse( 'all' in secProp, 'Default : All authorization rule is FORBIDDEN %s' % serviceName )
#            self.assertFalse( 'any' in secProp, 'Default : Any authorization rule is FORBIDDEN %s' % serviceName )
#          
#          if not ( secProp & set( [ 'all', 'any', 'authenticated' ] ) ):
#            self.assertTrue( secProp <= securityProperties, '%s is an invalid SecProp %s' % ( secProp, serviceName ) )
#          elif secProp & set( [ 'all', 'any' ] ):
#            self.log.warning( '%s.%s has all/any no SecurityProperty' % ( serviceName, method ) )
#
#          authRules[ serviceName ][ method ] = ', '.join([ sp for sp in secProp ])
#            
#           
#    # Write authorization report
#    with open( os.path.join( lhcb_ci.workspace, self.reportPath() ), 'w' ) as servFile:
#      for servName, authRule in authRules.iteritems():
#        servFile.write( '%s\n' % servName )
#        for method, secProp in authRule.iteritems():
#          servFile.write( '  %s : %s\n' % ( method.ljust( 40 ), secProp ) )
#
#  
#  def test_configure_agent( self ):
#    """ test_configure_agent
#    """
#    
#    self.logTestName()
#    
#    for systemName, agents in self.swAgents.iteritems():
#            
#      for agentName in agents:
#      
#        if self.isException( agentName ):
#          continue
#      
#        res = lhcb_ci.agent.configureAgent( systemName, agentName )
#        self.assertDIRACEquals( res[ 'OK' ], True, res )    
#
#
#  #.............................................................................
#  # Nosetests attrs
#  
#  test_configure_agent.configure = 1
#  test_configure_agent.agent     = 1
#
#
##...............................................................................
##EOF