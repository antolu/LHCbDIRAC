# $HeadURL$
''' LHCb Site Queues SAM Test Module

  Corresponds to SAM test CE-lhcb-queues.
'''

import os

from DIRAC import S_OK, S_ERROR

from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM import ModuleBaseSAM

__RCSID__ = "$Id$"

class SiteQueues( ModuleBaseSAM ):

  def __init__( self ):
    """ Standard constructor for SAM Module
    """
    ModuleBaseSAM.__init__( self )
    
    self.logFile  = 'sam-queues.log'
    self.testName = 'CE-lhcb-queues'

  def _execute( self ):
    '''
      The main execution method of the SiteQueues module.
    '''

    result = self.__checkConfig()
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
    samNode = result[ 'Value' ]

    ldapLog = 'ldap.log'
    fopen  = open( ldapLog, 'a' )
    
    result = self.__checkGlueHostBenchmarkSI00( samNode )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
    fopen.write( result[ 'Value' ] )
    
    result = self.__checkGlueCEPolicyMaxCPUTime( samNode )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
    fopen.write( result[ 'Value' ] )
    
    fopen.close()

    result = self.__checkQueueBenchmark( ldapLog )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    fopen = open( ldapLog, 'w' )
    
    result = self.__checkCPUScalingReferenceSI00( samNode )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
    
    fopen.write( result['Value'] )

    #FIXME: this is repeated, find reason why !!    
    result = self.__checkGlueCEPolicyMaxCPUTime( samNode )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )

    fopen.write( result[ 'Value' ] )
    fopen.close()

    result = self.__checkQueueScaling( ldapLog )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
    
    self.log.info( 'Test %s completed successfully' % self.testName )
    self.setApplicationStatus( '%s Successful' % self.testName )
    
    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 10)', 'ok' )

  ##############################################################################
  # Protected methods
  
  def __checkConfig( self ):
    '''
       Checks LCG_GFAL_INFOSYS and CE
    '''

    if 'LCG_GFAL_INFOSYS' in os.environ:
      self.log.info( 'LCG_GFAL_INFOSYS = %s' % ( os.environ[ 'LCG_GFAL_INFOSYS' ] ) )
    else:
      result = S_ERROR( 'Could not execute queue check' )
      result[ 'Description' ] = 'LCG_GFAL_INFOSYS is not defined'
      result[ 'SamResult' ]   = 'error'
      #return self.finalize( 'LCG_GFAL_INFOSYS is not defined', 'Could not execute queue check', 'error' )
      return result

    if not 'CE' in self.runInfo:
      result = S_ERROR( 'no CE' )
      result[ 'Description' ] = 'Could not get current CE'
      result[ 'SamResult' ]   = 'error'

      return result

    return S_OK( self.runInfo[ 'CE' ] )
        
  def __checkGlueHostBenchmarkSI00( self, samNode ):
    '''
       Checks GlueHostBenchmarkSI00
    '''
    
    cmd = 'ldapsearch -h ' + os.environ[ 'LCG_GFAL_INFOSYS' ] 
    cmd += ' -b "mds-vo-name=local,o=grid" -x -LLL "(GlueSubClusterUniqueID=' + samNode 
    cmd += ')" GlueHostBenchmarkSI00 | grep GlueHostBenchmarkSI00'
    
    result = self.runCommand( 'Checking current CE GlueHostBenchmarkSI00', cmd )
    if not result[ 'OK' ]:
      result[ 'Description' ] = 'Could not perform ldap query for GlueHostBenchmarkSI00'
      result[ 'SamResult' ]   = 'error'
      
    return result  

  def __checkGlueCEPolicyMaxCPUTime( self, samNode ):
    '''
       Checks GlueCEPolicyMaxCPUTime
    '''

    cmd = 'ldapsearch -h ' + os.environ[ 'LCG_GFAL_INFOSYS' ] 
    cmd += ' -b "mds-vo-name=local,o=grid" -x -LLL "(& (GlueForeignKey=GlueClusterUniqueID=' 
    cmd += samNode + ') (GlueCEAccessControlBaseRule=VO:lhcb))" GlueCEUniqueID '
    cmd += 'GlueCEPolicyMaxCPUTime | grep GlueCEPolicyMaxCPUTime'
    
    result = self.runCommand( 'Checking current CE GlueCEPolicyMaxCPUTime', cmd )
    if not result[ 'OK' ]:
      result[ 'Description' ] = 'Could not perform ldap query for GlueCEPolicyMaxCPUTime'
      result[ 'SamResult' ]   = 'error'       
        
    return result

  def __checkQueueBenchmark( self, ldapLog ):
    '''
       Analyzes logs of GlueCEPolicyMaxCPUTime and GlueCEPolicyMaxCPUTime 
    '''
    
    queueCount = 1
    for line in open( ldapLog, 'r' ):
      queueCount = queueCount + line.count( 'Time' )
   
    cmd  = 'awk \'{ i=i+1; '
    cmd += 'if ( $1 == "GlueCEPolicyMaxCPUTime:" ) CPUTIME = $2 ;'
    cmd += 'if ( $1 == "GlueHostBenchmarkSI00:" ) SI00 = $2 ;'
    cmd += 'if ( i == 2 ) {'
    cmd += 'if ( CPUTIME*SI00*60/500 > 270000 ) {i2= 0} ;'
    cmd += 'if ( CPUTIME*SI00*60/500 < 270000 ) {i2= 1} ;'
    cmd += '} ;'
    cmd += 'if ( i == 3 ) {'
    cmd += 'if ( CPUTIME*SI00*60/500 > 270000 ) {i3= 0} ;'
    cmd += 'if ( CPUTIME*SI00*60/500 < 270000 ) {i3= 1} ;'
    cmd += '} ;'
    cmd += 'if ( i == 4 ) {'
    cmd += 'if ( CPUTIME*SI00*60/500 > 270000 ) {i4= 0} ;'
    cmd += 'if ( CPUTIME*SI00*60/500 < 270000 ) {i4= 1} ;'
    cmd += '} ;'
    cmd += 'if ( i == 5 ) {'
    cmd += 'if ( CPUTIME*SI00*60/500 > 270000 ) {i5= 0} ;'
    cmd += 'if ( CPUTIME*SI00*60/500 < 270000 ) {i5= 1} ;'
    cmd += '} ;'
    cmd += 'if ( i == 6 ) {'
    cmd += 'if ( CPUTIME*SI00*60/500 > 270000 ) {i6= 0} ;'
    cmd += 'if ( CPUTIME*SI00*60/500 < 270000 ) {i6= 1} ;'
    cmd += '} ;'
    cmd += 'if ( i == 7 ) {'
    cmd += 'if ( CPUTIME*SI00*60/500 > 270000 ) {i7= 0} ;'
    cmd += 'if ( CPUTIME*SI00*60/500 < 270000 ) {i7= 1} ;'
    cmd += '} ;'
    cmd += 'if ( i == tot ){'
    cmd += 'if ( i2 == 0 || i3 == 0 || i4 == 0 || i5 == 0 || i6 == 0 || i7 == 0 ){'
    cmd += 'print 0 ;'
    cmd += '} ;'
    cmd += 'if ( i2 == 1 && i3 == 1 && i4 == 1 && i5 == 1 && i6 == 1 && i7 == 1 ){'
    cmd += 'print 1 ;'
    cmd += '}}}'
    cmd += 'tot=' + str( queueCount ) + ' i2=1 i3=1 i4=1 i5=1 i6=1 i7=1 ' + ldapLog  
    
    result = self.runCommand( 'Checking site queues', cmd )
    if not result[ 'OK' ]:
      result[ 'Description' ] = 'Could not check site queues'
      result[ 'SamResult' ]   = 'error'
      return result

    output = result[ 'Value' ]
    if not output.strip() == '0':
      result = S_ERROR( 'Status INFO (= 20)' )
      result[ 'Description' ] = 'LHCb queue length does not satisfy the minimum requirement of 39h of 5002KSI'
      result[ 'SamResult' ]   = 'info' 
      
    return result  

  def __checkCPUScalingReferenceSI00( self, samNode ):
    '''
       Checks CPUScalingReferenceSI00
    '''
    
    cmd = 'ldapsearch -h ' + os.environ[ 'LCG_GFAL_INFOSYS' ] 
    cmd += ' -b "mds-vo-name=local,o=grid" -x -LLL "(& (GlueForeignKey=GlueClusterUniqueID='
    cmd += samNode + ') (GlueCEAccessControlBaseRule=VO:lhcb))" GlueCECapability | grep ' 
    cmd += 'CPUScalingReferenceSI00 | tail -1 | awk -F= \'{printf(\"GlueCECapability: %s\\n\",$2)}\''
    
    result = self.runCommand( 'Checking current CE CPUScalingReferenceSI00', cmd, check = True )
    if not result[ 'OK' ]:
      result[ 'Description' ] = 'Could not perform ldap query for CPUScalingReferenceSI00'
      result[ 'SamResult' ]   = 'warning' 
      
    return result  

  def __checkQueueScaling( self, ldapLog ):
    '''
       Analyzes logs of CPUScalingReferenceSI00 and GlueCEPolicyMaxCPUTime 
    '''
    
    queueCount = 1
    for line in open( ldapLog, 'r' ):
      queueCount = queueCount + line.count( 'Time' )

    cmd = 'awk \'{ i=i+1;'
    cmd += 'if ( $1 == "GlueCEPolicyMaxCPUTime:" ) CPUTIME = $2 ;'
    cmd += 'if ( $1 == "GlueCECapability:" ) SI00 = $2 ;'
    cmd += 'if ( i == 2 ) {'
    cmd += 'if ( CPUTIME*SI00/250 > 18000 ) {i2= 0} ;'
    cmd += 'if ( CPUTIME*SI00/250 < 18000 ) {i2= 1} ;'
    cmd += '} ;'
    cmd += 'if ( i == 3 ) {'
    cmd += 'if ( CPUTIME*SI00/250 > 18000 ) {i3= 0} ;'
    cmd += 'if ( CPUTIME*SI00/250 < 18000 ) {i3= 1} ;'
    cmd += '} ;'
    cmd += 'if ( i == 4 ) {' 
    cmd += 'if ( CPUTIME*SI00/250 > 18000 ) {i4= 0} ;'
    cmd += 'if ( CPUTIME*SI00/250 < 18000 ) {i4= 1} ;'
    cmd += '} ;'
    cmd += 'if ( i == 5 ) {'
    cmd += 'if ( CPUTIME*SI00/250 > 18000 ) {i5= 0} ;'
    cmd += 'if ( CPUTIME*SI00/250 < 18000 ) {i5= 1} ;'
    cmd += '} ;'
    cmd += 'if ( i == 6 ) {'
    cmd += 'if ( CPUTIME*SI00/250 > 18000 ) {i6= 0} ;'
    cmd += 'if ( CPUTIME*SI00/250 < 18000 ) {i6= 1} ;'
    cmd += '} ;'
    cmd += 'if ( i == 7 ) {' 
    cmd += 'if ( CPUTIME*SI00/250 > 18000 ) {i7= 0} ;'
    cmd += 'if ( CPUTIME*SI00/250 < 18000 ) {i7= 1} ;'
    cmd += '} ;'
    cmd += 'if ( i == tot ){'
    cmd += 'if ( i2 == 0 || i3 == 0 || i4 == 0 || i5 == 0 || i6 == 0 || i7 == 0 ){'
    cmd += 'print 0 ;'
    cmd += '} ;'
    cmd += 'if ( i2 == 1 && i3 == 1 && i4 == 1 && i5 == 1 && i6 == 1 && i7 == 1 ){'
    cmd += 'print 1 ;'
    cmd += '}}}' 
    cmd += 'tot=' + str( queueCount ) + ' i2=1 i3=1 i4=1 i5=1 i6=1 i7=1 ' + ldapLog
    
    result = self.runCommand( 'Checking site queues', cmd )
    if not result[ 'OK' ]:
      result[ 'Description' ] = 'Could not check site queues'
      result[ 'SamResult' ]   = 'error'
      return result

    output = result[ 'Value' ]
    if not output.strip() == '0':
      result = S_ERROR( 'Status INFO (= 20)' )
      result[ 'Description' ] = 'LHCb queue length does not satisfy the minimum requirement of 39h of 5002KSI'
      result[ 'SamResult' ]   = 'info' 
      
    return result  
  
################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF