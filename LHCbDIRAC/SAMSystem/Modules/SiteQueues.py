# $HeadURL$
''' LHCb Site Queues SAM Test Module

  Corresponds to SAM test CE-lhcb-queues.
'''

import os

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
    """The main execution method of the SiteQueues module.
    """

    if 'CE' in self.runInfo:
      samNode = self.runInfo[ 'CE' ]
    else:
      return self.finalize( 'Could not get current CE', 'no CE', 'error' )

    if 'LCG_GFAL_INFOSYS' in os.environ:
      self.log.info( 'LCG_GFAL_INFOSYS = %s' % ( os.environ['LCG_GFAL_INFOSYS'] ) )
    else:
      return self.finalize( 'LCG_GFAL_INFOSYS is not defined', 'Could not execute queue check', 'error' )

    ldapLog = 'ldap.log'
    fopen  = open( ldapLog, 'a' )
    
    cmd = 'ldapsearch -h ' + os.environ['LCG_GFAL_INFOSYS'] 
    cmd += ' -b "mds-vo-name=local,o=grid" -x -LLL "(GlueSubClusterUniqueID=' + samNode 
    cmd += ')" GlueHostBenchmarkSI00 | grep GlueHostBenchmarkSI00'
    
    result = self.runCommand( 'Checking current CE GlueHostBenchmarkSI00', cmd )
    if not result['OK']:
      return self.finalize( 'Could not perform ldap query for GlueHostBenchmarkSI00', result['Message'], 'error' )

    fopen.write( result['Value'] )
    
    cmd = 'ldapsearch -h ' + os.environ['LCG_GFAL_INFOSYS'] 
    cmd += ' -b "mds-vo-name=local,o=grid" -x -LLL "(& (GlueForeignKey=GlueClusterUniqueID=' 
    cmd += samNode + ') (GlueCEAccessControlBaseRule=VO:lhcb))" GlueCEUniqueID '
    cmd += 'GlueCEPolicyMaxCPUTime | grep GlueCEPolicyMaxCPUTime'
    
    result = self.runCommand( 'Checking current CE GlueCEPolicyMaxCPUTime', cmd )
    if not result['OK']:
      return self.finalize( 'Could not perform ldap query for GlueCEPolicyMaxCPUTime', result['Message'], 'error' )

    fopen.write( result['Value'] )
    fopen.close()

    queueCount = 1
    for line in open( ldapLog, 'r' ):
      queueCount = queueCount + line.count( 'Time' )

    cmd = 'awk \'{ \
i=i+1; \
\
if ( $1 == "GlueCEPolicyMaxCPUTime:" ) CPUTIME = $2 ;\
if ( $1 == "GlueHostBenchmarkSI00:" ) SI00 = $2 ;\
\
if ( i == 2 ) { \
    if ( CPUTIME*SI00*60/500 > 270000 ) {i2= 0} ;\
    if ( CPUTIME*SI00*60/500 < 270000 ) {i2= 1} ;\
   } ;\
if ( i == 3 ) {\
    if ( CPUTIME*SI00*60/500 > 270000 ) {i3= 0} ;\
    if ( CPUTIME*SI00*60/500 < 270000 ) {i3= 1} ;\
   } ;\
if ( i == 4 ) { \
    if ( CPUTIME*SI00*60/500 > 270000 ) {i4= 0} ;\
    if ( CPUTIME*SI00*60/500 < 270000 ) {i4= 1} ;\
   } ;\
if ( i == 5 ) { \
    if ( CPUTIME*SI00*60/500 > 270000 ) {i5= 0} ;\
    if ( CPUTIME*SI00*60/500 < 270000 ) {i5= 1} ;\
   } ;\
if ( i == 6 ) { \
    if ( CPUTIME*SI00*60/500 > 270000 ) {i6= 0} ;\
    if ( CPUTIME*SI00*60/500 < 270000 ) {i6= 1} ;\
   } ;\
if ( i == 7 ) { \
    if ( CPUTIME*SI00*60/500 > 270000 ) {i7= 0} ;\
    if ( CPUTIME*SI00*60/500 < 270000 ) {i7= 1} ;\
   } ;\
\
if ( i == tot ){ \
 if ( i2 == 0 || i3 == 0 || i4 == 0 || i5 == 0 || i6 == 0 || i7 == 0 ){ \
    print 0 ;\
   } ;\
 if ( i2 == 1 && i3 == 1 && i4 == 1 && i5 == 1 && i6 == 1 && i7 == 1 ){ \
    print 1 ;\
  }\
 }\
}\' tot=' + str( queueCount ) + ' i2=1 i3=1 i4=1 i5=1 i6=1 i7=1 ' + ldapLog

    result = self.runCommand( 'Checking site queues', cmd )
    if not result['OK']:
      return self.finalize( 'Could not check site queues', result['Message'], 'error' )

    output = result['Value']
    if not output.strip() == '0':
      _msg = 'LHCb queue length does not satisfy the minimum requirement of 39h of 5002KSI'
      return self.finalize( _msg, 'Status INFO (= 20)', 'info' )

    fopen = open( ldapLog, 'w' )
    
    cmd = 'ldapsearch -h ' + os.environ['LCG_GFAL_INFOSYS'] 
    cmd += ' -b "mds-vo-name=local,o=grid" -x -LLL "(& (GlueForeignKey=GlueClusterUniqueID='
    cmd += samNode + ') (GlueCEAccessControlBaseRule=VO:lhcb))" GlueCECapability | grep ' 
    cmd += 'CPUScalingReferenceSI00 | tail -1 | awk -F= \'{printf(\"GlueCECapability: %s\\n\",$2)}\''
    
    result = self.runCommand( 'Checking current CE CPUScalingReferenceSI00', cmd, check = True )
    if not result['OK']:
      return self.finalize( 'Could not perform ldap query for CPUScalingReferenceSI00', result['Message'], 'warning' )

    fopen.write( result['Value'] )
    
    cmd = 'ldapsearch -h ' + os.environ['LCG_GFAL_INFOSYS']
    cmd += ' -b "mds-vo-name=local,o=grid" -x -LLL "(& (GlueForeignKey=GlueClusterUniqueID=' 
    cmd += samNode + ') (GlueCEAccessControlBaseRule=VO:lhcb))" GlueCEUniqueID GlueCEPolicyMaxCPUTime '
    cmd += '| grep GlueCEPolicyMaxCPUTime'
    
    result = self.runCommand( 'Checking current CE GlueCEPolicyMaxCPUTime', cmd )
    if not result['OK']:
      return self.finalize( 'Could not perform ldap query for GlueCEPolicyMaxCPUTime', result['Message'], 'error' )

    fopen.write( result['Value'] )
    fopen.close()

    queueCount = 1
    for line in open( ldapLog, 'r' ):
      queueCount = queueCount + line.count( 'Time' )

    cmd = 'awk \'{ \
i=i+1; \
\
if ( $1 == "GlueCEPolicyMaxCPUTime:" ) CPUTIME = $2 ;\
if ( $1 == "GlueCECapability:" ) SI00 = $2 ;\
\
if ( i == 2 ) { \
    if ( CPUTIME*SI00/250 > 18000 ) {i2= 0} ;\
    if ( CPUTIME*SI00/250 < 18000 ) {i2= 1} ;\
   } ;\
if ( i == 3 ) {\
    if ( CPUTIME*SI00/250 > 18000 ) {i3= 0} ;\
    if ( CPUTIME*SI00/250 < 18000 ) {i3= 1} ;\
   } ;\
if ( i == 4 ) { \
    if ( CPUTIME*SI00/250 > 18000 ) {i4= 0} ;\
    if ( CPUTIME*SI00/250 < 18000 ) {i4= 1} ;\
   } ;\
if ( i == 5 ) { \
    if ( CPUTIME*SI00/250 > 18000 ) {i5= 0} ;\
    if ( CPUTIME*SI00/250 < 18000 ) {i5= 1} ;\
   } ;\
if ( i == 6 ) { \
    if ( CPUTIME*SI00/250 > 18000 ) {i6= 0} ;\
    if ( CPUTIME*SI00/250 < 18000 ) {i6= 1} ;\
   } ;\
if ( i == 7 ) { \
    if ( CPUTIME*SI00/250 > 18000 ) {i7= 0} ;\
    if ( CPUTIME*SI00/250 < 18000 ) {i7= 1} ;\
   } ;\
\
if ( i == tot ){ \
 if ( i2 == 0 || i3 == 0 || i4 == 0 || i5 == 0 || i6 == 0 || i7 == 0 ){ \
    print 0 ;\
   } ;\
 if ( i2 == 1 && i3 == 1 && i4 == 1 && i5 == 1 && i6 == 1 && i7 == 1 ){ \
    print 1 ;\
  }\
 }\
}\' tot=' + str( queueCount ) + ' i2=1 i3=1 i4=1 i5=1 i6=1 i7=1 ' + ldapLog

    result = self.runCommand( 'Checking site queues', cmd )
    if not result['OK']:
      return self.finalize( 'Could not check site queues', result['Message'], 'error' )

    output = result['Value']
    if not output.strip() == '0':
      _msg = 'LHCb queue length does not satisfy the minimum requirement of 39h of 5002KSI'
      return self.finalize( _msg, 'Status INFO (= 20)', 'info' )

    self.log.info( 'Test %s completed successfully' % self.testName )
    self.setApplicationStatus( '%s Successful' % self.testName )
    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 10)', 'ok' )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF