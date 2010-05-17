########################################################################
# $HeadURL$
# Author : Stuart Paterson
########################################################################

""" LHCb Site Queues SAM Test Module

    Corresponds to SAM test CE-lhcb-queues.
"""

__RCSID__ = "$Id: SiteQueues.py 18161 2009-11-11 12:07:09Z acasajus $"

import DIRAC
from DIRAC import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM import *

import string, os, sys, re

SAM_TEST_NAME='CE-lhcb-queues'
SAM_LOG_FILE='sam-queues.log'

class SiteQueues(ModuleBaseSAM):

  #############################################################################
  def __init__(self):
    """ Standard constructor for SAM Module
    """
    ModuleBaseSAM.__init__(self)
    self.version = __RCSID__
    self.logFile = SAM_LOG_FILE
    self.testName = SAM_TEST_NAME
    self.log = gLogger.getSubLogger( "SiteQueues" )
    self.result = S_ERROR()

    self.jobID = None
    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']

    #Workflow parameters for the test
    self.enable = True

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the workflow parameters are resolved here.
    """
    if self.step_commons.has_key('enable'):
      self.enable=self.step_commons['enable']
      if not type(self.enable)==type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' %self.enable)
        self.enable=False

    self.log.verbose('Enable flag is set to %s' %self.enable)
    return S_OK()

  #############################################################################
  def execute(self):
    """The main execution method of the SiteQueues module.
    """
    self.log.info('Initializing '+self.version)
    self.resolveInputVariables()
    self.setSAMLogFile()
    self.result = S_OK()
    if not self.result['OK']:
      return self.result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.info('An error was detected in a previous step, exiting with status error.')
      return self.finalize('Problem during execution','Failure detected in a previous step','error')

    self.setApplicationStatus('Starting %s Test' %self.testName)

    result = self.getRunInfo()
    if result.has_key('CE'):
      samNode = result['CE']
    else:
      return self.finalize('Could not get current CE','no CE','error')

    if os.environ.has_key('LCG_GFAL_INFOSYS'):
      self.log.info('LCG_GFAL_INFOSYS = %s' %(os.environ['LCG_GFAL_INFOSYS']))
    else:
      return self.finalize('LCG_GFAL_INFOSYS is not defined','Could not execute queue check','error')

    ldapLog ='ldap.log'
    fopen=open(ldapLog,'a')
    cmd='ldapsearch -h '+os.environ['LCG_GFAL_INFOSYS']+' -b "mds-vo-name=local,o=grid" -x -LLL "(GlueSubClusterUniqueID='+samNode+')" GlueHostBenchmarkSI00 | grep GlueHostBenchmarkSI00'
    result = self.runCommand('Checking current CE GlueHostBenchmarkSI00',cmd)
    if not result['OK']:
      return self.finalize('Could not perform ldap query for GlueHostBenchmarkSI00',result['Message'],'error')

    fopen.write(result['Value'])
    cmd='ldapsearch -h '+os.environ['LCG_GFAL_INFOSYS']+' -b "mds-vo-name=local,o=grid" -x -LLL "(& (GlueForeignKey=GlueClusterUniqueID='+samNode+') (GlueCEAccessControlBaseRule=VO:lhcb))" GlueCEUniqueID GlueCEPolicyMaxCPUTime | grep GlueCEPolicyMaxCPUTime'
    result = self.runCommand('Checking current CE GlueCEPolicyMaxCPUTime',cmd)
    if not result['OK']:
      return self.finalize('Could not perform ldap query for GlueCEPolicyMaxCPUTime',result['Message'],'error')

    fopen.write(result['Value'])
    fopen.close()

    queueCount = 1
    for line in open(ldapLog,'r'):
      queueCount = queueCount + line.count('Time')

    cmd='awk \'{ \
i=i+1; \
\
if ( $1 == "GlueCEPolicyMaxCPUTime:" ) CPUTIME = $2 ;\
if ( $1 == "GlueHostBenchmarkSI00:" ) SI00 = $2 ;\
\
if ( i == 2 ) { \
    if ( CPUTIME*SI00*60/500 > 140000 ) {i2= 0} ;\
    if ( CPUTIME*SI00*60/500 < 140000 ) {i2= 1} ;\
   } ;\
if ( i == 3 ) {\
    if ( CPUTIME*SI00*60/500 > 140000 ) {i3= 0} ;\
    if ( CPUTIME*SI00*60/500 < 140000 ) {i3= 1} ;\
   } ;\
if ( i == 4 ) { \
    if ( CPUTIME*SI00*60/500 > 140000 ) {i4= 0} ;\
    if ( CPUTIME*SI00*60/500 < 140000 ) {i4= 1} ;\
   } ;\
if ( i == 5 ) { \
    if ( CPUTIME*SI00*60/500 > 140000 ) {i5= 0} ;\
    if ( CPUTIME*SI00*60/500 < 140000 ) {i5= 1} ;\
   } ;\
if ( i == 6 ) { \
    if ( CPUTIME*SI00*60/500 > 140000 ) {i6= 0} ;\
    if ( CPUTIME*SI00*60/500 < 140000 ) {i6= 1} ;\
   } ;\
if ( i == 7 ) { \
    if ( CPUTIME*SI00*60/500 > 140000 ) {i7= 0} ;\
    if ( CPUTIME*SI00*60/500 < 140000 ) {i7= 1} ;\
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
}\' tot='+str(queueCount)+' i2=1 i3=1 i4=1 i5=1 i6=1 i7=1 '+ldapLog

    result = self.runCommand('Checking site queues',cmd)
    if not result['OK']:
      return self.finalize('Could not check site queues',result['Message'],'error')

    output = result['Value']
    if not output.strip()=='0':
      return self.finalize('LHCb queue length does not satisfy the minimum requirement of 39h of 5002KSI','Status INFO (= 20)','info')

    fopen=open(ldapLog,'w')
    cmd='ldapsearch -h '+os.environ['LCG_GFAL_INFOSYS']+' -b "mds-vo-name=local,o=grid" -x -LLL "(& (GlueForeignKey=GlueClusterUniqueID='+samNode+') (GlueCEAccessControlBaseRule=VO:lhcb))" GlueCECapability | grep CPUScalingReferenceSI00 | tail -1 | awk -F= \'{printf(\"GlueCECapability: %s\\n\",$2)}\''
    result = self.runCommand('Checking current CE CPUScalingReferenceSI00',cmd,check=True)
    if not result['OK']:
      return self.finalize('Could not perform ldap query for CPUScalingReferenceSI00',result['Message'],'warning')

    fopen.write(result['Value'])
    cmd='ldapsearch -h '+os.environ['LCG_GFAL_INFOSYS']+' -b "mds-vo-name=local,o=grid" -x -LLL "(& (GlueForeignKey=GlueClusterUniqueID='+samNode+') (GlueCEAccessControlBaseRule=VO:lhcb))" GlueCEUniqueID GlueCEPolicyMaxCPUTime | grep GlueCEPolicyMaxCPUTime'
    result = self.runCommand('Checking current CE GlueCEPolicyMaxCPUTime',cmd)
    if not result['OK']:
      return self.finalize('Could not perform ldap query for GlueCEPolicyMaxCPUTime',result['Message'],'error')

    fopen.write(result['Value'])
    fopen.close()

    queueCount = 1
    for line in open(ldapLog,'r'):
      queueCount = queueCount + line.count('Time')

    cmd='awk \'{ \
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
}\' tot='+str(queueCount)+' i2=1 i3=1 i4=1 i5=1 i6=1 i7=1 '+ldapLog

    result = self.runCommand('Checking site queues',cmd)
    if not result['OK']:
      return self.finalize('Could not check site queues',result['Message'],'error')

    output = result['Value']
    if not output.strip()=='0':
      return self.finalize('LHCb queue length does not satisfy the minimum requirement of 39h of 5002KSI','Status INFO (= 20)','info')

    self.log.info('Test %s completed successfully' %self.testName)
    self.setApplicationStatus('%s Successful' %self.testName)
    return self.finalize('%s Test Successful' %self.testName,'Status OK (= 10)','ok')

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#