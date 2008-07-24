########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Testing/SAM/Modules/ModuleBaseSAM.py,v 1.3 2008/07/24 08:19:06 paterson Exp $
# Author : Stuart Paterson
########################################################################

""" ModuleBaseSAM - base class for LHCb SAM workflow modules. Defines several
    common utility methods

"""

__RCSID__ = "$Id: ModuleBaseSAM.py,v 1.3 2008/07/24 08:19:06 paterson Exp $"

from DIRAC  import S_OK, S_ERROR, gLogger, gConfig
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.Core.Utilities.Subprocess import shellCall
from DIRAC.RequestManagementSystem.Client.DISETSubRequest import DISETSubRequest

import string,os,time

class ModuleBaseSAM(object):

  #############################################################################
  def __init__(self):
    """ Initialize some common SAM parameters.
    """
    self.samStatus = {'ok':'10','info':'20','notice':'30','warning':'40','error':'50','critical':'60','maintenance':'100'}

  #############################################################################
  def setApplicationStatus(self,status):
    """Wraps around setJobApplicationStatus of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    gLogger.verbose('setJobApplicationStatus(%s,%s,%s)' %(self.jobID,status,'JobFinalization'))

    if self.workflow_commons.has_key('JobReport'):
      self.jobReport  = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK('No reporting tool given')
    jobStatus = self.jobReport.setApplicationStatus(status)
    if not jobStatus['OK']:
      gLogger.warn(jobStatus['Message'])

    return jobStatus

  #############################################################################
  def getSAMNode(self):
    """In case CE isn't defined in the local config file, try to get it through
       broker info calls.
    """
    csCE = gConfig.getValue('/LocalSite/GridCE','')
    if not csCE:
      gLogger.warn('Could not get CE from local config file')

    cmd = 'edg-brokerinfo getCE || glite-brokerinfo getCE'
    result = self.runCommand('Trying to get local CE (SAM node name)',cmd)
    if not result['OK']:
      return result

    output = result['Value'].strip()
    ce = output.split(':')[0]
    if not ce:
      gLogger.warn('Could not get CE from broker-info call:\n%s' %output)
      return S_ERROR('Could not get CE from local cfg or broker-info call')

    return S_OK(ce)

  #############################################################################
  def setJobParameter(self,name,value):
    """Wraps around setJobParameter of state update client
    """
    if not self.jobID:
      return S_OK('JobID not defined') # e.g. running locally prior to submission

    gLogger.verbose('setJobParameter(%s,%s,%s)' %(self.jobID,name,value))

    if self.workflow_commons.has_key('JobReport'):
      self.jobReport  = self.workflow_commons['JobReport']

    if not self.jobReport:
      return S_OK('No reporting tool given')
    jobParam = self.jobReport.setJobParameter(str(name),str(value))
    if not jobParam['OK']:
      gLogger.warn(jobParam['Message'])

    return jobParam

  #############################################################################
  def runCommand(self,message,cmd):
    """Wrapper around shellCall to return S_OK(stdout) or S_ERROR(message) and
       produce the SAM log files with messages and outputs.
    """
    if not self.logFile:
      return S_ERROR('No LogFile defined')

    if not self.testName:
      return S_ERROR('No SAM test name defined')

    if not self.version:
      return S_ERROR('CVS version tag is not defined')

    try:
      if not self.enable in [True,False]:
        return S_ERROR('Expected boolean for enable flag')
    except Exception,x:
      return S_ERROR('Enable flag is not defined')

    gLogger.verbose(message)
    if not os.path.exists('%s' %(self.logFile)):
      fopen = open(self.logFile,'w')
      header = self.getMessageString('DIRAC SAM Test: %s\nLogFile: %s\nVersion: %s\nTest Executed On: %s' %(self.logFile,self.testName,self.version,time.asctime()),True)
      fopen.write(header)
      fopen.close()

    if not self.enable:
      cmd = 'echo "Enable flag is False, would have executed:"\necho "%s"' %cmd

    result = shellCall(0,cmd)
    if not result['OK']:
      return result
    status = result['Value'][0]
    stdout = result['Value'][1]
    stderr = result['Value'][2]
    gLogger.verbose(stdout)
    if stderr:
      gLogger.warn(stderr)

    fopen = open(self.logFile,'a')
    cmdHeader = self.getMessageString('Message: %s\nCommand: %s' %(message,cmd))
    fopen.write(cmdHeader)
    fopen.write(stdout)
    gLogger.verbose(cmdHeader)
    gLogger.verbose(stdout)
    if stderr:
      gLogger.warn(stderr)
      fopen.write(stderr)
    fopen.close()
    if status:
      gLogger.info('Non-zero status %s while executing %s' %(status,cmd))
      return S_OK(stdout)
    else:
      return S_OK(stdout)

  #############################################################################
  def getMessageString(self,message,header=False):
    """Return a nicely formatted string for the SAM logs.
    """
    border = ''
    limit = 0
    for line in message.split('\n'):
      if line:
        if len(line)>limit:
          limit=len(line)

    max = 100
    if limit > 100:
      limit = max

    for i in xrange(limit):
      if header:
        border+='='
      else:
        border+='-'
    if header:
      message = '\n%s\n%s\n%s\n' %(border,message,border)
    else:
      message = '%s\n%s\n%s\n' %(border,message,border)
    return message

  #############################################################################
  def finalize(self,message,result,samResult):
    """Finalize properly by setting the appropriate result at the step level
       in the workflow, errorDict is an S_ERROR() from a command that failed.
    """
    if not self.logFile:
      return S_ERROR('No LogFile defined')

    if not self.samStatus.has_key(samResult):
      return S_ERROR('%s is not a valid SAM status' %(samResult))

    gLogger.info('%s\n%s' %(message,result))
    fopen = open(self.logFile,'a')
    fopen.write(self.getMessageString('%s\n%s' %(message,result)))
    statusCode = self.samStatus[samResult]
    fopen.write(self.getMessageString('Exiting with SAM status %s=%s' %(samResult,statusCode),True))
    fopen.close()
    if not self.workflow_commons.has_key('SAMResults'):
      self.workflow_commons['SAMResults'] = {}

    self.workflow_commons['SAMResults'][self.testName]=statusCode
    if int(statusCode)<50:
      return S_OK(message)
    else:
      return S_ERROR(message)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#