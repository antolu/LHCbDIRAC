########################################################################
# $Id: RemoveInputData.py,v 1.2 2009/04/08 14:28:04 paterson Exp $
########################################################################
""" Module to remove input data files for given workflow. Initially written
    for use after merged outputs have been successfully uploaded to an SE.
"""

__RCSID__ = "$Id: RemoveInputData.py,v 1.2 2009/04/08 14:28:04 paterson Exp $"

from WorkflowLib.Module.ModuleBase                         import *
from DIRAC.DataManagementSystem.Client.ReplicaManager      import ReplicaManager
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC                                                 import S_OK, S_ERROR, gLogger, gConfig

import string,os

class RemoveInputData(ModuleBase):

  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    ModuleBase.__init__(self)
    self.version = __RCSID__
    self.log = gLogger.getSubLogger( "RemoveInputData" )
    self.rm = ReplicaManager()
    self.jobID = ''
    self.enable=True
    self.failoverTest=False #flag to set a failover removal request for testing

    #List all parameters here
    self.request = None

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the module parameters are resolved here.
    """
    self.log.verbose(self.workflow_commons)
    self.log.verbose(self.step_commons)

    if self.step_commons.has_key('Enable'):
      self.enable=self.step_commons['Enable']
      if not type(self.enable)==type(True):
        self.log.warn('Enable flag set to non-boolean value %s, setting to False' %self.enable)
        self.enable=False

    if self.step_commons.has_key('TestFailover'):
      self.enable=self.step_commons['TestFailover']
      if not type(self.failoverTest)==type(True):
        self.log.warn('Test failover flag set to non-boolean value %s, setting to False' %self.failoverTest)
        self.failoverTest=False

    if os.environ.has_key('JOBID'):
      self.jobID = os.environ['JOBID']
      self.log.verbose('Found WMS JobID = %s' %self.jobID)
    else:
      self.log.info('No WMS JobID found, disabling module via control flag')
      self.enable=False

    if self.workflow_commons.has_key('Request'):
      self.request = self.workflow_commons['Request']
    else:
      self.request = RequestContainer()
      self.request.setRequestName('job_%s_request.xml' % self.jobID)
      self.request.setJobID(self.jobID)
      self.request.setSourceComponent("Job_%s" % self.jobID)

    #Get job input data files to be removed if previous modules were successful
    if self.workflow_commons.has_key('InputData'):
      self.inputData = self.workflow_commons['InputData']
      if type(self.inputData) != type([]):
        self.inputData = self.inputData.split(';')
      self.inputData = [x.replace('LFN:','') for x in self.inputData]
    else:
      return S_ERROR('No Input Data Defined')

    return S_OK('Parameters resolved')

  #############################################################################
  def execute(self):
    """ Main execution function.
    """
    self.log.info('Initializing %s' %self.version)
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      return S_OK('No input data removal attempted since workflow status not ok')

    #At this point can exit and see exactly what the module will try to remove
    #for testing purposes.
    if not self.enable:
      self.log.info('Module is disabled by control flag, would have attempted to remove the following files:\n%s' %(string.join(self.inputData,'\n')))
      for fileName in self.inputData:
        self.log.info('Remove - %s' %fileName)

      return S_OK('Module is disabled by control flag')

    #Try to remove the file list with failover if necessary
    failover = []
    if not self.failoverTest:
      self.log.info('Attempting rm.removeFile("%s")' %(self.inputData))
      result = self.rm.removeFile(self.inputData)
      if not result['OK']:
        self.log.error('Could not remove files with message:\n%s' %(result['Message']))
        failureDict = result['Value']['Failed']
        if failureDict:
          self.log.info('Not all files were successfully removed, see "LFN : reason" below')
          for lfn,reason in failureDict.items():
            self.log.info('%s : %s' %(lfn,reason))
        failover = failureDict.keys()
    else:
      self.log.info('Failover test flag is enabled, setting removal requests by default')
      failover = self.inputData

    for lfn in failover:
      self.__setFileRemovalRequest(lfn)

    self.workflow_commons['Request']=self.request
    return S_OK('Input Data Removed')

  #############################################################################
  def __setFileRemovalRequest(self,lfn):
    """ Sets a removal request for a file including all replicas.
    """
    self.log.info('Setting file removal request for %s' %lfn)
    result = self.request.addSubRequest({'Attributes':{'Operation':'removeFile',
                                                       'TargetSE':'','ExecutionOrder':1}},
                                         'removal')
    index = result['Value']
    fileDict = {'LFN':lfn,'Status':'Waiting'}
    result = self.request.setSubRequestFiles(index,'removal',[fileDict])
    return S_OK()

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#