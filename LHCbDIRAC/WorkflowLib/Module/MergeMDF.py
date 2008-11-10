########################################################################
# $Id: MergeMDF.py,v 1.1 2008/11/10 12:46:37 paterson Exp $
########################################################################
""" Simple merging module for MDF files. """

__RCSID__ = "$Id: MergeMDF.py,v 1.1 2008/11/10 12:46:37 paterson Exp $"

from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from WorkflowLib.Module.ModuleBase                       import *
from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager
from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig

import string,os

class MergeMDF(ModuleBase):

  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    self.log = gLogger.getSubLogger( "MergeMDF" )
    self.commandTimeOut = 10*60
    self.rm = ReplicaManager()
    self.outputDataName = ''
    self.outputLFN = ''

    #List all input parameters here
    self.inputData = ''
    self.outputDataSE = ''

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the module parameters are resolved here.
    """
    self.log.info(self.workflow_commons)
    self.log.info(self.step_commons)
    result = S_OK()
    if self.workflow_commons.has_key('InputData'):
      self.inputData = self.workflow_commons['InputData']
      if type(self.inputData) != type([]):
        self.inputData = self.inputData.split(';')
      self.inputData = [x.replace('LFN:','') for x in self.inputData]
    else:
      result = S_ERROR('No Input Data Defined')

    if self.step_commons.has_key('outputDataSE'):
      self.outputDataSE = self.step_commons['outputDataSE']
    else:
      result = S_ERROR('No Output SE Defined')

    if self.workflow_commons.has_key('ProductionOutputData'):
      self.outputLFN = self.workflow_commons['ProductionOutputData']
      self.outputDataName = os.path.basename(self.outputLFN)
    else:
      result = S_ERROR('Could Not Find Output LFN')

    return result

  #############################################################################
  def execute(self):
    """ Main execution function.
    """
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    #Now use the RM to obtain all input data sets locally
    for lfn in self.inputData:
      if os.path.exists(os.path.basename(lfn)):
        self.log.info('File %s already in local directory' %lfn)
      else:
        self.log.info('Attempting to download replica of %s' %lfn)
        result = self.rm.getFile(lfn)
        self.log.info(result)
        if not result['OK']:
          return result
        if not os.path.exists('%s/%s' %(os.getcwd(),os.path.basename(lfn))):
          self.log.warn('File does not exist in local directory after download')
          return S_ERROR('Downloaded File Not Found')

    #Now all MDF files are local, merge is a 'cat'
    cmd = 'cat %s > %s' %(string.join([os.path.basename(x) for x in self.inputData],' '),self.outputDataName)
    self.log.info('Executing "%s"' %cmd)
    result = shellCall(self.commandTimeOut,cmd)
    if not result['OK']:
      self.log.error(result)
      return result
    status = result['Value'][0]
    stdout = result['Value'][1]
    stderr = result['Value'][2]
    self.log.info(stdout)
    if stderr:
      self.log.error(stderr)

    if status:
      self.log.info('Non-zero status %s while executing "%s"' %(status,cmd))
      return S_ERROR('Non-zero Status During Merge')

    outputFilePath = '%s/%s' %(os.getcwd(),self.outputDataName)
    if not os.path.exists(outputFilePath):
      self.log.info('%s does not exist locally' %outputFlePath)

    self.log.info('All input files downloaded and merged to produce %s' %self.outputDataName)

    #Now upload the file and register in the LFC
    self.log.info('Attempting putAndRegister("%s","%s","%s",catalog="LcgFileCatalogCombined")' %(self.outputLFN,outputFilePath,self.outputDataSE))
    upload = self.rm.putAndRegister(self.outputLFN,outputFilePath,self.outputDataSE,catalog='LcgFileCatalogCombined')
    self.log.info(upload)
    if not upload['OK']:
      self.log.error('putAndRegister() operation failed')
      return result

    #Now can remove the input files from the LFC
#    for lfn in self.inputData:
#      self.log.info('Attempting removal of %s' %lfn)
#      result = self.rm.removeFile(lfn)
#      if not result['OK']:
#        self.log.error('File removal failed',lfn)
#        self.log.error(result)

    return S_OK('Produced merged MDF file')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#