########################################################################
# $Id: MergeMDF.py 18064 2009-11-05 19:40:01Z acasajus $
########################################################################
""" Simple merging module for MDF files. """

__RCSID__ = "$Id: MergeMDF.py 18064 2009-11-05 19:40:01Z acasajus $"

from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Core.DISET.RPCClient                          import RPCClient
from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager
from DIRAC                                               import S_OK, S_ERROR, gLogger, gConfig

from LHCbDIRAC.Workflow.Modules.ModuleBase                        import ModuleBase
from LHCbDIRAC.LHCbSystem.Utilities.ProductionData                import constructProductionLFNs

import string,os

class MergeMDF(ModuleBase):

  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    self.log = gLogger.getSubLogger( "MergeMDF" )
    self.version = __RCSID__
    self.commandTimeOut = 10*60
    self.rm = ReplicaManager()
    self.outputDataName = ''
    self.outputLFN = ''
    #List all input parameters here
    self.inputData = ''

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

#    if self.step_commons.has_key('outputDataSE'):
#      self.outputDataSE = self.step_commons['outputDataSE']
#    else:
#      result = S_ERROR('No Output SE Defined')

    if self.step_commons.has_key('applicationLog'):
      self.applicationLog = self.step_commons['applicationLog']
    else:
      self.applicationLog='mergingMDF.log'

    if self.step_commons.has_key('listoutput'):
       self.listoutput = self.step_commons['listoutput']
    else:
      return S_ERROR('Could not find listoutput')

    if self.workflow_commons.has_key('outputList'):
        self.workflow_commons['outputList'] = self.workflow_commons['outputList'] + self.listoutput
    else:
        self.workflow_commons['outputList'] = self.listoutput

    if self.workflow_commons.has_key('ProductionOutputData'):
        self.prodOutputLFNs = self.workflow_commons['ProductionOutputData']
        if not type(self.prodOutputLFNs)==type([]):
          self.prodOutputLFNs = [i.strip() for i in self.prodOutputLFNs.split(';')]
    else:
      self.log.info('ProductionOutputData parameter not found, creating on the fly')
      result = constructProductionLFNs(self.workflow_commons)
      if not result['OK']:
        self.log.error('Could not create production LFNs',result['Message'])
        return result
      self.prodOutputLFNs=result['Value']['ProductionOutputData']

    if len(self.prodOutputLFNs) > 1:
      return S_ERROR('MergeMDF module can only have one output LFN')

    self.outputLFN = self.prodOutputLFNs[0]
    self.outputDataName = os.path.basename(self.outputLFN)
    return result

  #############################################################################
  def execute(self):
    """ Main execution function.
    """
    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error(result['Message'])
      return result

    logLines = ['#'*len(self.version),self.version,'#'*len(self.version)]
    logLines.append('The following files will be downloaded for merging:\n%s' %(string.join(self.inputData,'\n')))
    #Now use the RM to obtain all input data sets locally
    for lfn in self.inputData:
      if os.path.exists(os.path.basename(lfn)):
        self.log.info('File %s already in local directory' %lfn)
      else:
        logLines.append('#'*len(lfn))
        msg = 'Attempting to download replica of:\n%s' %lfn
        self.log.info(msg)
        logLines.append(msg)
        logLines.append('#'*len(lfn))
        result = self.rm.getFile(lfn)
        self.log.info(result)
        logLines.append(result)
        if not result['OK']:
          logLines.append('\nFailed to download %s' %(lfn))
          return self.finalize(logLines,error='Failed To Download LFN')
        if not os.path.exists('%s/%s' %(os.getcwd(),os.path.basename(lfn))):
          logLines.append('\nFile does not exist in local directory after download')
          return self.finalize(logLines,error='Downloaded File Not Found')

    #Now all MDF files are local, merge is a 'cat'
    cmd = 'cat %s > %s' %(string.join([os.path.basename(x) for x in self.inputData],' '),self.outputDataName)
    logLines.append('\nExecuting merge operation...')
    self.log.info('Executing "%s"' %cmd)
    result = shellCall(self.commandTimeOut,cmd)
    if not result['OK']:
      self.log.error(result)
      logLines.append('Merge operation failed with result:\n%s' %result)
      return self.finalize(logLines,error='shellCall Failed')

    status = result['Value'][0]
    stdout = result['Value'][1]
    stderr = result['Value'][2]
    self.log.info(stdout)
    if stderr:
      self.log.error(stderr)

    if status:
      msg = 'Non-zero status %s while executing "%s"' %(status,cmd)
      self.log.info(msg)
      logLines.append(msg)
      return self.finalize(logLines,error='Non-zero Status During Merge')

    outputFilePath = '%s/%s' %(os.getcwd(),self.outputDataName)
    if not os.path.exists(outputFilePath):
      logLines.append('Merged file not found in local directory after merging operation')
      return self.finalize(logLines,error='Merged File Not Created')

    msg = 'SUCCESS: All input files downloaded and merged to produce %s' %(self.outputDataName)
    self.log.info(msg)
    logLines.append(msg)
    return self.finalize(logLines,msg='Produced merged MDF file')

  #############################################################################
  def finalize(self,logLines,msg='',error=''):
    """ Return appropriate message and write to log file.
    """
    self.log.info(msg)
    logLines.append(msg)
    logLines = [ str(i) for i in logLines]
    logLines.append('#EOF')
    fopen = open(self.applicationLog,'w')
    fopen.write(string.join(logLines,'\n')+'\n')
    fopen.close()
    if msg:
      return S_OK(msg)
    elif error:
      return S_ERROR(error)
    else:
      return S_ERROR('MergeMDF: no msg or error defined')

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#