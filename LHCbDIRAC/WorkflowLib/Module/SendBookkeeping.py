########################################################################
# $Id: SendBookkeeping.py,v 1.1 2009/02/05 17:01:06 paterson Exp $
########################################################################
""" This module uploads the BK records prior to performing the transfer
    and registration (BK,LFC) operations using the preprepared BK XML
    files from the BKReport module.  These are only sent to the BK if
    no application crashes have been observed.
"""

__RCSID__ = "$Id: SendBookkeeping.py,v 1.1 2009/02/05 17:01:06 paterson Exp $"

from WorkflowLib.Module.ModuleBase                         import *
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from DIRAC.BookkeepingSystem.Client.BookkeepingClient      import BookkeepingClient
from DIRAC                                                 import S_OK, S_ERROR, gLogger, gConfig

import os,string,glob

class SendBookkeeping(ModuleBase):

  #############################################################################
  def __init__(self):
    """Module initialization.
    """
    self.version = __RCSID__
    self.log = gLogger.getSubLogger( "SendBookkeeping" )
    #Internal parameters
    self.enable=True
    self.failoverTest=False
    self.jobID = ''
    #Workflow parameters
    self.request = None
    #Globals
    self.fileReport = None
    self.bk = BookkeepingClient()

  #############################################################################
  def resolveInputVariables(self):
    """ By convention the module input parameters are resolved here.
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

    if self.workflow_commons.has_key('FileReport'):
      self.fileReport = self.workflow_commons['FileReport']

    if self.workflow_commons.has_key('Request'):
      self.request = self.workflow_commons['Request']
    else:
      self.request = RequestContainer()
      self.request.setRequestName('job_%s_request.xml' % self.jobID)
      self.request.setJobID(self.jobID)
      self.request.setSourceComponent("Job_%s" % self.jobID)

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
      self.log.info('Workflow status = %s, step status = %s' %(self.workflowStatus['OK'],self.stepStatus['OK']))
      inputFiles = self.fileReport.getFiles()
      for lfn in inputFiles:
        if inputFiles[lfn] != 'ApplicationCrash':
          self.fileReport.setFileStatus(int(self.PRODUCTION_ID),lfn,'Unused')
      result = self.fileReport.commit()
      if not result['OK']:
        self.log.error('Failed to report file status to ProductionDB',result['Message'])
      self.workflow_commons['FileReport'] = self.fileReport

      self.log.info('Job completed with errors, no bookkeeping records will be sent')
      return S_OK('Job completed with errors')

    bkFileExtensions = ['bookkeeping*.xml']
    bkFiles=[]
    for ext in bkFileExtensions:
      self.log.verbose('Looking at BK file wildcard: %s' %ext)
      globList = glob.glob(ext)
      for check in globList:
        if os.path.isfile(check):
          self.log.verbose('Found locally existing BK file: %s' %check)
          bkFiles.append(check)

    self.log.info('The following BK files will be sent: %s' %(string.join(bkFiles,', ')))

    if not self.enable:
      self.log.info('Module is disabled by control flag')
      return S_OK('Module is disabled by control flag')

    for bkFile in bkFiles:
      fopen = open(bkFile,'r')
      bkXML = fopen.read()
      fopen.close()
      if not self.failoverTest:
        result = self.bk.sendBookkeeping(bkFile,bkXML)
        self.log.verbose(result)
        if result['OK']:
          self.log.info('Bookkeeping report sent for %s' %bkFile)
        else:
          self.log.error('Could not send Bookkeeping XML file to server, preparing DISET request for',bkFile)
          self.request.setDISETRequest(result['rpcStub'],executionOrder=0)
          self.workflow_commons['Request']=self.request
      else:
        self.log.error('Testing failover, would prepare DISET request for',bkFile)
        #need example rpcStub
#        self.request.setDISETRequest(result['rpcStub'],executionOrder=0)
#        self.workflow_commons['Request']=self.request

    return S_OK('SendBookkeeping Module Execution Complete')

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#