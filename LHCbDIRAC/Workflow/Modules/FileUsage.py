########################################################################
# $Id: FileUsage.py 35442 2012-01-16 11:00:54Z dremensk $
########################################################################
""" Module to upload specified job output files according to the parameters
    defined in the user workflow.
"""

__RCSID__ = "$Id: FileUsage.py 35442 2012-01-16 11:00:54Z dremensk $"
import os
#from DIRAC.DataManagementSystem.Client.ReplicaManager      import ReplicaManager
#from DIRAC.DataManagementSystem.Client.FailoverTransfer    import FailoverTransfer
#from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
#from DIRAC.Core.Security.Misc                              import getProxyInfo
#from DIRAC.Core.Utilities.File                             import getGlobbedFiles
from DIRAC.Core.Utilities                                  import List
from DIRAC.Core.DISET.RPCClient import RPCClient
from LHCbDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase
from DIRAC.Resources.Catalog.PoolXMLCatalog import PoolXMLCatalog

#from LHCbDIRAC.Core.Utilities.ProductionData               import constructUserLFNs
#from LHCbDIRAC.Core.Utilities.ResolveSE                    import getDestinationSEList

from DIRAC                                                 import S_OK, S_ERROR, gLogger, gConfig

import DIRAC
import string, os, random, time, re

class FileUsage( ModuleBase ):

  #############################################################################
  def __init__( self ):
    """Module initialization.
    """
    self.log = gLogger.getSubLogger( "FileUsage" )
    super( FileUsage, self ).__init__( self.log )

    self.version = __RCSID__
    self.debug = True
    self.enable = True
    self.lastStep = False
    self.inputData = [] # to be resolved
    self.dirDict = {}

  #############################################################################
  def _resolveInputVariables( self ):
    """ Resolve all input variables for the module here.
    """
    super( FileUsage, self )._resolveInputVariables()
    self.log.info( self.workflow_commons )
    #self.log.verbose( self.step_commons )
    self.dirDict = {}
    if self.workflow_commons.has_key( 'InputData' ):
      self.inputData = self.workflow_commons['InputData']
      if self.inputData:
        if type( self.inputData ) != type( [] ):
          self.inputData = self.inputData.split( ';' )
      else:
        self.inputData = []

    #inputData: ['LFN:/lhcb/LHCb/Collision11/BHADRON.DST/00012957/0000/00012957_00000753_1.bhadron.dst', '/lhcb/LHCb/Collision11/BHADRON.DST/00012957/0000/00012957_00000752_1.bhadron.dst', '/lhcb/certification/test/ALLSTREAMS.DST/00000002/0000/test.dst']
    #build the dictionary of dataset usage
    #strip the 'LFN:' part if present

    if self.inputData:
      for inputFile in self.inputData:
        baseName = os.path.basename( inputFile )
        strippedDir = inputFile[0:inputFile.find( baseName )].strip( 'LFN:' )
        if not strippedDir:
          self.log.error('Dataset unknown for file %s, probably file specified without path!' %(inputFile))
        else:
          if strippedDir in self.dirDict:
            self.dirDict[strippedDir] += 1
          else:
            self.dirDict[strippedDir] = 1
    else:
      self.log.info("No input data specified for this job")

    self.log.info( 'dirDict = ', self.dirDict )
    #NOTE: Enable before commit?
    #if os.environ.has_key( 'JOBID' ):
    #  self.jobID = os.environ['JOBID']
    #  self.log.verbose( 'Found WMS JobID = %s' % self.jobID )
    #else:
    #  self.log.info( 'No WMS JobID found, disabling module via control flag' )
    #  self.enable = False

    return S_OK( 'Parameters resolved' )

  #############################################################################
  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_id = None, step_number = None,
               projectEnvironment = None ):
    """ Main execution function.
    """

    try:

      super( FileUsage, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                            workflowStatus, stepStatus,
                                            wf_commons, step_commons, step_number, step_id )

      if not self._enableModule():
        return S_OK()

      result = self._resolveInputVariables()
      if not result['OK']:
        self.log.error( result['Message'] )
        return result
      self.log.info( 'Initializing %s' % self.version )
      #Have to work out if the module is part of the last step i.e.
      #user jobs can have any number of steps and we only want
      #to run the finalization once.
      """ #the UserJobFinalization is actually the last step

      currentStep = int( self.step_commons['STEP_NUMBER'] )
      totalSteps = int( self.workflow_commons['TotalSteps'] )

      if currentStep == totalSteps:
        self.lastStep = True
      else:
        self.log.verbose( 'Current step = %s, total steps of workflow = %s, FileUsage will enable itself only at the last workflow step.' % ( currentStep, totalSteps ) )

      if not self.lastStep:
        return S_OK()

      #result = self._getJobJDL( jobID )
      if not self.enable:
        self.log.info( 'Module is disabled by control flag, would have attempted to report the following dataset usage %s' % self.dirDict )
        return S_OK( 'Module is disabled by control flag' )
      """

      if self.dirDict:
        result = self._reportFileUsage( self.dirDict )
        if not result['OK']:
          self.log.error( result['Message'] )
          return result
        self.log.info( "Reporting input file usage successful!" )
      else:
        self.log.info( "No input data usage to report!" )

      if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
        self.log.info( 'Workflow status = %s, step status = %s' % ( self.workflowStatus['OK'], self.stepStatus['OK'] ) )
        self.log.error( 'Workflow status is not ok, will not overwrite application status.' )
        return S_ERROR( 'Workflow failed, FileUsage module completed' )

      return S_OK('File Usage reported successfully')

    except Exception, e:
      self.log.exception( e )
      return S_ERROR( e )

    finally:
      super( FileUsage, self ).finalize( self.version )

  def _reportFileUsage( self, dirDict ):
    """Send the data usage report (SE,dirDict) where dirDict = {'Dataset':NumberOfHits}
    example: {'/lhcb/certification/test/ALLSTREAMS.DST/00000002/0000/': 1, '/lhcb/LHCb/Collision11/BHADRON.DST/00012957/0000/': 2}
    """
    self.log.info( 'FileUsage._reportFileUsage' )
    self.log.info( 'Reporting input file usage:' )
    for entry in dirDict:
      self.log.info( '%s:%s' % ( entry, dirDict[entry] ) )
    dataUsageClient = RPCClient( 'DataManagement/DataUsage', timeout = 120 )
    localSEList = gConfig.getValue( '/LocalSite/LocalSE', '' )
    if not localSEList:
      self.log.error( 'FileUsage._reportFileUsage: Could not get value from DIRAC Configuration Service for option /LocalSite/LocalSE' )
      localSEList = "UNKNOWN"
    self.log.info( "Using /LocalSite/LocalSE: %s" % ( localSEList ) )
    # example LocalSEList = 'SARA-RAW, SARA-RDST, SARA-ARCHIVE, SARA-DST, SARA_M-DST, SARA-USER'

    # we only care about the site, so strip the SE list up to the site name
    localSE = localSEList
    cutoff = min( localSEList.find( '_' ), localSEList.find( '-' ) )
    if cutoff != -1:
      localSE = localSE[0:cutoff]

    usageStatus = dataUsageClient.sendDataUsageReport( localSE, dirDict )
    if not usageStatus['OK']:
      self.log.warn( usageStatus['Message'] )

    return usageStatus
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
