########################################################################
# $Id$
########################################################################
""" Module to remove input data files for given workflow. Initially written
    for use after merged outputs have been successfully uploaded to an SE.
"""

__RCSID__ = "$Id$"

from DIRAC.DataManagementSystem.Client.ReplicaManager      import ReplicaManager
from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
from LHCbDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase

from DIRAC                                                 import S_OK, S_ERROR, gLogger

import string, os

class RemoveInputData( ModuleBase ):

  #############################################################################

  def __init__( self ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( "RemoveInputData" )
    super( RemoveInputData, self ).__init__( self.log )

    self.version = __RCSID__
    self.rm = ReplicaManager()

    #List all parameters here
    self.request = None

  #############################################################################

  def resolveInputVariables( self ):
    """ By convention the module parameters are resolved here.
    """

    self.log.verbose( self.workflow_commons )
    self.log.verbose( self.step_commons )

    if self.workflow_commons.has_key( 'Request' ):
      self.request = self.workflow_commons['Request']
    else:
      self.request = RequestContainer()
      self.request.setRequestName( 'job_%s_request.xml' % self.jobID )
      self.request.setJobID( self.jobID )
      self.request.setSourceComponent( "Job_%s" % self.jobID )

    #Get job input data files to be removed if previous modules were successful
    if self.workflow_commons.has_key( 'InputData' ):
      self.inputData = self.workflow_commons['InputData']
      if type( self.inputData ) != type( [] ):
        self.inputData = self.inputData.split( ';' )
      self.inputData = [x.replace( 'LFN:', '' ) for x in self.inputData]
    else:
      return S_ERROR( 'No Input Data Defined' )

    return S_OK( 'Parameters resolved' )

  #############################################################################
  def execute( self ):
    """ Main execution function.
    """

    self.log.info( 'Initializing %s' % self.version )

    if not self._enableModule():
      return S_OK()

    self.resolveInputVariables()

    if not self.workflowStatus['OK'] or not self.stepStatus['OK']:
      self.log.verbose( 'Workflow status = %s, step status = %s' % ( self.workflowStatus['OK'], self.stepStatus['OK'] ) )
      return S_OK( 'No input data removal attempted since workflow status not ok' )

    #Try to remove the file list with failover if necessary
    failover = []
    self.log.info( 'Attempting rm.removeFile("%s")' % ( self.inputData ) )
    result = self.rm.removeFile( self.inputData )
    self.log.verbose( result )
    if not result['OK']:
      self.log.error( 'Could not remove files with message:\n"%s"\nWill set removal requests just in case.' % ( result['Message'] ) )
      failover = self.inputData
    if result['Value']['Failed']:
      failureDict = result['Value']['Failed']
      if failureDict:
        self.log.info( 'Not all files were successfully removed, see "LFN : reason" below\n%s' % ( failureDict ) )
      failover = failureDict.keys()

    for lfn in failover:
      self.__setFileRemovalRequest( lfn )

    self.workflow_commons['Request'] = self.request
    return S_OK( 'Input Data Removed' )

  #############################################################################
  def __setFileRemovalRequest( self, lfn ):
    """ Sets a removal request for a file including all replicas.
    """
    self.log.info( 'Setting file removal request for %s' % lfn )
    result = self.request.addSubRequest( {'Attributes':{'Operation':'removeFile',
                                                       'TargetSE':'', 'ExecutionOrder':1}},
                                         'removal' )
    index = result['Value']
    fileDict = {'LFN':lfn, 'Status':'Waiting'}
    result = self.request.setSubRequestFiles( index, 'removal', [fileDict] )
    return S_OK()

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
