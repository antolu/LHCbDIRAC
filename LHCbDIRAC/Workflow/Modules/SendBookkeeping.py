########################################################################
# $Id$
########################################################################
""" This module uploads the BK records prior to performing the transfer
    and registration (BK,LFC) operations using the preprepared BK XML
    files from the BKReport module.  These are only sent to the BK if
    no application crashes have been observed.
"""

__RCSID__ = "$Id$"

import os, string, glob
from DIRAC                                                 import S_OK, S_ERROR, gLogger
from LHCbDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase

class SendBookkeeping( ModuleBase ):

  #############################################################################

  def __init__( self ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( "SendBookkeeping" )
    super( SendBookkeeping, self ).__init__( self.log )

    self.version = __RCSID__
    #Workflow parameters
    self.request = None

  #############################################################################

  def resolveInputVariables( self ):
    """ By convention the module input parameters are resolved here.
    """

    self.log.debug( self.workflow_commons )
    self.log.debug( self.step_commons )

    if self.workflow_commons.has_key( 'Request' ):
      self.request = self.workflow_commons['Request']
    else:
      from DIRAC.RequestManagementSystem.Client.RequestContainer import RequestContainer
      self.request = RequestContainer()
      self.request.setRequestName( 'job_%s_request.xml' % self.jobID )
      self.request.setJobID( self.jobID )
      self.request.setSourceComponent( "Job_%s" % self.jobID )

    return S_OK( 'Parameters resolved' )

  #############################################################################
  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None, bk = None ):

    """ Main execution function.
    """

    super( SendBookkeeping, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                            workflowStatus, stepStatus,
                                            wf_commons, step_commons, step_number, step_id )

    if not self._enableModule():
      return S_OK()

    if not bk:
      from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
      bk = BookkeepingClient()

    result = self.resolveInputVariables()
    if not result['OK']:
      self.log.error( result['Message'] )
      return result

    bkFileExtensions = ['bookkeeping*.xml']
    bkFiles = []
    for ext in bkFileExtensions:
      self.log.verbose( 'Looking at BK file wildcard: %s' % ext )
      globList = glob.glob( ext )
      for check in globList:
        if os.path.isfile( check ):
          self.log.verbose( 'Found locally existing BK file: %s' % check )
          bkFiles.append( check )

    #Unfortunately we depend on the file names to order the BK records
    bkFiles.sort()
    self.log.info( 'The following BK files will be sent: %s' % ( string.join( bkFiles, ', ' ) ) )

    for bkFile in bkFiles:
      fopen = open( bkFile, 'r' )
      bkXML = fopen.read()
      fopen.close()
      self.log.verbose( 'Sending BK record %s:\n%s' % ( bkFile, bkXML ) )
      result = bk.sendBookkeeping( bkFile, bkXML )
      self.log.verbose( result )
      if result['OK']:
        self.log.info( 'Bookkeeping report sent for %s' % bkFile )
      else:
        self.log.error( 'Could not send Bookkeeping XML file to server, preparing DISET request for', bkFile )
        self.request.setDISETRequest( result['rpcStub'], executionOrder = 0 )
        self.workflow_commons['Request'] = self.request

    return S_OK( 'SendBookkeeping Module Execution Complete' )
