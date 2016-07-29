""" This module uploads the BK records prior to performing the transfer
    and registration (BK,LFC) operations using the preprepared BK XML
    files from the BKReport module.  These are only sent to the BK if
    no application crashes have been observed.
"""

import os
import glob
from DIRAC                                                 import S_OK, S_ERROR, gLogger
from LHCbDIRAC.Workflow.Modules.ModuleBase                 import ModuleBase

__RCSID__ = "$Id$"

class SendBookkeeping( ModuleBase ):

  #############################################################################

  def __init__( self, bkClient = None, dm = None ):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger( "SendBookkeeping" )
    super( SendBookkeeping, self ).__init__( self.log, bkClientIn = bkClient, dm = dm )

    self.version = __RCSID__

  #############################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None ):

    """ Main execution function.
    """

    try:

      super( SendBookkeeping, self ).execute( self.version, production_id, prod_job_id, wms_job_id,
                                              workflowStatus, stepStatus,
                                              wf_commons, step_commons, step_number, step_id )

      if not self._checkWFAndStepStatus():
        self.log.info( 'Job completed with errors, no bookkeeping records will be sent' )
        return S_OK( 'Job completed with errors' )

      if not self._enableModule():
        return S_OK()

      self._resolveInputVariables()

      bkFileExtensions = ['bookkeeping*.xml']
      bkFiles = []
      for ext in bkFileExtensions:
        self.log.verbose( 'Looking at BK file wildcard: %s' % ext )
        globList = glob.glob( ext )
        for check in globList:
          if os.path.isfile( check ):
            self.log.verbose( 'Found locally existing BK file: %s' % check )
            bkFiles.append( check )

      # Unfortunately we depend on the file names to order the BK records
      bkFiles.sort()
      self.log.info( "The following BK files will be sent: %s" % ( ', '.join( bkFiles ) ) )

      for bkFile in bkFiles:
        fopen = open( bkFile, 'r' )
        bkXML = fopen.read()
        fopen.close()
        self.log.verbose( "Sending BK record %s:\n%s" % ( bkFile, bkXML ) )
        result = self.bkClient.sendXMLBookkeepingReport( bkXML )
        self.log.verbose( result )
        if result['OK']:
          self.log.info( "Bookkeeping report sent for %s" % bkFile )
        else:
          self.log.error( "Could not send Bookkeeping XML file to server, preparing DISET request for", bkFile )
          self.setBKRegistrationRequest( bkFile )
          self.workflow_commons['Request'] = self.request

      return S_OK( 'SendBookkeeping Module Execution Complete' )

    except Exception as e: #pylint:disable=broad-except
      self.log.exception( "Failure in SendBookkeeping execute module", lException = e )
      return S_ERROR( e )

    finally:
      super( SendBookkeeping, self ).finalize( self.version )
