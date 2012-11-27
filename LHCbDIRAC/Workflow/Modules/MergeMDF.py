""" Simple merging module for MDF files.
"""

__RCSID__ = "$Id: MergeMDF.py 57931 2012-10-23 17:00:11Z fstagni $"

from DIRAC                                               import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.Resources.Catalog.PoolXMLCatalog              import PoolXMLCatalog

from LHCbDIRAC.Workflow.Modules.ModuleBase               import ModuleBase

class MergeMDF( ModuleBase ):
  """ To be used in normal workflows
  """

  #############################################################################
  def __init__( self, bkClient = None, rm = None ):
    """Module initialization.
    """
    self.log = gLogger.getSubLogger( "MergeMDF" )
    super( MergeMDF, self ).__init__( self.log, bkClientIn = bkClient, rm = rm )

    self.version = __RCSID__

    self.outputLFN = ''
    #List all input parameters here
    self.stepInputData = []
    self.poolXMLCatName = 'pool_xml_catalog.xml'
    self.applicationName = 'cat'

  #############################################################################
  def _resolveInputVariables( self ):
    """ By convention the module parameters are resolved here.
    """


    super( MergeMDF, self )._resolveInputVariables()
    super( MergeMDF, self )._resolveInputStep()
    self.log.debug( "Getting the step outputs" )

  #############################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_number = None, step_id = None ):
    """ Main execution function.
    """

    try:

      super( MergeMDF, self ).execute( self.version,
                                       production_id, prod_job_id, wms_job_id,
                                       workflowStatus, stepStatus,
                                       wf_commons, step_commons,
                                       step_number, step_id )

      poolCat = PoolXMLCatalog( self.poolXMLCatName )

      self._resolveInputVariables()

      stepOutputs, stepOutputTypes, _histogram = self._determineOutputs()

      logLines = ['#' * len( self.version ), self.version, '#' * len( self.version )]

      localInputs = [str( poolCat.getPfnsByLfn( x )['Replicas']['Uknown'] ) for x in self.stepInputData]
      inputs = ' '.join( localInputs )
      cmd = 'cat %s > %s' % ( inputs, self.outputFilePrefix + '.' + stepOutputTypes[0] )
      logLines.append( '\nExecuting merge operation...' )
      self.log.info( 'Executing "%s"' % cmd )
      result = shellCall( timeout = 600, cmdSeq = cmd )
      if not result['OK']:
        self.log.error( result )
        logLines.append( 'Merge operation failed with result:\n%s' % result )
        return self.finalize( logLines, error = 'shellCall Failed' )

      status = result['Value'][0]
      stdout = result['Value'][1]
      stderr = result['Value'][2]
      self.log.info( stdout )
      if stderr:
        self.log.error( stderr )

      if status:
        msg = 'Non-zero status %s while executing "%s"' % ( status, cmd )
        self.log.info( msg )
        logLines.append( msg )
        return self.finalize( logLines, error = 'Non-zero Status During Merge' )

      self.log.info( "Going to manage %s output" % self.applicationName )
      try:
        self._manageAppOutput( stepOutputs )
      except IOError, e:
        return S_ERROR( e )

      # Still have to set the application status e.g. user job case.
      self.setApplicationStatus( '%s %s Successful' % ( self.applicationName, self.applicationVersion ) )

      # Write to log file
      msg = 'Produced merged MDF file'
      self.log.info( msg )
      logLines.append( msg )
      logLines = [ str( i ) for i in logLines]
      logLines.append( '#EOF' )
      fopen = open( self.applicationLog, 'w' )
      fopen.write( '\n'.join( logLines ) + '\n' )
      fopen.close()

      return S_OK( '%s %s Successful' % ( self.applicationName, self.applicationVersion ) )

    except Exception, e:
      self.log.exception( e )
      return S_ERROR( e )

    finally:
      super( MergeMDF, self ).finalize( self.version )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
