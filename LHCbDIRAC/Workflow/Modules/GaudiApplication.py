""" Gaudi Application module - main module: creates the environment,
    executes gaudirun with the right options
"""

import re
import os

from DIRAC import S_OK, S_ERROR, gLogger, gConfig

from LHCbDIRAC.Core.Utilities.ProductionOptions import getDataOptions, getModuleOptions
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from LHCbDIRAC.Core.Utilities.RunApplication import RunApplication

__RCSID__ = "$Id$"

class GaudiApplication( ModuleBase ):
  """ GaudiApplication class
  """

  #############################################################################

  def __init__( self, bkClient = None, dm = None ):
    """ Usual init for LHCb workflow modules
    """

    self.log = gLogger.getSubLogger( "GaudiApplication" )
    super( GaudiApplication, self ).__init__( self.log, bkClientIn = bkClient, dm = dm )

    self.applicationName = ''
    self.applicationVersion = ''
    self.systemConfig = ''
    self.applicationLog = ''
    self.stdError = ''
    self.runTimeProjectName = ''
    self.runTimeProjectVersion = ''
    self.inputDataType = 'MDF'
    self.stepInputData = []  # to be resolved
    self.poolXMLCatName = 'pool_xml_catalog.xml'
    self.optionsFile = ''
    self.optionsLine = ''
    self.extraOptionsLine = ''
    self.extraPackages = ''
    self.jobType = ''
    self.multicoreJob = 'True'
    self.multicoreStep = 'N'

  #############################################################################

  def _resolveInputVariables( self ):
    """ Resolve all input variables for the module here.
    """

    super( GaudiApplication, self )._resolveInputVariables()
    super( GaudiApplication, self )._resolveInputStep()

  #############################################################################

  def execute( self, production_id = None, prod_job_id = None, wms_job_id = None,
               workflowStatus = None, stepStatus = None,
               wf_commons = None, step_commons = None,
               step_id = None, step_number = None ):
    """ The main execution method of GaudiApplication.
    """

    try:

      super( GaudiApplication, self ).execute( __RCSID__, production_id, prod_job_id, wms_job_id,
                                               workflowStatus, stepStatus,
                                               wf_commons, step_commons, step_number, step_id )

      if not self._checkWFAndStepStatus():
        return S_OK()

      self._resolveInputVariables()

      self.log.info( "Executing application %s %s for CMT configuration %s" % ( self.applicationName,
                                                                                self.applicationVersion,
                                                                                self.systemConfig ) )
      self.log.verbose( "/LocalSite/Root directory for job is %s" % ( gConfig.getValue( '/LocalSite/Root',
                                                                                        os.getcwd() ) ) )

      if self.jobType.lower() == 'merge':
        self._disableWatchdogCPUCheck()

      # Resolve options files
      commandOptions = []
      if self.optionsFile and self.optionsFile != "None":
        for fileopt in self.optionsFile.split( ';' ):
          if os.path.exists( '%s/%s' % ( os.getcwd(), os.path.basename( fileopt ) ) ):
            commandOptions.append( fileopt )
          # Otherwise take the one from the application options directory
          elif re.search( r'\$', fileopt ):
            self.log.info( 'Found options file containing environment variable: %s' % fileopt )
            commandOptions.append( fileopt )
          else:
            self.log.error( 'Cannot process options: "%s" not found via environment variable or in local directory' % ( fileopt ) )

      self.log.info( 'Final options files: %s' % ( ', '.join( commandOptions ) ) )

      runNumberGauss = 0
      firstEventNumberGauss = 0
      if self.applicationName.lower() == "gauss" and self.production_id and self.prod_job_id:
        if self.jobType.lower() == 'user':
          eventsMax = self.numberOfEvents
        else:
          # maintaining backward compatibility
          eventsMax = self.maxNumberOfEvents if self.maxNumberOfEvents else self.numberOfEvents
        runNumberGauss = int( self.production_id ) * 100 + int( self.prod_job_id )
        firstEventNumberGauss = eventsMax * ( int( self.prod_job_id ) - 1 ) + 1

      if self.optionsLine or self.jobType.lower() == 'user':
        self.log.debug( "Won't get any step outputs (USER job)" )
        stepOutputs = []
        stepOutputTypes = []
        histogram = False
      else:
        self.log.debug( "Getting the step outputs" )
        stepOutputs, stepOutputTypes, histogram = self._determineOutputs()


      if self.optionsLine or self.jobType.lower() == 'user':
        # Prepare standard project run time options
        generatedOpts = 'gaudi_extra_options.py'
        if os.path.exists( generatedOpts ):
          os.remove( generatedOpts )
        inputDataOpts = getDataOptions( self.applicationName,
                                        self.stepInputData,
                                        self.inputDataType,
                                        self.poolXMLCatName )['Value']  # always OK
        projectOpts = getModuleOptions( self.applicationName,
                                        self.numberOfEvents,
                                        inputDataOpts,
                                        self.optionsLine,
                                        runNumberGauss,
                                        firstEventNumberGauss,
                                        self.jobType )['Value']  # always OK
        self.log.info( 'Extra options generated for %s %s step:' % ( self.applicationName, self.applicationVersion ) )
        print projectOpts  # Always useful to see in the logs (don't use gLogger as we often want to cut n' paste)
        options = open( generatedOpts, 'w' )
        options.write( projectOpts )
        options.close()
        commandOptions.append( generatedOpts )

      else:
        prodConfFileName = self.createProdConfFile( stepOutputTypes, histogram, runNumberGauss, firstEventNumberGauss )

      # How to run the application
      ra = RunApplication()
      # lb-run stuff
      ra.applicationName = self.applicationName
      ra.applicationVersion = self.applicationVersion
      ra.systemConfig = self.systemConfig
      ra.extraPackages = self.extraPackages
      ra.runTimeProject = self.runTimeProjectName
      ra.runTimeProjectVersion = self.runTimeProjectVersion
      # actual stuff to run
      ra.command = self.executable
      ra.extraOptionsLine = self.extraOptionsLine
      ra.commandOptions = commandOptions
      if self.multicoreStep.upper() == 'Y':
        ra.multicore = self.multicoreJob
      ra.prodConfFileName = prodConfFileName
      ra.applicationLog = self.applicationLog
      ra.stdError = self.stdError

      # Now really running
      self.setApplicationStatus( '%s step %s' % ( self.applicationName, self.step_number ) )
      ra.run() # This would trigger an exception in case of failure, or application status != 0

      self.log.info( "Going to manage %s output" % self.applicationName )
      self._manageAppOutput( stepOutputs )

      # Still have to set the application status e.g. user job case.
      self.setApplicationStatus( '%s %s Successful' % ( self.applicationName, self.applicationVersion ) )

      return S_OK( "%s %s Successful" % ( self.applicationName, self.applicationVersion ) )

    except Exception as e: #pylint:disable=broad-except
      self.log.exception( "Failure in GaudiApplication execute module", lException = e )
      self.setApplicationStatus( repr(e) )
      return S_ERROR( str(e) )

    finally:
      super( GaudiApplication, self ).finalize( __RCSID__ )

# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
