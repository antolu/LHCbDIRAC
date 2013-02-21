''' LHCb SAM Job Class

   The LHCb SAM Job class inherits generic VO functionality from the Job base class
   and provides VO-specific functionality to aid in the construction of
   workflows.

   Helper functions are documented with example usage for the DIRAC API.

   Below are several examples of LHCbSAMJob usage.

   An example SAM Test script would be::

    from DIRAC.Interfaces.API.Dirac import Dirac
    from LHCbDIRAC.SAMSystem.Client.LHCbSAMJob import LHCbSAMJob

    j = LHCbSAMJob()
    j.setDestinationCE('LCG.PIC.es')
    j.setSharedAreaLock(forceDeletion=False,enableFlag=False)
    j.checkSystemConfiguration(enableFlag=False)
    j.installSoftware(forceDeletion=False,enableFlag=False)
    j.reportSoftware(enableFalg=False)
    j.testApplications(enableFlag=False)
    j.finalizeAndPublish(logUpload=False,publishResults=False,enableFlag=False)
    dirac = Dirac()
    print j._toJDL()
    jobID = dirac.submit(j,mode='Local')
    print 'Submission Result: ',jobID
'''

import os

from DIRAC                                               import S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities.SiteCEMapping                  import getSiteForCE

from LHCbDIRAC.Interfaces.API.LHCbJob                    import LHCbJob
from LHCbDIRAC.Workflow.Utilities.Utils                  import getStepDefinition, addStepToWorkflow

__RCSID__ = '$Id$'
COMPONENT_NAME = 'LHCbDIRAC/SAMSystem/Client/LHCbSAMJob'

class LHCbSAMJob( LHCbJob ):
  '''
     LHCbSAMJob implementation of API Job

     It provides the following methods:
     - setSAMGroup
     - setPriority
     - setSharedAreaLock
     - checkSystemConfiguration
     - installSoftware
     - reportSoftware
     - testApplications
     - finalizeAndPublish
     - runTestScript
     - setDestinationCE
  '''

  def __init__( self, script = None, stdout = 'std.out', stderr = 'std.err' ):
    """Instantiates the Workflow object and some default parameters.
    """
    LHCbJob.__init__( self, script, stdout, stderr )

    self.gaudiStepCount = 0
    self.opsH = Operations()
    self.appTestPath = 'SAM/TestApplications'
    self.appTestList = 'SAM/ApplicationTestList'
    self.importLine = 'LHCbDIRAC.SAMSystem.Modules'

  def setDefaults( self ):
    """ Set some SAM specific defaults.
    """

    samLogLevel = self.opsH.getValue( 'SAM/LogLevel', 'verbose' )
    res = self.setLogLevel( samLogLevel )
    if not res[ 'OK' ]:
      return res

    samDefaultCPUTime = self.opsH.getValue( 'SAM/CPUTime', 50000 )
    res = self.setCPUTime( samDefaultCPUTime )
    if not res[ 'OK' ]:
      return res

    samPlatform = self.opsH.getValue( 'SAM/Platform', 'gLite-SAM' )
    res = self.setPlatform( samPlatform )
    if not res[ 'OK' ]:
      return res

    samOutputFiles = self.opsH.getValue( 'SAM/OutputSandbox', ['*.log'] )
    res = self.setOutputSandbox( samOutputFiles )
    if not res[ 'OK' ]:
      return res

    samGroup = self.opsH.getValue( 'SAM/JobGroup', 'SAM' )
    res = self.setJobGroup( samGroup )
    if not res[ 'OK' ]:
      return res

    samType = self.opsH.getValue( 'SAM/JobType', 'SAM' )
    res = self.setType( samType )
    if not res[ 'OK' ]:
      return res

    samPriority = self.opsH.getValue( 'SAM/Priority', 1 )
    res = self.setPriority( samPriority )
    if not res[ 'OK' ]:
      return res

    self._addJDLParameter( 'SubmitPools', 'SAM' )

    return S_OK()

  def setSAMGroup( self, samGroup ):
    """ Helper function. Set the SAM group and pilot types as required.

        Example usage:

        >>> job = LHCbSAMJob()
        >>> job.setSAMGroup('SAMsw')

        @param: SAM job group
        @param: string
    """
    if not samGroup in ( 'SAM', 'SAMsw' ):
      return S_ERROR( 'Expected SAM / SAMsw for SAM group' )

    self.setJobGroup( samGroup )
    if samGroup == 'SAMsw':
      self._addJDLParameter( 'PilotTypes', 'private' )

    return S_OK()

  def setPriority( self, priority ):
    """Helper function.

       Add the priority.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.setPriority(10)

       @param priority: User priority
       @type priority: Int
    """
    if not isinstance( priority, int ):
      try:
        priority = int( priority )
      except ValueError:
        return S_ERROR( 'Expected Integer for User priority' )

    self._addParameter( self.workflow, 'Priority', 'JDL', priority, 'User Job Priority' )
    return S_OK()

  def setSharedAreaLock( self, forceDeletion = False, enableFlag = True ):
    """Helper function.

       Add the LockSharedArea test.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.setSharedAreaLock()

       @param enableFlag: Flag to enable / disable calls for testing purposes
       @type enableFlag: boolean
       @param forceDeletion: Flag to force SAM lock file deletion
       @type forceDeletion: boolean
    """
    if not isinstance( forceDeletion, bool ):
      return S_ERROR( 'Expected boolean value for forceDeletion' )
    if not isinstance( enableFlag, bool ):
      return S_ERROR( 'Expected boolean value for enableFlag' )

    self.addToOutputSandbox.append( '*.log' )
    self._addJDLParameter( 'SAMLockTest', str( enableFlag ) )
    if forceDeletion:
      self._addJDLParameter( 'LockRemovalFlag', 'True' )

    self.gaudiStepCount += 1

    stepName = 'SAM_%s_Step%s' % ( 'LockSharedArea', self.gaudiStepCount )
    modulesNameList = [ 'LockSharedArea' ]
    parametersList = [ ( 'enable', 'bool', '', 'enable flag' ),
                        ( 'forceLockRemoval', 'bool', '', 'lock deletion flag' ) ]

    stepDef = getStepDefinition( stepName, modulesNameList, self.importLine, parametersList )
    stepInstance = addStepToWorkflow( self.workflow, stepDef, stepName )

    stepInstance.setValue( 'enable', enableFlag )
    stepInstance.setValue( 'forceLockRemoval', forceDeletion )

    return S_OK( True )


  def checkSystemConfiguration( self, enableFlag = True ):
    """Helper function.

       Add the SystemConfiguration test.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.checkSystemConfiguration('True')

       @param enableFlag: Flag to enable / disable calls for testing purposes
       @type enableFlag: boolean
    """

    if not isinstance( enableFlag, bool ):
      return S_ERROR( 'Expected boolean value for enableFlag' )

    if not enableFlag:
      return S_OK( False )

    self.addToOutputSandbox.append( '*.log' )
    self._addJDLParameter( 'SystemConfigurationTest', str( enableFlag ) )

    self.gaudiStepCount += 1

    # stepName        = '%sStep%s' % ( 'SAM', self.gaudiStepCount )
    stepName = 'SAM_%s_Step%s' % ( 'SystemConfiguration', self.gaudiStepCount )
    modulesNameList = [ 'SystemConfiguration' ]
    parametersList = [ ( 'enable', 'bool', '', 'enable flag' ) ]

    stepDef = getStepDefinition( stepName, modulesNameList, self.importLine, parametersList )
    stepInstance = addStepToWorkflow( self.workflow, stepDef, stepName )

    stepInstance.setValue( 'enable', enableFlag )

    return S_OK( True )


  def installSoftware( self, forceDeletion = False, enableFlag = True, installProjectURL = '' ):
    """Helper function.

       Add the SoftwareInstallation test.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.installSoftware( enableFlag = True )

       @param enableFlag: Flag to enable / disable calls for testing purposes
       @type enableFlag: boolean
       @param forceDeletion: Flag to force shared area deletion e.g. rm -rf *
       @type forceDeletion: boolean
    """

    if not isinstance( enableFlag, bool ):  # in [True, False] or not forceDeletion in [True, False]:
      return S_ERROR( 'Expected boolean value for enableFlag' )
    if not isinstance( forceDeletion, bool ):
      return S_ERROR( 'Expected boolean value for forceDeletion' )

    if installProjectURL:
      if not isinstance( installProjectURL, str ):
        return S_ERROR( 'Expected string for install_project URL' )

    self._addJDLParameter( 'SoftwareInstallationTest', str( enableFlag ) )
    self._addJDLParameter( 'DeleteSharedArea', str( forceDeletion ) )
    self._addJDLParameter( 'installProjectURL', str( installProjectURL ) )

    self.gaudiStepCount += 1
    self.addToOutputSandbox.append( '*.log' )

    # stepName        = '%sStep%s' % ( 'SAM', self.gaudiStepCount )
    stepName = 'SAM_%s_Step%s' % ( 'SoftwareInstallation', self.gaudiStepCount )
    modulesNameList = [ 'SoftwareInstallation' ]
    parametersList = [  # ( 'enable', 'bool', '', 'enable flag' ),
                        ( 'softwareFlag', 'bool', '', 'software flag' ),
                        ( 'purgeSharedAreaFlag', 'bool', '', 'remove all software in shared area' ),
                        ( 'installProjectURL', 'string', '', 'optional install_project URL' ) ]

    stepDef = getStepDefinition( stepName, modulesNameList, self.importLine, parametersList )
    stepInstance = addStepToWorkflow( self.workflow, stepDef, stepName )

    stepInstance.setValue( 'softwareFlag', enableFlag )
    stepInstance.setValue( 'purgeSharedAreaFlag', forceDeletion )
    stepInstance.setValue( 'installProjectURL', installProjectURL )

    return S_OK( True )

  def reportSoftware( self, enableFlag = True ):
    """Helper function.

       Add the reportSoftware step.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.reportSoftware( True )

       @param enableFlag: Flag to enable / disable calls for testing purposes
       @type enableFlag: boolean

    """

    if not isinstance( enableFlag, bool ):
      return S_ERROR( 'Expected boolean value for enableFlag' )

    self._addJDLParameter( 'ReportSoftwareTest', str( enableFlag ) )
    if not enableFlag:
      return S_OK( enableFlag )

    self.addToOutputSandbox.append( '*.log' )
    self.gaudiStepCount += 1

    # stepName        = '%sStep%s' % ( 'SAM', self.gaudiStepCount )
    stepName = 'SAM_%s_Step%s' % ( 'SoftwareReport', self.gaudiStepCount )
    modulesNameList = [ 'SoftwareReport' ]
    importLine = 'LHCbDIRAC.SAMSystem.Modules'
    parametersList = [ ( 'reportFlag', 'bool', '', 'report flag' ) ]
    # parametersList  = [ ( 'enable', 'bool', '', 'enable flag' ) ]

    stepDef = getStepDefinition( stepName, modulesNameList, importLine, parametersList )
    stepInstance = addStepToWorkflow( self.workflow, stepDef, stepName )

    stepInstance.setValue( 'reportFlag', enableFlag )

    return S_OK( True )


  def finalizeAndPublish( self, logUpload = True, enableFlag = True ):
    """Helper function.

       Add the SAM Finalization module step.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.finalizeAndPublish( enableFlag = True )

       @param enableFlag: Flag to enable / disable calls for testing purposes
       @type enableFlag: boolean
       @param logUpload: Flag to enable / disable log files upload
       @type logUpload: boolean
    """

    if not isinstance( logUpload, bool ):
      return S_ERROR( 'Expected boolean value for logUpload' )
    if not isinstance( enableFlag, bool ):
      return S_ERROR( 'Expected boolean value for enableFlag' )

    self.addToOutputSandbox.append( '*.log' )
    self._addJDLParameter( 'FinalizeAndPublishTest', str( enableFlag ) )

    self.gaudiStepCount += 1

    # stepName        = '%sStep%s' % ( 'SAM', self.gaudiStepCount )
    stepName = 'SAM_%s_Step%s' % ( 'SAMFinalization', self.gaudiStepCount )
    modulesNameList = [ 'SAMFinalization' ]
    parametersList = [ ( 'enable', 'bool', '', 'enable flag' ),
                        ( 'uploadLogsFlag', 'bool', '', 'flag to trigger upload of SAM logs to LogSE' ) ]

    stepDef = getStepDefinition( stepName, modulesNameList, self.importLine, parametersList )
    stepInstance = addStepToWorkflow( self.workflow, stepDef, stepName )

    stepInstance.setValue( 'enable', enableFlag )
    stepInstance.setValue( 'uploadLogsFlag', logUpload )

    return S_OK( True )


  def runTestScript( self, scriptName = '', enableFlag = True ):
    """Helper function.

       Add the optional SAM Run Test Script module step to run an arbitrary
       python script as part of the SAM test job.

       Example usage:

       >>> job = LHCbSAMJob()
       >>> job.runTestScript( 'myPythonScript.py', enableFlag = True )

       @param enableFlag: Flag to enable / disable calls for testing purposes
       @type enableFlag: boolean
       @param scriptName: Path to python script to execute
       @type scriptName: string
    """

    if not isinstance( enableFlag, bool ):
      return S_ERROR( 'Expected boolean value for enableFlag' )
    if not isinstance( scriptName, str ) or not scriptName:
      return S_ERROR( 'Expected string type for script name' )
    if not os.path.exists( scriptName ):
      return S_ERROR( 'Path to script %s must exist' % ( scriptName ) )

    self.addToInputSandbox.append( scriptName )
    self.addToOutputSandbox.append( '*.log' )
    self._addJDLParameter( 'RunTestScriptTest', str( enableFlag ) )

    self.gaudiStepCount += 1

    # stepName        = '%sStep%s' % ( 'SAM', self.gaudiStepCount )
    stepName = 'SAM_%s_Step%s' % ( 'RunTestScript', self.gaudiStepCount )
    modulesNameList = [ 'RunTestScript' ]
    parametersList = [ ( 'enable', 'bool', '', 'enable flag' ),
                        ( 'scriptName', 'string', '', 'script name to execute' ) ]

    stepDef = getStepDefinition( stepName, modulesNameList, self.importLine, parametersList )
    stepInstance = addStepToWorkflow( self.workflow, stepDef, stepName )

    stepInstance.setValue( 'enable', enableFlag )
    stepInstance.setValue( 'scriptName', os.path.basename( scriptName ) )

    return S_OK( True )


  def setDestinationCE( self, ceName ):
    """ Sets the Grid requirements for the pilot job to be directed to a particular SE.
        At the same time this adds a requirement for the DIRAC site name for matchmaking.
    """

    diracSite = getSiteForCE( ceName )
    if not diracSite[ 'OK' ]:
      return diracSite
    if not diracSite[ 'Value' ]:
      return S_ERROR( 'No DIRAC site name found for CE %s' % ( ceName ) )

    diracSite = diracSite[ 'Value' ]
    self.setDestination( diracSite )
    self._addJDLParameter( 'GridRequiredCEs', ceName )
    self.setName( 'SAM-%s' % ( ceName ) )
    self.log.verbose( 'Set GridRequiredCEs to %s and destination to %s' % ( ceName, diracSite ) )

    return S_OK( True )

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
