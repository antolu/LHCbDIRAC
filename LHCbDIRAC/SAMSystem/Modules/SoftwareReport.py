''' LHCb SoftwareReport SAM Test Module

  Corresponds to SAM test CE-lhcb-softreport, utilizes the SoftwareManagementAgent
  to report the installation of LHCb software in site shared areas.

'''

import os
import sys

from DIRAC                                               import S_OK, S_ERROR, systemCall
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
from DIRAC.Core.Utilities                                import Os as DIRACOs

from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM import ModuleBaseSAM
from LHCbDIRAC.SAMSystem.Utilities             import Utils

__RCSID__ = "$Id$"

#installProject    = 'install_project.py'
#installProjectURL = 'http://lhcbproject.web.cern.ch/lhcbproject/dist/'

class SoftwareReport( ModuleBaseSAM ):

  def __init__( self ):
    '''
       Standard constructor for SAM Module
    '''
    ModuleBaseSAM.__init__( self )
    
    #Operations helper
    self.opH = Operations()
    
    #self.logFile  = 'sam-softreport.log'
    #self.testName = 'CE-lhcb-softreport'
    
    self.reportFlag = False

  def resolveInputVariables( self ):
    """ By convention the workflow parameters are resolved here.
    """
    
    ModuleBaseSAM.resolveInputVariables( self )
    
    if 'reportFlag' in self.step_commons:
      self.reportFlag = self.step_commons['reportFlag']
      if not isinstance( self.reportFlag, bool ):
        self.log.warn( 'Report flag set to non-boolean value %s, setting to False' % self.reportFlag )
        self.reportFlag = False

    self.log.verbose( 'reportFlag = %s' % self.reportFlag )    
    return S_OK()

  def _execute( self ):
    '''
       The main execution method of the SoftwareReport module.
    '''

    if not self.reportFlag:
      self.log.verbose( 'Disabled module via reportFlag' )
      return self.finalize( '%s test is disabled via control flag' % self.testName, 'Status INFO (= 0)', 'info' )

    #Check software
    result = self.__checkSoftware()
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )  
    softwareDict, softwareDictPb, softwareDictRemove = result[ 'Value' ]

    self.__writeSoftReport( softwareDict, softwareDictPb, softwareDictRemove )
    
    self.log.info( 'Test %s completed successfully' % self.testName )
    self.setApplicationStatus( '%s Successful' % self.testName )

    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 0)', 'ok' )

################################################################################

  def __checkSoftware( self ):
    '''
       Check software
    '''
    self.log.info( '>> __checkSoftware' )
      
    result = self.__getSoftware()
    if not result[ 'OK' ]:
      return result
    softwareActiveList, softwareDictRemove = result[ 'Value' ]

    result = Utils.getLocalPlatforms()
    if not result[ 'OK' ]:
      return result
    localPlatforms = result[ 'Value' ]
    
    self.log.verbose( 'LocalPlatforms: %s' % ( ', '.join( localPlatforms ) ) )
    
    softwareInstalled, softwareProblematic = {}, {}
          
    for systemConfig in localPlatforms.iterkeys():
      
      self.log.verbose( 'Checking software for %s platform' % localPlatforms )
      
      swDistPackageList = self.opH.getValue( '/SoftwareDistribution/%s' % systemConfig, [] )

      for appNameVersion in softwareActiveList:

        activePackage = '.'.join( appNameVersion )
          
        #Must check that package to install is supported by LHCb for requested system configuration
        if activePackage in swDistPackageList:
          _msg = 'Attempting to check %s %s for system configuration %s'
          self.log.info( _msg % ( appNameVersion[ 0 ], appNameVersion[ 1 ], systemConfig ) )
          result = self.__checkPackage( appNameVersion, systemConfig )

          if not result:
            softwareProblematic.setdefault( activePackage, [] )
            softwareProblematic[ activePackage ].append( systemConfig )  
          else:
            softwareInstalled.setdefault( activePackage, [] )
            softwareInstalled[ activePackage ].append( systemConfig )            
        else:
          self.log.info( "%s.%s is not on package List" % appNameVersion )

    return S_OK( ( softwareInstalled, softwareProblematic, softwareDictRemove ) )

  def __writeSoftReport( self, softwareInstalled, softwareProblematic, softwareDictRemove ):
    '''
       Write soft report
    '''
    self.log.info( '>> __writeSoftReport' )
    
    fd = open( 'Soft_install.html', 'w' )
    self.log.info( 'Applications properly installed in the area' )
    fd.write( '<H1>Applications properly installed in the area</H1>' )
    self.log.info( softwareInstalled )
    fd.write( self.getSoftwareReport( softwareInstalled ) )
    self.log.info( 'Applications NOT properly installed in the area' )
    fd.write( '<H1>Applications NOT properly installed in the area</H1>' )
    self.log.info( softwareProblematic )
    fd.write( self.getSoftwareReport( softwareProblematic ) )
    self.log.info( 'Applications that could be removed in the area' )
    fd.write( '<H1>Applications that could be removed in the area</H1>' )
    self.log.info( softwareDictRemove )
    fd.write( self.getSoftwareReport( softwareDictRemove ) )
    fd.close()

################################################################################

  def __getSoftware( self ):
    '''
       Get software
    '''
    self.log.info( '>> __getSoftware' )

    activeSoftware = '/SoftwareDistribution/Active'
    self.log.verbose( 'Getting Active software from %s' % activeSoftware )
    activeSoftwareList = self.opH.getValue( activeSoftware, [] )
    
    if not activeSoftwareList:
      self.log.error( 'The active list of software could not be retreived' )
      result = S_ERROR( activeSoftwareList )
      result[ 'Description' ] = 'The active list of software could not be retreived'
      result[ 'SamResult' ]   = 'error'
      return result
    
    #Sanity check. Also takes advantage of the fact we have to split the string 
    #so we do not have to do it again on the future.
    activeSoftwareListSplitted = []
    for appVersion in activeSoftwareList: 
      appNameVersion = appVersion.split( '.' )
      if len( appNameVersion ) != 2:
        self.log.error( 'AppNameVersion has length different than 2: %s' % appVersion )
        result = S_ERROR( appVersion )
        result[ 'Description' ] = 'Could not determine name and version of package:'
        result[ 'SamResult' ]   = 'warning'
        return result
      activeSoftwareListSplitted.append( appNameVersion )
    
    result = self.__checkSoftwareToBeRemoved()
    if not result[ 'OK' ]:
      result[ 'Description' ] = 'Problem SoftwareReport'
      result[ 'SamResult' ]   = 'warning'
      return result
    result = result[ 'Value' ]
    
    # ['ALL'] is mutable, caution !  
    softwareDictRemove = dict.fromkeys( result, [ 'ALL' ] )  
     
    return S_OK( ( activeSoftwareList, softwareDictRemove ) )

  ##############################################################################

  def getSoftwareReport( self, softwareDict ):
    '''
       Returns the list of software installed at the site organized by platform.
       If the test status is not successful, returns a link to the install test
       log.  Creates an html table for the results.
    '''
    self.log.info( '>> __getSoftwareReport' )

    #If software installation test was not run by this job e.g. is 'notice' status, do not add software report.

    self.log.verbose( softwareDict )
    rows = '<br><br><br>\n'
    rows += 'Software summary from job running on node with system configuration :\n'
    rows += '<br><br><br>'
    
    sortedKeys = softwareDict.keys()
    sortedKeys.sort()
    for appNameVersion in sortedKeys:
      archList   = softwareDict[appNameVersion]
      name       = appNameVersion.split( '.' )[0]
      version    = appNameVersion.split( '.' )[1]
      sysConfigs = ', '.join( archList )
      rows += '<tr><td> %s </td><td> %s </td><td> %s </td></tr>' % ( name, version, sysConfigs )

    self.log.debug( rows )

    table = '<table border="1" bordercolor="#000000" width="50%" bgcolor="#BCCDFE">'
    table += '<tr><td>Project Name</td><td>Project Version</td>'
    table += '<td>System Configurations</td></tr>' + rows + '</table>'

    self.log.debug( table )
    return table

  def __checkPackage( self, appNameVersionTuple, systemConfig ):
    '''
      check if given application is available in the given area
    '''
    self.log.verbose( '>> __checkPackage' )
    
#    #FIXME: get this out of here, it runs on a loop !
#    if not os.path.exists( '%s/%s' % ( os.getcwd(), installProject ) ):
#      _localname, _headers = urllib.urlretrieve( '%s%s' % ( installProjectURL, installProject ), installProject )
#      if not os.path.exists( '%s/%s' % ( os.getcwd(), installProject ) ):
#        self.log.error( '%s/%s could not be downloaded' % ( installProjectURL, installProject ) )
#        return False
#
#   #FIXME: WTF is this ?
#   #This simply cannot happen as sharedArea is generated with "CombinedSoftwareInstallation.getSharedArea"
#    localArea = self.sharedArea
#    if re.search( ':', self.sharedArea ):
#      localArea = ':'.split( self.sharedArea )[0]
#
#    _installProject = os.path.join( localArea, installProject )
#    if not os.path.exists( _installProject ):
#      try:
#        shutil.copy( _installProject, localArea )
#      except shutil.Error:
#        self.log.error( 'Failed (Error) to get:', _installProject )
#        return False
#      except IOError:
#        self.log.error( 'Failed (IOError) to get:', _installProject )
#        return False

    # Now run the installation
    curDir = os.getcwd()
    #NOTE: must cd to LOCAL area directory (install_project requirement)
    #os.chdir( localArea )
    os.chdir( self.sharedArea )

    #This is a Python trick that copies the dictionary instead of passing it as
    #reference. so we do not alter the former dictionary
    cmtEnvironment = dict( os.environ )
    cmtEnvironment[ 'MYSITEROOT' ] = self.sharedArea
    self.log.info( 'MYSITEROOT = %s' % cmtEnvironment[ 'MYSITEROOT' ] )
    
    cmtEnvironment[ 'CMTCONFIG' ] = systemConfig
    self.log.info( 'CMTCONFIG = %s' % cmtEnvironment[ 'CMTCONFIG' ] )
    cmtEnvironment[ 'LHCBTAR' ] = os.environ[ 'VO_LHCB_SW_DIR' ]
    self.log.info( 'LHCBTAR = %s' % cmtEnvironment[ 'LHCBTAR' ] )

    # Assumes it has been copied over properly on SoftwareInstallation step.
    installProjectPath  = os.path.join( self.sharedArea, 'install_project.py' )
    appName, appVersion = appNameVersionTuple
    
    cmdList  = [ sys.executable, installProjectPath ]
    #obsolete options
    #cmdList += [ '-d', '-p', appName, '-v', appVersion, '--check' ]
    cmdList += [ '-d', '--check', '-b', appName, appVersion ]
    self.log.info( 'Executing %s' % ' '.join( cmdList ) )
    
    #300 secs of timeout
    checkApp = systemCall( 300, cmdList, env = cmtEnvironment )
    os.chdir( curDir )
    
    decission = False
    
    if not checkApp[ 'OK' ]:
      _msg = 'Software check failed, missing software', '%s %s:\n%s'
      self.log.error( _msg % ( appName, appVersion, checkApp[ 'Value' ][ 2 ] ) )
    elif checkApp[ 'Value' ][ 0 ] != 0:
      _msg = 'Software check failed with non-zero status'
      self.log.error( _msg, '%s %s:\n%s' % ( appName, appVersion, checkApp[ 'Value' ][ 2 ] ) )
    elif checkApp[ 'Value' ][ 2 ]:
      self.log.error( 'Error reported with ok status for install_project check:\n%s' % checkApp[ 'Value' ][ 2 ] )
    else:
      decission = True

    return decission    

  def __checkSoftwareToBeRemoved( self ):
    '''
       Check if all application  in the  area are used or not
    '''

    self.log.verbose( '>> __checkSoftwareToBeRemoved' )

#FIXME: does this really apply ?
#    localArea = self.sharedArea
#    if re.search( ':', self.sharedArea ):
#      localArea = ':'.split( self.sharedArea )[0]

    #lbLogin = '%s/LbLogin' % localArea
    
    lblogin = '%s/LbLogin' % self.sharedArea
    self.log.verbose( 'Sourcing LbLogin "%s"' % lblogin )
    
    # 300 is the timeout for the call
    environment = DIRACOs.sourceEnv( 300, [ lblogin ], dict( os.environ ) )
    if not environment[ 'OK' ]:
      self.log.error( 'Error during lbLogin\n%s' % environment[ 'Message' ]  )
      return environment

    environment = environment[ 'outputEnv' ]
    self.log.debug( environment )
    
    if not 'LBSCRIPTS_HOME' in environment:
      self.log.error( 'LBSCRIPTS_HOME is not defined' )
      return S_ERROR( 'LBSCRIPTS_HOME is not defined' )

    usedProjectsPath = environment[ 'LBSCRIPTS_HOME' ] + '/InstallArea/scripts/usedProjects'
    if not os.path.exists( usedProjectsPath ):
      self.log.error( 'UsedProjects is not in the path %s' % usedProjectsPath )
      return S_ERROR( 'UsedProjects is not in the path %s' % usedProjectsPath )

    # Now run the installation
    curDir = os.getcwd()
    #NOTE: must cd to LOCAL area directory (install_project requirement)
    os.chdir( self.sharedArea )
    
    environment[ 'LHCBTAR' ] = environment[ 'VO_LHCB_SW_DIR' ] + '/lib'
    self.log.info( 'VO_LHCB_SW_DIR = %s' % environment[ 'VO_LHCB_SW_DIR' ] )
    self.log.info( 'LHCBTAR = %s' % environment[ 'LHCBTAR' ] )

    cmdList = [ 'usedProjects', '-r', '-v' ]
    self.log.info( 'Executing "%s"' % ' '.join( cmdList ) )
    
    usedProjects = systemCall( 300, cmdList, env = environment )
    os.chdir( curDir )
    
    #This looks like a bug, but it is not. This S_ERROR has also a 'Value' key
    #The third value [2] is the Message, the first one is the exit code 
    errorMsg = ''
    if not usedProjects[ 'OK' ]:
      errorMsg = 'Software check failed, missing software', '\n%s' % usedProjects[ 'Value' ][ 2 ]
    elif usedProjects[ 'Value' ][ 0 ] != 0:
      errorMsg = 'Software check failed with non-zero status', '\n%s' % ( usedProjects[ 'Value' ][ 2 ] )
    elif usedProjects[ 'Value' ][ 2 ]:
      #FIXME: what does thisreally mean ?? 
      errorMsg = 'Error reported with ok status for usedProjects:\n%s' % usedProjects[ 'Value' ][ 2 ]
    
    if errorMsg:
      self.log.error( errorMsg )
      return S_ERROR( errorMsg )     
    
    usedProjects        = usedProjects[ 'Value' ][ 1 ]
    softwareToBeRemoved = []

    for line in usedProjects.split( '\n' ):
      if 'remove' in line:
        #remove <PROJECT> version <VERSION>
        line = line.split()
        project, version = line[1], line[3]
        softwareToBeRemoved.append( '%s.%s' % ( project, version ) )
                  
    self.log.verbose( 'Applications that could be removed:' )
    self.log.verbose( softwareToBeRemoved )
    return S_OK( softwareToBeRemoved )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF