# $HeadURL$
''' LHCb SoftwareReport SAM Test Module

  Corresponds to SAM test CE-lhcb-softreport, utilizes the SoftwareManagementAgent
  to report the installation of LHCb software in site shared areas.

'''

import os
import shutil
import sys
import re
import urllib

from DIRAC                import S_OK, S_ERROR, gConfig, systemCall, gLogger
from DIRAC.Core.Utilities import Os as DIRACOs

from LHCbDIRAC.Core.Utilities.CombinedSoftwareInstallation  import getSharedArea, createSharedArea
from LHCbDIRAC.SAMSystem.Modules.ModuleBaseSAM              import ModuleBaseSAM

__RCSID__ = "$Id$"

installProject    = 'install_project.py'
installProjectURL = 'http://lhcbproject.web.cern.ch/lhcbproject/dist/'

class SoftwareReport( ModuleBaseSAM ):

  def __init__( self ):
    '''
       Standard constructor for SAM Module
    '''
    ModuleBaseSAM.__init__( self )
    
    self.logFile  = 'sam-softreport.log'
    self.testName = 'CE-lhcb-softreport'
    
    self.site = gConfig.getValue( '/LocalSite/Site', 'LCG.Unknown.ch' )

    #Workflow parameters for the test
    self.installProjectURL = None

  def resolveInputVariables( self ):
    '''
       By convention the workflow parameters are resolved here.
    '''
    
    #FIXME: looks to me like a wrong and old copy/paste from SoftwareInstallation
    ModuleBaseSAM.resolveInputVariables( self )

    if 'installProjectURL' in self.step_commons:
      self.installProjectURL = self.step_commons[ 'installProjectURL' ]
      if not isinstance( self.installProjectURL, str ) or not self.installProjectURL:
        self.log.warn( 'Install project URL not set to non-zero string parameter, setting to None' )
        self.installProjectURL = None

    self.log.verbose( 'Install project URL set to %s' % ( self.installProjectURL ) )
    return S_OK()

  def _execute( self ):
    '''
       The main execution method of the SoftwareReport module.
    '''

    if not 'SAMResults' in self.workflow_commons:
      _msg = 'Problem determining CE-lhcb-lock test result'
      return self.finalize( _msg, 'No SAMResults key in workflow commons', 'error' )

    if not self.enable:
      return self.finalize( '%s test is disabled via control flag' % self.testName, 'Status INFO (= 20)', 'info' )

    result = self.__checkSharedArea()
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )
    sharedArea = result[ 'Value' ]

#FIXME: there are bugs here, and it is not anymore used
#    #Check for optional install project URL
#    if self.installProjectURL:
#      self.writeToLog( 'Found specified install_project URL %s' % ( self.installProjectURL ) )
#      installProjectName = 'install_project.py'
#      if os.path.exists( '%s/%s' % ( os.getcwd(), installProjectName ) ):
#        self.writeToLog( 'Removing previous install project script from local area' )
#        os.remove( '%s/%s' % ( os.getcwd(), installProjectName ) )
#      installProjectFile = os.path.basename( self.installProjectURL )
#      localname, headers = urllib.urlretrieve( self.installProjectURL, installProjectFile )
#      if not os.path.exists( '%s/%s' % ( os.getcwd(), installProjectFile ) ):
#        return self.finalize( '%s could not be downloaded to local area' % ( self.installProjectURL ) )
#      else:
#        self.writeToLog( 'install_project downloaded from %s to local area' % ( self.installProjectURL ) )
#      self.writeToLog( 'Copying downloaded install_project to sharedArea %s' % sharedArea )
#      if not installProjectFile == installProjectName:
#        shutil.copy( '%s/%s' % ( os.getcwd(), installProjectFile ), '%s/%s' % ( os.getcwd(), installProjectName ) )
#      shutil.copy( '%s/%s' % ( os.getcwd(), installProjectName ), '%s/%s' % ( sharedArea, installProjectName ) )

    #Check software
    result = self.__checkSoftware( sharedArea )
    if not result[ 'OK' ]:
      return self.finalize( result[ 'Description' ], result[ 'Message' ], result[ 'SamResult' ] )  
    softwareDict, softwareDictPb, softwareDictRemove = result[ 'Value' ]

    self.__writeSoftReport( softwareDict, softwareDictPb, softwareDictRemove )
    
    self.log.info( 'Test %s completed successfully' % self.testName )
    self.setApplicationStatus( '%s Successful' % self.testName )

    return self.finalize( '%s Test Successful' % self.testName, 'Status OK (= 10)', 'ok' )

  ##############################################################################

  def __checkSharedArea( self ):
    '''
       Check sharedArea
    '''
    
    self.log.info( '>> __checkSharedArea' )
        
    if not createSharedArea():
      self.log.info( 'Can not get access to Shared Area for SW installation' )
      result = S_ERROR( 'Status ERROR (=50)' )
      result[ 'Description' ] = 'Could not determine shared area for site'
      result[ 'SamResult' ]   = 'error'
      
      return result
    
    sharedArea = getSharedArea()
    if not sharedArea or not os.path.exists( sharedArea ):
      # After previous check this error should never occur
      self.log.info( 'Could not determine sharedArea for site %s:\n%s' % ( self.site, sharedArea ) )
      result = S_ERROR( sharedArea )
      result[ 'Description' ] = 'Could not determine shared area for site'
      result[ 'SamResult' ]   = 'critical' 
      
      return result
    
    else:
      self.log.info( 'Software shared area for site %s is %s' % ( self.site, sharedArea ) )

    return S_OK( sharedArea )

  def __getSoftware( self, sharedArea ):
    '''
       Get software
    '''
    self.log.verbose( '>> __getSoftware' )

    activeSoftware = '/Operations/SoftwareDistribution/Active'
    installList = gConfig.getValue( activeSoftware, [] )
    if not installList:
      result = S_ERROR( activeSoftware )
      result[ 'Description' ] = 'The active list of software could not be retreived from'
      result[ 'SamResult' ]   = 'error'
      return result    
    
    result = self.__checkArea( sharedArea )
    if not result[ 'OK' ]:
      result[ 'Description' ] = 'Problem SoftwareReport'
      result[ 'SamResult' ]   = 'warning'
      return result
      
    softwareDictRemove = {}  
      
    for app, vers in result[ 'Value' ].items():
      for ver in vers:
        appName = app + '.' + ver
        softwareDictRemove[ appName ] = 'ALL'    
    
    return S_OK( ( installList, softwareDictRemove ) )

  def __getLocalPlatforms( self ):
    '''
       Get local platforms
    '''
    self.log.verbose( '>> __getLocalPlatforms' )
    
    localArch = gConfig.getValue( '/LocalSite/Architecture', '' )
    if not localArch:
            
      result = S_ERROR( 'Could not get /LocalSite/Architecture' )
      result[ 'Description' ] = '/LocalSite/Architecture is not defined in the local configuration' 
      result[ 'SamResult' ]   = 'error'
      
      return result

    #must get the list of compatible platforms for this architecture
    localPlatforms = gConfig.getOptionsDict( '/Resources/Computing/OSCompatibility' )
    if not localPlatforms:
      _msg = 'Could not obtain compatible platforms for /Resources/Computing/OSCompatibility/'
      result = S_ERROR( 'Could not get /Resources/Computing/OSCompatibility' )
      result[ 'Description' ] = '/Resources/Computing/OSCompatibility is not defined in the local configuration' 
      result[ 'SamResult' ]   = 'error'
      
      return result

    return S_OK( localPlatforms )

  def __checkSoftware( self, sharedArea ):
    '''
       Check software
    '''
    self.log.info( '>> __checkSoftware' )
    
    if not self.enable:  
      self.log.info( 'Software check is disabled via enable flag' )      
      return S_OK()
      
    result = self.__getSoftware( sharedArea )
    if not result[ 'OK' ]:
      return result
    installList, softwareDictRemove = result[ 'Value' ]

    result = self.__getLocalPlatforms()
    if not result[ 'OK' ]:
      return result
    localPlatforms = result[ 'Value' ]
    
    softwareDict, softwareDictPb = {}, {}
          
    for systemConfig in localPlatforms[ 'Value' ].keys():
      packageList = gConfig.getValue( '/Operations/SoftwareDistribution/%s' % ( systemConfig ), [] )

      for installPackage in installList:
        
        appNameVersion = '.'.split( installPackage )
        if not len( appNameVersion ) == 2:
          result = S_ERROR( installPackage )
          result[ 'Description' ] = 'Could not determine name and version of package:'
          result[ 'SamResult' ]   = 'warning'
          return result
          
        #Must check that package to install is supported by LHCb for requested system configuration
        if installPackage in packageList:
          _msg = 'Attempting to check %s %s for system configuration %s'
          self.log.info( _msg % ( appNameVersion[ 0 ], appNameVersion[ 1 ], systemConfig ) )

          result = self.__checkPackage( appNameVersion, systemConfig, sharedArea )

          if not result:  
            if installPackage in softwareDictPb:
              current = softwareDictPb[ installPackage ]
              current.append( systemConfig )
              softwareDictPb[ installPackage ] = current
            else:
              softwareDictPb[ installPackage ] = [ systemConfig ]
          else:
            if installPackage in softwareDict:
              current = softwareDict[ installPackage ]
              current.append( systemConfig )
              softwareDict[ installPackage ] = current
            else:
              softwareDict[ installPackage ] = [ systemConfig ]

    return S_OK( ( softwareDict, softwareDictPb, softwareDictRemove ) )
      
    
  def __writeSoftReport( self, softwareDict, softwareDictPb, softwareDictRemove ):
    '''
       Write soft report
    '''
    self.log.info( '>> __writeSoftReport' )
    
    fd = open( 'Soft_install.html', 'w' )
    self.log.info( 'Applications properly installed in the area' )
    fd.write( '<H1>Applications properly installed in the area</H1>' )
    self.log.info( softwareDict )
    fd.write( self.getSoftwareReport( softwareDict ) )
    self.log.info( 'Applications NOT properly installed in the area' )
    fd.write( '<H1>Applications NOT properly installed in the area</H1>' )
    self.log.info( softwareDictPb )
    fd.write( self.getSoftwareReport( softwareDictPb ) )
    self.log.info( 'Applications that could be remove in the area' )
    fd.write( '<H1>Applications that could be remove in the area</H1>' )
    self.log.info( softwareDictRemove )
    fd.write( self.getSoftwareReport( softwareDictRemove ) )
    fd.close()

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

  def __checkPackage( self, app, config, area ):
    '''
      check if given application is available in the given area
    '''
    self.log.verbose( '>> __checkPackage' )
    
    if not os.path.exists( '%s/%s' % ( os.getcwd(), installProject ) ):
      _localname, _headers = urllib.urlretrieve( '%s%s' % ( installProjectURL, installProject ), installProject )
      if not os.path.exists( '%s/%s' % ( os.getcwd(), installProject ) ):
        self.log.error( '%s/%s could not be downloaded' % ( installProjectURL, installProject ) )
        return False

    if not area:
      return False

    localArea = area
    if re.search( ':', area ):
      localArea = ':'.split( area )[0]

    appName = app[0]
    appVersion = app[1]

    _installProject = os.path.join( localArea, installProject )
    if not os.path.exists( _installProject ):
      try:
        shutil.copy( _installProject, localArea )
      except shutil.Error:
        self.log.error( 'Failed (Error) to get:', _installProject )
        return False
      except IOError:
        self.log.error( 'Failed (IOError) to get:', _installProject )
        return False

    # Now run the installation
    curDir = os.getcwd()
    #NOTE: must cd to LOCAL area directory (install_project requirement)
    os.chdir( localArea )

    cmtEnv = dict( os.environ )
    cmtEnv['MYSITEROOT'] = area
    self.log.info( 'Defining MYSITEROOT = %s' % area )
    cmtEnv['CMTCONFIG'] = config
    self.log.info( 'Defining CMTCONFIG = %s' % config )
    cmtEnv['LHCBTAR'] = os.environ['VO_LHCB_SW_DIR']
    self.log.info( 'Defining LHCBTAR = %s' % os.environ['VO_LHCB_SW_DIR'] )

    cmdTuple = [ sys.executable ]
    cmdTuple += [ _installProject ]
    cmdTuple += [ '-d' ]
    cmdTuple += [ '-p', appName ]
    cmdTuple += [ '-v', appVersion ]
    cmdTuple += [ '--check' ]

    self.log.info( 'Executing %s' % ' '.join( cmdTuple ) )
    timeout = 300
    ret = systemCall( timeout, cmdTuple, env = cmtEnv )

    os.chdir( curDir )
    if not ret[ 'OK' ]:
      _msg = 'Software check failed, missing software', '%s %s:\n%s'
      self.log.error( _msg % ( appName, appVersion, ret[ 'Value' ][ 2 ] ) )
      return False
    if ret[ 'Value' ][ 0 ]: # != 0
      _msg = 'Software check failed with non-zero status'
      self.log.error( _msg, '%s %s:\n%s' % ( appName, appVersion, ret[ 'Value' ][ 2 ] ) )
      return False

    if ret[ 'Value' ][ 2 ]:
      self.log.debug( 'Error reported with ok status for install_project check:\n%s' % ret[ 'Value' ][ 2 ] )

    return True

  def __checkArea( self, area ):
    '''
       Check if all application  in the  area are used or not
    '''

    if not area:
      return S_ERROR( 'No shared area' )

    localArea = area
    if re.search( ':', area ):
      localArea = ':'.split( area )[0]

    lbLogin = '%s/LbLogin' % localArea
    ret = DIRACOs.sourceEnv( 300, [ lbLogin ], dict( os.environ ) )
    if not ret['OK']:
      gLogger.warn( 'Error during lbLogin\n%s' % ret )
      self.log.error( 'Error during lbLogin\n%s' % ret )

      return S_ERROR( 'Error during lbLogin' )

    lbenv = ret[ 'outputEnv' ]

    if not 'LBSCRIPTS_HOME' in lbenv:
      self.log.error( 'LBSCRIPTS_HOME is not defined' )
      return S_ERROR( 'LBSCRIPTS_HOME is not defined' )

    if not os.path.exists( lbenv['LBSCRIPTS_HOME'] + '/InstallArea/scripts/usedProjects' ):
      self.log.error( 'UsedProjects is not in the path' )
      return S_ERROR( 'UsedProjects is not in the path' )

    # Now run the installation
    curDir = os.getcwd()
    #NOTE: must cd to LOCAL area directory (install_project requirement)
    os.chdir( localArea )
    software_remove    = {}
    lbenv[ 'LHCBTAR' ] = os.environ[ 'VO_LHCB_SW_DIR' ] + '/lib'
    self.log.info( 'Defining LHCBTAR = %s' % os.environ['VO_LHCB_SW_DIR'] )

    cmdTuple = ['usedProjects']
    cmdTuple += ['-r']
    cmdTuple += ['-v']

    self.log.info( 'Executing %s' % ' '.join( cmdTuple ) )
    timeout = 300
    ret = systemCall( timeout, cmdTuple, env = lbenv )
    self.log.info( ret )
    os.chdir( curDir )
    if not ret[ 'OK' ]:
      self.log.error( 'Software check failed, missing software', '\n%s' % ( ret[ 'Value' ][ 2 ] ) )
      return S_ERROR( 'Software check failed, missing software \n%s' % ( ret[ 'Value' ][ 2 ] ) )
    if ret[ 'Value' ][ 0 ]: # != 0
      self.log.error( 'Software check failed with non-zero status', '\n%s' % ( ret[ 'Value' ][ 2 ] ) )
      return S_ERROR( 'Software check failed with non-zero status \n%s' % ( ret[ 'Value' ][ 2 ] ) )

    if ret[ 'Value' ][ 2 ]:
      self.log.debug( 'Error reported with ok status for install_project check:\n%s' % ret[ 'Value' ][ 2 ] )

    for line in ret[ 'Value' ][ 1 ].split( '\n' ):
      if line.find( 'remove' ) != -1:
        line = line.split()
        if line[1] in software_remove:
          current = software_remove[ line[ 1 ] ]
          current.append( line[ 3 ] )
          software_remove[ line[ 1 ] ] = current
        else:
          software_remove[ line[ 1 ] ] = [ line[ 3 ] ]
    self.log.info( 'Applications that could be remove' )
    self.log.info( software_remove )
    return S_OK( software_remove )

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF