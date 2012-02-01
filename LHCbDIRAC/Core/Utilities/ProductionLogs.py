""" Utilities to check the application log files (for production jobs)  
"""

__RCSID__ = "$Id$"

import re, os
from DIRAC import S_OK, S_ERROR

################################################################################

''' Allowed application names '''
__APPLICATION_NAMES__ = ['Boole', 'Gauss', 'Brunel', 'DaVinci', 'LHCb', 'Moore']

''' Well known application Errors '''
__APPLICATION_ERRORS__ = {
  'Terminating event processing loop due to errors' : 'Event Loop Not Terminated'
}

''' Well known Gaudi Errors '''
__GAUDI_ERRORS__ = {
  'Cannot connect to database'                                     : 'error database connection',
  'Could not connect'                                              : 'CASTOR error connection',
  'SysError in <TDCacheFile::ReadBuffer>: error reading from file' : 'DCACHE connection error',
  'Failed to resolve'                                              : 'IODataManager error',
  'Error: connectDataIO'                                           : 'connectDataIO error',
  'Error:connectDataIO'                                            : 'connectDataIO error',
  ' glibc '                                                        : 'Problem with glibc',
  'segmentation violation'                                         : 'segmentation violation',
  'GaussTape failed'                                               : 'GaussTape failed',
  'Writer failed'                                                  : 'Writer failed',
  'Bus error'                                                      : 'Bus error',
  'Standard std::exception is caught'                              : 'Exception caught',
  'User defined signal 1'                                          : 'User defined signal 1',
  'Not found DLL'                                                  : 'Not found DLL'
}

################################################################################

def _openFile( fileName ):
  """ just return the content on the file in input
  """

  if not os.path.exists( fileName ):
    raise IOError, 'Log File Not Available'

  if os.stat( fileName )[6] == 0:
    raise IOError, 'Log File Is Empty'

  fopen = open( fileName, 'r' )
  fileString = fopen.read()
  fopen.close()

  return fileString

################################################################################

def _guessAppName( fileString ):
  """ Given a log file (in a string), look for the application
  """

  for line in fileString.split( '\n' ):
    if ( re.search( 'Welcome to', line ) and re.search( 'version', line ) ) or \
       ( re.search( 'Welcome to', line ) and re.search( 'Revision', line ) ):

      applicationName = line.split()[2]

  if applicationName == 'ApplicationMgr':
    applicationName = 'LHCb'

  return applicationName

################################################################################

def _guessStepID( fileName ):
  ''' This is a horrible practice, and if the syntax changes, this will crash.
      We know that the log file names look like this:
      <AppName>_<prodName>_<jobName>_<stepName>.log
  '''

  guess = fileName
  guess = guess.replace( '.log', '' )
  guess = guess.split( '_' )

  if len( guess ) != 4:
    raise ValueError, 'Could not guess production, job and step from %s' % fileName

  prodName = guess[ 1 ]
  jobName = guess[ 2 ]
  stepName = guess[ 3 ]

  return [prodName, jobName, stepName]

################################################################################

def _checkLogEnd( fileString, applicationName ):
  """ This method uses Gaudi strings that are well known and determines
      success / failure based on conventions.
  """
  # Check if the application finish successfully
  toFind = 'Application Manager Finalized successfully'
  if applicationName.lower() == 'moore':
    #toFind = 'Service finalized successfully'
    toFind = ''

  if toFind:
    okay = re.findall( toFind, fileString )
    if not okay:
      raise NameError, '"%s" was not found in log...' % toFind

################################################################################

def _checkErrors( fileString, errDict, log ):

  for errString, description in errDict.items():
    log.info( 'Checking for "%s" meaning job would fail with "%s"' % ( errString, description ) )
    found = re.findall( errString, fileString )
    if found:
      log.error( 'Found error in log file => "%s"' % errString )
      raise NameError, description

#      if type == 'APPLICATION':
#        result = self.__getLastFile()
#        if result[ 'OK' ]:
#          lastFile = result[ 'Value' ]
#          log.info( 'Determined last file before crash to be: %s => ApplicationCrash' % lastFile )
#          self.dataSummary[ lastFile ] = 'ApplicationCrash'

################################################################################

def analyseLogFile( fileName, applicationName = '', stepName = '',
                    prod = '', job = '', jobType = '', log = None ):
  """ Analyse a log file
  """

  if not log:
    from DIRAC import gLogger
    log = gLogger.getSubLogger( 'analyseLogFile' )

  try:
    log.info( "Attempting to open log file: %s" % fileName )
    fileString = _openFile( fileName )

    #For the production case the application name will always be given, for the
    #standalone utility this may not always be true so try to guess
    if not applicationName:
      applicationName = _guessAppName( fileString )
      log.info( 'Guessed application name is "%s"' % ( applicationName ) )

    if not applicationName in __APPLICATION_NAMES__:
      log.error( 'Application name "%s" is not in allowed list: %s' % ( applicationName, ', '.join( __APPLICATION_NAMES__ ) ) )
      raise IOError, 'Log Analysis of %s Not Supported' % ( applicationName )

    #If we have no stepName, prodName and jobName ( typically from the
    #  script ), we try to get them from the log file name.
    if not ( prod and job and stepName ):
      log.info( 'Guessing production, job and step from %s' % fileName )
      res = _guessStepID( fileName )
      prodName = res[ 0 ]
      jobName = res[ 1 ]
      stepName = res[ 2 ]
      log.info( 'Guessed prod: %s, job: %s and step: %s' % ( prodName, jobName, stepName ) )

    # Check to find and of log
    _checkLogEnd( fileString, applicationName )

    # Check that no errors were seen in the log
    _checkErrors( fileString, __APPLICATION_ERRORS__, log )
    _checkErrors( fileString, __GAUDI_ERRORS__, log )

  except Exception, e:
    return S_ERROR( e )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
