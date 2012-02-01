""" Utilities to check the application log files (for production jobs)  
"""

__RCSID__ = "$Id$"

import re, os
from DIRAC import S_OK, S_ERROR

class LogError( Exception ):

  def __init__( self, message = "" ):

    self.message = message
    Exception.__init__( self, message )

  def __str__( self ):
    return "LogError:" + repr( self.message )

################################################################################

class ProductionLog:
  """ Encapsulate production log info
  """

  def __init__( self, fileName, applicationName = '',
                prodName = '', jobName = '', stepName = '',
                log = None ):

    ''' Allowed application names '''
    self.__APPLICATION_NAMES__ = ['Boole', 'Gauss', 'Brunel', 'DaVinci', 'LHCb', 'Moore']

    ''' Well known application Errors '''
    self.__APPLICATION_ERRORS__ = {
      'Terminating event processing loop due to errors' : 'Event Loop Not Terminated'
    }

    ''' Well known Gaudi Errors '''
    self.__GAUDI_ERRORS__ = {
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
      'Not found DLL'                                                  : 'Not found DLL',
      'std::bad_alloc'                                                 : 'FATAL Bad alloc'
    }


    if not log:
      from DIRAC import gLogger
      self.log = gLogger.getSubLogger( 'analyseLogFile' )
    else:
      self.log = log

    self.fileName = fileName

    self.log.info( "Attempting to open log file: %s" % fileName )
    if not os.path.exists( fileName ):
      raise LogError, 'Log File Not Available'

    if os.stat( fileName )[6] == 0:
      raise LogError, 'Log File Is Empty'

    fopen = open( fileName, 'r' )
    self.fileString = fopen.read()
    fopen.close()

    self.applicationName = applicationName
    if not self.applicationName:
      self.__guessAppName()
      self.log.info( 'Guessed application name is "%s"' % ( self.applicationName ) )
    if not self.applicationName in self.__APPLICATION_NAMES__:
      self.log.error( 'Application name "%s" is not in allowed list: %s' % ( applicationName, ', '.join( self.__APPLICATION_NAMES__ ) ) )
      raise LogError, 'Log Analysis of %s Not Supported' % ( applicationName )

################################################################################

  def analyse( self ):
    """ analyse the log
    """
    try:
      self.__checkErrors()
      self.__checkFinish()
      self.__checkLogEnd()
      return S_OK( 'Logs OK' )
    except LogError, e:
      self.log.error( "Found error in " + self.fileName + ": " + str( e ) )
      return S_ERROR( e )

################################################################################

  def _guessStepID( self ):
    ''' This is a horrible practice, and if the syntax changes, this will crash.
        We know that the log file names look like this:
        <AppName>_<prodName>_<jobName>_<stepName>.log
    '''

    guess = self.fileName
    guess = guess.replace( '.log', '' )
    guess = guess.split( '_' )

    if len( guess ) != 4:
      raise LogError, 'Could not guess production, job and step from %s' % self.fileName

    self.prodName = guess[ 1 ]
    self.jobName = guess[ 2 ]
    self.stepName = guess[ 3 ]

################################################################################

  def __checkErrors( self ):

    for errString in self.__GAUDI_ERRORS__.keys():
  #    log.info( 'Checking for "%s" meaning job would fail with "%s"' % ( errString, description ) )
      found = re.findall( errString, self.fileString )
      if found:
        raise LogError, errString

  #      if type == 'APPLICATION':
  #        result = self.__getLastFile()
  #        if result[ 'OK' ]:
  #          lastFile = result[ 'Value' ]
  #          self.log.info( 'Determined last file before crash to be: %s => ApplicationCrash' % lastFile )
  #          self.dataSummary[ lastFile ] = 'ApplicationCrash'

################################################################################

  def __checkFinish( self ):

    for errString in self.__APPLICATION_ERRORS__.keys():
  #    self.log.info( 'Checking for "%s" meaning job would fail with "%s"' % ( errString, description ) )
      found = re.findall( errString, self.fileString )
      if found:
        raise LogError, errString

################################################################################

  def __checkLogEnd( self ):
    """ This method uses Gaudi strings that are well known and determines
        success / failure based on conventions.
    """
    # Check if the application finish successfully
    toFind = 'Application Manager Finalized successfully'
    if self.applicationName.lower() == 'moore':
      #toFind = 'Service finalized successfully'
      toFind = ''

    if toFind:
      okay = re.findall( toFind, self.fileString )
      if not okay:
        raise LogError, '"%s" was not found in the log' % toFind

################################################################################

  def __guessAppName( self ):
    """ Given a log file (in a string), look for the application
    """

    for line in self.fileString.split( '\n' ):
      if ( re.search( 'Welcome to', line ) and re.search( 'version', line ) ) or \
         ( re.search( 'Welcome to', line ) and re.search( 'Revision', line ) ):

        self.applicationName = line.split()[2]

    if self.applicationName == 'ApplicationMgr':
      self.applicationName = 'LHCb'

    if not self.applicationName:
      raise LogError, "Could not guess the app name"

    return self.applicationName

################################################################################


def analyseLogFile( fileName, applicationName = '', prod = '', job = '',
                    stepName = '', log = None, lf_o = None ):
  """ Analyse a log file
  """

  try:
    if not lf_o:
      lf_o = ProductionLog( fileName, applicationName, prod, job, stepName, log = log )
    return lf_o.analyse()
  except LogError, e:
    return S_ERROR ( str( e ) )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
