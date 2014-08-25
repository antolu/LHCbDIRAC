#####################################################################
# File: TargzJobLogAgent.py
########################################################################
""" :mod: TargzJobLogAgent
    ======================

    .. module: TargzJobLogAgent
    :synopsis: Compress old jobs
"""
# # imports
import os
import shutil
import glob
import re
from datetime import datetime, timedelta
import tarfile
# # from DIRAC
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule import AgentModule
from DIRAC.Resources.Storage.StorageElement import StorageElement

__RCSID__ = "$Id$"

AGENT_NAME = "DataManagement/TargzJobLogAgent"

class TargzJobLogAgent( AgentModule ):
  """
  .. class:: TargzJobLogAgent

  :param int pollingTime: polling time
  :param str logPath: log path location
  :param StorageElement storageElement: CERN-tape SE
  :param str destDirectory: destination directory
  :param str tempDirectory: temporal directory for tar files
  """
  pollingTime = 3600
  logPath = "/storage/lhcb/MC/MC09/LOG"
  storageElement = None
  destDirectory = "/lhcb/backup/log"
  tempDirectory = "/opt/dirac/tmp"

  def __init__( self, *args, **kwargs ):
    ''' c'tor
    '''
    AgentModule.__init__( self, *args, **kwargs )

    self.storageElement = StorageElement( "CERN-tape" )

  def initialize( self ):
    """ agent initialisation """

    self.log.info( "PollingTime %d hours" % ( int( self.pollingTime ) / 3600 ) )

    self.logPath = self.am_getOption( 'LogPath', self.logPath )
    self.log.info( "LogPath", self.logPath )

    self.actions = self.am_getOption( 'Actions', ['SubProductions', 'Jobs'] )
    self.log.info( "Actions", self.actions )

    self.tempDirectory = self.am_getOption( 'TempDirectory', self.tempDirectory )
    self.log.info( "TempDirectory", self.tempDirectory )

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/TestManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'TestManager' )

    return S_OK()

  def execute( self ):
    """ execution in one cycle """

    self.log.info( 'Starting Agent loop' )

    path = os.path.abspath( self.logPath )

    jobage = self.am_getOption( 'JobAgeDays', 100 )
    self.log.info( "JobAgeDays", jobage )
    prodage = self.am_getOption( 'ProdAgeDays', 365 )
    self.log.info( "ProdAgeDays", prodage )

    g1 = self.am_getOption( 'ProductionGlob', '????????' )
    self.log.info( "ProductionGlob", g1 )
    g2 = self.am_getOption( 'SubdirGlob', '????' )
    self.log.info( "SubdirGlob", g2 )
    g3 = self.am_getOption( 'JobGlob', '????' )
    self.log.info( "JobGlob", g3 )

    logPathList = self.am_getOption( 'LogPathList', [] )
    self.log.info( "LogPathList", logPathList )

    if 'SubProductions' in self.actions:

      numberOfTared = 0
      numberOfFailed = 0

      for path in logPathList:
        self.log.info( "LogPath", path )
        for subprodpath in self._iFindOldSubProd( path, g1, g2, prodage ):
          pathlist = subprodpath.split( "/" )
          sub = pathlist[-1]
          prod = pathlist[-2]
          self.log.info( "Found Old Log", "Production %s, subProduction %s" % ( prod, sub ) )
          res = self._tarSubProdDir( path, prod, sub )
          if res['OK']:
            numberOfTared += 1
          else:
            numberOfFailed += 1

      self.log.info( "Number of tared subproduction %d" % numberOfTared )
      self.log.info( "Number of failed subproduction %d" % numberOfFailed )

    if 'Jobs' in self.actions:

      numberOfTared = 0
      numberOfFailed = 0

      for path in logPathList:
        self.log.info( "LogPath", path )
        for jobpath in self._iFindOldJob( path, g1, g2, g3, jobage ):
          pathlist = jobpath.split( "/" )
          job = pathlist[-1]
          prod = pathlist[-3]
          self.log.debug( "Found Old Log", "Production %s, Job %s" % ( prod, job ) )

          name = prod + "_" + job + ".tgz"
          try:
            lines = open( os.path.join( jobpath, 'index.html' ) ).read()
            lines = lines.replace( '</title>', ' compressed</title>' )
            lines = lines.replace( '</h3>', ' compressed</h3>', 1 )
            lines = re.compile( '<a href.*</a><br>.*\n' ).sub( '', lines )
            lines = lines.replace( 'compressed</h3>', 'compressed</h3>\n<br><a href="%s">%s</a><br>' % ( name, name ) )

            self._tarJobDir( path, prod, job )
            indexHTML = open( os.path.join( jobpath, 'index.html' ), 'w' )
            indexHTML.write( lines )
            indexHTML.close()
            numberOfTared += 1
          except Exception, x:
            self.log.warn( "Exception during taring %s " % x, "Production %s, Job %s" % ( prod, job ) )
            numberOfFailed += 1

      self.log.info( "Number of tared jobs %d" % numberOfTared )
      self.log.info( "Number of failed jobs %d" % numberOfFailed )

    return S_OK()

  @staticmethod
  def _iFindOldJob( path, g1, g2, g3, agedays ):
    """ old job directory generator """

    c1 = re.compile( '^\d{8}$' )
    c2 = re.compile( '^\d{4}$' )
    c3 = re.compile( '^\d{8}$' )

    def iFindDir( path, gl, reobject ):
      """ directory generator """
      dirs = glob.glob( os.path.join( path, gl ) )
      for directory in dirs:
        name = os.path.basename( directory )
        if reobject.match( name ) and os.path.isdir( directory ):
          yield directory

    for d1 in iFindDir( path, g1, c1 ):
      for d2 in iFindDir( d1, g2, c2 ):
        for d3 in iFindDir( d2, os.path.basename( d2 ) + g3, c3 ):
          mtime = os.path.getmtime( d3 )  # os.stat( d3 )[8]
          modified = datetime.fromtimestamp( mtime )
          if datetime.now() - modified > timedelta( days = agedays ):
            prod = os.path.basename( d1 )
            job = os.path.basename( d3 )
            name = prod + "_" + job + ".tgz"
            if not os.path.exists( os.path.join( d3, name ) ):
              yield d3

  @staticmethod
  def _tarJobDir( path, prod, job ):
    """ tar job directory """
    oldpath = os.getcwd()
    try:
      name = prod + "_" + job + ".tgz"
      jobpath = os.path.join( path, prod, job[0:4], job )
      files = os.listdir( jobpath )
      os.chdir( jobpath )

      tarFile = tarfile.open( name, "w:gz" )
      for fd in files:
        tarFile.add( fd )
      tarFile.close()
      for fd in files:
        os.remove( fd )
    finally:
      os.chdir( oldpath )

  @staticmethod
  def _iFindOldSubProd( path, g1, g2, agedays ):
    """ subprod directory generator """

    c1 = re.compile( '^\d{8}$' )
    c2 = re.compile( '^\d{4}$' )

    def iFindDir( path, gl, reobject ):
      """ directory generator """
      dirs = glob.glob( os.path.join( path, gl ) )
      for directory in dirs:
        name = os.path.basename( directory )
        if reobject.match( name ) and os.path.isdir( directory ):
          yield directory

    for d1 in iFindDir( path, g1, c1 ):
      for d2 in iFindDir( d1, g2, c2 ):
        mtime = os.path.getmtime( d2 )  # os.stat( d2 )[8]
        modified = datetime.fromtimestamp( mtime )
        if datetime.now() - modified > timedelta( days = agedays ):
          yield d2

  def _tarSubProdDir( self, path, prod, sub ):
    """ create tar file for old prod directories """
    oldpath = os.getcwd()

    date = str( datetime.now() ).split( " " )[0]

    tarname = self.tempDirectory + "/" + prod + "_" + sub + ".tgz"
    destFile = self.destDirectory + "/" + prod + "_" + sub + "_" + date + ".tgz"

    res = self.storageElement.getPfnForLfn( destFile )
    if res['OK'] and destFile in res['Value']['Successful']:
      pfn = res["Value"]['Successful'][destFile]
    else:
      self.log.error( "getPfnForLfnfor file %s" % destFile, res.get( 'Message', res.get( 'Value', {} ).get( 'Failed', {} ).get( pfn ) ) )
      return S_ERROR()

    res = self.storageElement.exists( pfn )
    if not res['OK']:
      self.log.error( "Can not check file exists %s" % pfn, res['Message'] )
      return S_ERROR()
    if res['Value']['Successful'].get( pfn, False ):
      self.log.error( "file exists ", pfn )
      return S_ERROR()

    tared = False
    try:
      os.chdir( path )
      subprodpath = os.path.join( prod, sub )
      tarFile = tarfile.open( tarname, "w:gz" )
      tarFile.add( subprodpath )
      tarFile.close()
      tared = True
    finally:
      os.chdir( oldpath )

    if not tared:
      os.remove( tarname )
      self.log.error( "Can not tar file %s", subprodpath )
      return S_ERROR()

    putok = False
    fileDict = {pfn:tarname}
    self.log.info( "putFile", fileDict )
    res = self.storageElement.putFile( fileDict )
    if res['OK']:
      if not res['Value']['Failed']:
        subprodpath = os.path.join( path, prod, sub )
        self.log.info( "rmTree", subprodpath )
        shutil.rmtree( subprodpath )
        putok = True
      else:
        self.log.error( "putFile", res['Value']['Failed'] )
    else:
      self.log.error( "putFile", res['Message'] )

    os.remove( tarname )

    if putok:
      return S_OK()
    else:
      return S_ERROR()
