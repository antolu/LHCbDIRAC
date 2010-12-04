########################################################################
# $HeadURL$
########################################################################

__RCSID__ = "$Id$"

""" Compress Old Jobs 
"""

from DIRAC                                                    import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule                              import AgentModule
from DIRAC.Core.Utilities.Subprocess                          import shellCall
from DIRAC.Resources.Storage.StorageElement                   import StorageElement
import sys, os, shutil
import time
import types
import glob
import re
from datetime import datetime, timedelta
import tarfile

AGENT_NAME = "DataManagement/TargzJobLogAgent"

class TargzJobLogAgent( AgentModule ):

  def initialize( self ):

    self.logLevel = self.am_getOption( 'LogLevel', 'INFO' )
    gLogger.info( "LogLevel", self.logLevel )
    gLogger.setLevel( self.logLevel )

    self.pollingTime = self.am_getOption( 'PollingTime', 3600 )
    gLogger.info( "PollingTime %d hours" % ( int( self.pollingTime ) / 3600 ) )

    self.logPath = self.am_getOption( 'LogPath', '/storage/lhcb/MC/MC09/LOG' )
    gLogger.info( "LogPath", self.logPath )

    # This sets the Default Proxy to used as that defined under
    # /Operations/Shifter/SAMManager
    # the shifterProxy option in the Configuration can be used to change this default.
    self.am_setOption( 'shifterProxy', 'SAMManager' )

    self.storageElement = StorageElement( "CERN-tape" )
    self.destDirectory = "/lhcb/backup/log"

    return S_OK()

  def execute( self ):

    gLogger.info( 'Starting Agent loop' )

    path = os.path.abspath( self.logPath )

#    jobage = gConfig.getValue(self.section+'/JobAgeDays', 100)
    jobage = self.am_getOption( 'JobAgeDays', 100 )
    gLogger.info( "JobAgeDays", jobage )
#    prodage = gConfig.getValue(self.section+'/ProdAgeDays', 365)
    prodage = self.am_getOption( 'ProdAgeDays', 365 )
    gLogger.info( "ProdAgeDays", prodage )

#    g1 = gConfig.getValue(self.section+'/ProductionGlob', '????????')
    g1 = self.am_getOption( 'ProductionGlob', '????????' )
    gLogger.info( "ProductionGlob", g1 )
#    g2 = gConfig.getValue(self.section+'/SubdirGlob', '????')
    g2 = self.am_getOption( 'SubdirGlob', '????' )
    gLogger.info( "SubdirGlob", g2 )
#    g3 = gConfig.getValue(self.section+'/JobGlob', '????')
    g3 = self.am_getOption( 'JobGlob', '????' )
    gLogger.info( "JobGlob", g3 )

    numberOfTared = 0
    numberOfFailed = 0
    for subprodpath in self._iFindOldSubProd( path, g1, g2, prodage ):
      pathlist = subprodpath.split( "/" )
      sub = pathlist[-1]
      prod = pathlist[-2]
      gLogger.info( "Found Old Log", "Production %s, subProduction %s" % ( prod, sub ) )
      res = self._tarSubProdDir( path, prod, sub )
      if res['OK']:
        numberOfTared += 1
      else:
        numberOfFailed += 1

    gLogger.info( "Number of tared subproduction %d" % numberOfTared )
    gLogger.info( "Number of failed subproduction %d" % numberOfFailed )

    numberOfTared = 0
    numberOfFailed = 0

    for jobpath in self._iFindOldJob( path, g1, g2, g3, jobage ):
      pathlist = jobpath.split( "/" )
      job = pathlist[-1]
      prod = pathlist[-3]
      gLogger.debug( "Found Old Log", "Production %s, Job %s" % ( prod, job ) )

      name = prod + "_" + job + ".tgz"
      try:
        lines = open( os.path.join( jobpath, 'index.html' ) ).read()
        lines = lines.replace( '</title>', ' compressed</title>' )
        lines = lines.replace( '</h3>', ' compressed</h3>', 1 )
        lines = re.compile( '<a href.*</a><br>.*\n' ).sub( '', lines )
        lines = lines.replace( 'compressed</h3>', 'compressed</h3>\n<br><a href="%s">%s</a><br>' % ( name, name ) )

        self._tarJobDir( path, prod, job )
        open( os.path.join( jobpath, 'index.html' ), 'w' ).write( lines )
        numberOfTared += 1
      except Exception, x:
        gLogger.warn( "Exception during taring %s" % x, "Production %s, Job %s" % ( prod, job ) )
        numberOfFailed += 1

    gLogger.info( "Number of tared jobs %d" % numberOfTared )
    gLogger.info( "Number of failed jobs %d" % numberOfFailed )

    return S_OK()

  def _iFindOldJob( self, path, g1, g2, g3, agedays ):

    p1 = '^\d{8}$'
    c1 = re.compile( p1 )
    p2 = '^\d{4}$'
    c2 = re.compile( p2 )
    p3 = '^\d{8}$'
    c3 = re.compile( p3 )

    def iFindDir( path, gl, reobject ):

      dirs = glob.glob( os.path.join( path, gl ) )
      for d in dirs:
        name = os.path.basename( d )
        if reobject.match( name ) and os.path.isdir( d ):
          yield d

    for d1 in iFindDir( path, g1, c1 ):
      for d2 in iFindDir( d1, g2, c2 ):
        for d3 in iFindDir( d2, os.path.basename( d2 ) + g3, c3 ):
          mtime = os.stat( d3 )[8]
          modified = datetime.fromtimestamp( mtime )
          if datetime.now() - modified > timedelta( days = agedays ):
            prod = os.path.basename( d1 )
            job = os.path.basename( d3 )
            name = prod + "_" + job + ".tgz"
            if not os.path.exists( os.path.join( d3, name ) ):
              yield d3

  def _tarJobDir( self, path, prod, job ):

    oldpath = os.getcwd()
    try:
      name = prod + "_" + job + ".tgz"
      jobpath = os.path.join( path, prod, job[0:4], job )
      files = os.listdir( jobpath )
      os.chdir( jobpath )

      tarFile = tarfile.open( name, "w:gz" )
      for f in files:
        tarFile.add( f )
      tarFile.close()
      for f in files:
        os.remove( f )
    finally:
      os.chdir( oldpath )


  def _iFindOldSubProd( self, path, g1, g2, agedays ):

    p1 = '^\d{8}$'
    c1 = re.compile( p1 )
    p2 = '^\d{4}$'
    c2 = re.compile( p2 )

    def iFindDir( path, gl, reobject ):

      dirs = glob.glob( os.path.join( path, gl ) )
      for d in dirs:
        name = os.path.basename( d )
        if reobject.match( name ) and os.path.isdir( d ):
          yield d

    for d1 in iFindDir( path, g1, c1 ):
      for d2 in iFindDir( d1, g2, c2 ):
        mtime = os.stat( d2 )[8]
        modified = datetime.fromtimestamp( mtime )
        if datetime.now() - modified > timedelta( days = agedays ):
          yield d2

  def _tarSubProdDir( self, path, prod, sub ):

    oldpath = os.getcwd()

    tarname = "/tmp/" + prod + "_" + sub + ".tgz"
    destFile = self.destDirectory + "/" + prod + "_" + sub + ".tgz"

    if not self._noStorageFile( destFile ):
      gLogger.info( "File exist ", destFile )
      return S_OK()

    try:
      os.chdir( path )
      subprodpath = os.path.join( prod, sub )

      tarFile = tarfile.open( tarname, "w:gz" )
      tarFile.add( subprodpath )
      tarFile.close()
    finally:
      os.chdir( oldpath )

    tared = False
    res = self.storageElement.getPfnForLfn( destFile )
    if res['OK']:
      pfn = res["Value"]
      fileDict = {pfn:tarname}
      gLogger.info( "putFile", fileDict )
      res = self.storageElement.putFile( fileDict )
      if res['OK']:
        if not res['Value']['Failed']:
          subprodpath = os.path.join( path, prod, sub )
          gLogger.info( "rmTree", subprodpath )
          shutil.rmtree( subprodpath )
          tared = True
        else:
          gLogger.error( "putFile", res['Value']['Failed'] )
      else:
        gLogger.error( "putFile", res['Message'] )
    else:
      print res

    gLogger.info( "remove ", tarname )
    os.remove( tarname )
    if tared:
      return S_OK()
    else:
      return S_ERROR()

  def _noStorageFile( self, path ):

    com = 'rfstat /castor/cern.ch/grid/%s' % path
    res = shellCall( 5, com )
    if res['OK'] and res['Value'][0] == 1:
      return True
    else:
      return False
