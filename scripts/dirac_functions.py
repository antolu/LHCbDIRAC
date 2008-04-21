########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/scripts/Attic/dirac_functions.py,v 1.41 2008/04/21 07:44:11 rgracian Exp $
# File :   dirac-functions.py
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id: dirac_functions.py,v 1.41 2008/04/21 07:44:11 rgracian Exp $"
__VERSION__ = "$Revision: 1.41 $"
"""
    Some common functions used in dirac-distribution, dirac-update
"""
try:
  import getopt, sys, os, signal, shutil 
  import urllib, popen2, tempfile, fileinput, re, time
except Exception, x:
  import sys
  print 'ERROR python interpreter does not support necessary modules'
  print 'ERROR', x
  print
  sys.exit(-1)

python = {'python24':'external/Python-2.4.4',
          'python25':'external/Python-2.5.2' }

availablePlatforms =  [ 'slc4_amd64_gcc34',
                        'slc4_ia32_gcc34',
                        'slc3_ia32_gcc323',
                        'slc3_ia32_gcc296' ]


diracTimeout       = 120
externalTimeout    = 300

src_tars = [ { 'name': 'DIRAC-scripts',
               'packages': ['scripts'],
               'directories': ['scripts']
             },
             { 'name': 'DIRAC',
               'packages': ['DIRAC',
                            'WorkflowLib'],
               'directories': ['scripts',
                               'DIRAC',
                               'WorkflowLib']
             },
             { 'name': 'DIRAC-external-client',
               'packages': ['contrib/pyGSI',
                            'external/openssl-0.9.7m',
                            'external/sqlite-3.5.4',
                            'external/Python-2.4.4',
                            'external/Python-2.5.2',
                            'external/runit'],
               'directories': ['contrib',
                               'external']
             },
             { 'name' : 'DIRAC-external',
               'packages': ['external/matplotlib',
                            'external/mod_python',
                            'external/MySQL',
                            'external/MySQL-python',
                            'external/rrdtool',
                            'external/Pylons'],
               'directories': ['contrib',
                               'external']
             },
           ]
bin_tars = [ { 'name': 'DIRAC-external-client',
               'packages': ['external/sqlite-3.5.4',
                            'contrib/pyGSI']
             },
             { 'name' : 'DIRAC-external',
               'packages': ['external/MySQL',
                            'external/MySQL-python',
                            'external/Pylons',
                            'external/rrdtool',
                            'external/matplotlib',
                            'external/runit']
             },
           ]

srcNo = len(src_tars)
binNo = len(bin_tars)


class functions:
  def __init__( self, fullName  ):
    """
     Initialize the functions class using the given script path to set the 
     rootPath of the DIRAC intallation.
    """
    self.setRoot( fullName )
    
    self.debugFlag   = False
    self.buildFlag  = False
    self.CVS         = ':pserver:anonymous@isscvs.cern.ch:/local/reps/dirac'
    self.URL         = 'http://cern.ch/lhcbproject/dist/DIRAC3'
    self.setVersion( 'HEAD' )
    self.setPython( '25' )

    self.cvsFlag()
    self.requireClient()
    
    self.platform()

  def __rmDir(self, dir):
    """
     Remove dir if it exits
    """
    if os.path.exists( dir ):
      try:
        shutil.rmtree( dir )
      except Exception, x:
        self.logERROR( 'Can not removed existing directory %s' 
                  % os.path.join( self.__rootPath, dir ) )
        self.logEXCEP( x )

  def __log( self, level, msg ):
    """
     Print log entries similar to DIRAC Logger
    """
    logTime = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime()) + " UTC"
    # logTime = str( datetime.datetime.utcnow( ) ).split('.')[0] + " UTC"
    if level != 'DEBUG ' or self.debugFlag:
      for line in msg.split( '\n' ):
        print logTime, self.shortName, level, line
        sys.stdout.flush()

  def logEXCEP( self, msg ):
    self.__log( 'EXCEPT', str(msg) )
    sys.exit( -1 )
  
  def logERROR( self, msg ):
    self.__log( 'ERROR ', msg )
  
  def logINFO( self, msg ):
    self.__log( 'INFO  ', msg )
  
  def logDEBUG( self, msg ):
    self.__log( 'DEBUG ', msg )
  
  def logHelp( self, help ):
    print
    print 'Usage: %s [options]' % self.shortName
    print help
    sys.exit(-1)

  def setRoot( self, fullName ):
    if fullName[0] != '/':
      self.logERROR( 'Install path must be absolute.' )
      sys.exit(-1)
    self.shortName   = os.path.basename( fullName )
    self.scriptsPath = os.path.dirname( fullName )
    self.__rootPath    = os.path.dirname( self.scriptsPath )
    os.chdir( self.__rootPath )

  def root(self):
    return self.__rootPath

  def debug(self):
    """
     Set debug Flag
    """
    self.debugFlag = True
    return self.debugFlag

  def build(self):
    """
     Require local build
    """
    self.buildFlag = True

  def tarFlag(self):
    """
     Require download from tar
    """
    self.fromTar = True
    self.fromCVS = False

  def cvsFlag(self):
    """
     Require download from CVS
    """
    self.fromTar = False
    self.fromCVS = True

  def requireServer(self):
    """
     Require full external
    """
    self.serverFlag = True

  def requireClient(self):
    """
     Require full external
    """
    self.serverFlag = False

  def tmpDir(self):
    """
     Create a TMP Directory to to prepare the distribution
    """
    try:
      self.__rootPath = tempfile.mkdtemp( 'tmp', 'DIRAC3')
    except:
      self.__rootPath = tempfile.mktemp()
      os.mkdir( self.__rootPath )

    os.chdir( self.__rootPath )
    self.scriptsPath = os.path.join( self.__rootPath, 'scripts' )
    return self.__rootPath

  def platform( self, platform=None ):
    """
     Use dirac-architecture script to retrieve the local platform
    """
    if not platform:
      dirac_architecture = os.path.join( self.scriptsPath, 'dirac-architecture' )
      (child_stdout, child_stdin) = popen2.popen2( dirac_architecture )
      self.localPlatform = child_stdout.read().strip()
      child_stdout.close()
    else:
      self.localPlatform = platform

    if self.localPlatform == 'ERROR':
      self.logERROR( 'Can not determine local platform' )
      sys.exit(-1)


  def setPython( self, ver ):
    self.python = 'python%s' % ver
    if not python.has_key( self.python ):
      self.logERROR( 'Python version "%s" not available' % self.python )
      sys.exit(-1)

  def setURL( self, url ):
    self.URL = url

  def setCVS( self, cvs  ):
    self.CVS = cvs

  def setVersion( self, ver ):
    """
     Set DIRAC version to install
    """
    self.version  = ver
    self.external = ver

  def setExternal( self, ver ):
    """
     Set external version, different from DIRAC version
    """
    self.external = ver

  def createSrcTars(self):
    """
     Create distribution tars for source code 
    """
    tars = 0
    n = srcNo
    if not self.serverFlag:
      # prepare tars only for a client distribution
      n -= 1
    if self.fromCVS:
      for i in range(n):
        tar = src_tars[i]
        name = tar['name']
        version = self.version
        if name.find('external') != -1 :
          version = self.external
        self._getCVS( version, tar['packages']  )
        tarName = '%s-%s' % ( name, version )
        if name == 'DIRAC':
          self._diracMake( 'DIRAC' )
        self._createTar( tarName, tar['directories'] )
        tars += 1
    else:
      self.logINFO( 'Donwloading from tar, do not create src tars.' )
      if self.buildFlag:
        # if build is required, download external tars
        for i in range(n):
          name = src_tars[i]['name']
          if i == 0 or name.find('external') != -1 :
            tarName = '%s-%s' % ( name, self.external )
            self._getTar( tarName, externalTimeout )
    return tars

  def createBinTars(self):
    """
     Create distribution tars with compiled components
    """
    tars = 0
    if not self.buildFlag:
      return tars
    n = binNo
    if not self.serverFlag:
      # prepare tars only for a client distribution
      n -= 1  
    for i in range(n):
      tar = bin_tars[i]
      name = tar['name']
      for j in range(len(tar['packages'])):
        dir = tar['packages'][j]
        self._diracMake( dir )
        if i == 0 and j == 0:
          self._diracMake( python[self.python] )
          # need to check zlib module
      tarName = '%s-%s-%s-%s' % ( name, self.external, self.localPlatform, self.python )
      self._createTar( tarName, [self.localPlatform] )
      tars += 1
    return tars
  
  def checkInterpreter( self ):
    """
     Check if DIRAC version of python interpreter is installed and make sure 
     all scripts will make use of it
    """
    self.localPython = os.path.join( self.__rootPath, 
                                self.localPlatform, 
                                'bin', 'python' )
    python = sys.executable
    if python ==  self.localPython:
      self.logDEBUG( 'Using python interpreter "%s"' % python )
      return True
    else:
      return False

  def installExternal( self ):
    """
     Install external package for the requiered platform
    """
    # remove requested platform directory if it exists
    externalDir = os.path.join( self.__rootPath, self.localPlatform )
    self.__rmDir( externalDir )

    if self.buildFlag:
      self.buildExternal( )
    else:
      if not self.localPlatform in availablePlatforms:
        self.logERROR( 'Platform "%s" not available, use --build flag' % 
                  self.localPlatform )
        sys.exit(1)
      if self.serverFlag:
        name = 'DIRAC-external-%s-%s-%s' % \
        ( self.external, self.localPlatform, self.python )
      else:
        name = 'DIRAC-external-client-%s-%s-%s' % \
        ( self.external, self.localPlatform, self.python )
      self._getTar( name, externalTimeout )

  def diracMagic( self, magic ):
    """
     Replace first magic line of all python scripts in scripts
    """
    os.chdir( self.scriptsPath )
    files = os.listdir( '.' )
    for file in files:
      # exclude CVS directory if present
      if file.find('CVS') != -1:
        files.remove(file)
    output = []
    input = fileinput.FileInput( files, inplace = 1 )
    for line in input:
      if input.isfirstline():
        if re.compile( '^#!.*python').match(line):
          output.append( input.filename() )
          if len( magic ) < 100:
            print magic
          else:
            # to avoid a long magic line, make it a paragraph
            python = magic[magic.find('/'):]
            print '#! /usr/bin/env python'
            print 'import sys, os'
            print 'if sys.executable != "%s":' % python
            print '  sys.exit( os.system( "%%s %%s" %% ( "%s", str.join(" ",sys.argv ) ) ) )' % python
        else:
          print line,
      else:
        print line,
    input.close()
    if output:
      self.logDEBUG( 'Magic first line updated in files:' )
      self.logDEBUG( '  %s' % str.join(', ', output ) )
    os.chdir( self.__rootPath )

  def checkDirac(self):
    """
     Compare required DIRAC version with installed one and update if necesary
    """
    localVersion = self.checkDiracVersion()
    if self.version == localVersion:
      self.logDEBUG( 'DIRAC version "%s" already installed' % self.version )
    else:
      if self.fromTar:
        name = 'DIRAC-%s' % self.version
        self._getTar( name, diracTimeout )
      else:
        name = 'DIRAC'
        self._getCVS( self.version, [name] )
      self._diracMake( 'DIRAC' )
      dirac_version = os.path.join( self.scriptsPath, 'dirac-version' )
      ( ch_out, ch_in, ch_err) = popen2.popen3( '%s %s' % ( 
        self.localPython, dirac_version ) )
      self.version = ch_out.readline().strip()
      if self.version.find('ERROR') != -1:
        self.logERROR( self.version )
        try:
          import zlib
        except Exception, x:
          self.logEXCEP( x )
        sys.exit(-1)
      self.logDEBUG( 'DIRAC version "%s" installed' % self.version )
      ch_out.close()
      ch_err.close()
      
    return self.version

  def _diracMake( self, dir ):
    """
     Build packages using dirac-make script, on the given directory
    """
    self.logINFO( ' Building "%s"' % dir )
    diracMake = os.path.join( dir, 'dirac-make' )
    makeCmd = '%s 1>> %s.log 2>> %s.log' % ( diracMake, diracMake, diracMake )
    self.logDEBUG( makeCmd )
    ret = os.system ( makeCmd )
    if ret != 0:
      self.logERROR( 'Failed making %s' %dir )
      self.logERROR( 'Check log file at "%s.log"' % diracMake )
      f = open( '%s.log' % diracMake )
      lines = f.readlines()
      while lines:
        self.logERROR( str.join('',lines) )
        lines = f.readlines()
      sys.exit( -1 )

  def _getCVS( self, version, packages ):
    """
     Check access to CVS repository (retrieve scripts)
    """
    for destDir in packages:
      # remove existing directories, if any
      cvsDir = 'DIRAC3/%s' % destDir
      self.__rmDir( cvsDir )
      self.__rmDir( destDir )
      self.logINFO( 'Retrieving %s' % cvsDir )
      cvsCmd = 'cvs -Q -d %s export -f -r %s %s' \
               % ( self.CVS, version, cvsDir )
      if self.debugFlag:
        cvsCmd = 'cvs -q -d %s export -f -r %s %s' \
                 % ( self.CVS, version, cvsDir )
      self.logDEBUG( cvsCmd )  
      ret = os.system( cvsCmd )
      if ret != 0:
        self.logEXCEP( 'Check your cvs installation' )
      # Move the resulting dir
      try:
        os.renames( cvsDir, destDir )
      except Exception, x:
        self.logERROR( 'Failed to rename "%s" to "%s"' % (cvsDir, destDir ) )
        self.logEXCEP(x)        

  def _getTar( self, name, timeout ):
  
    try:
      ( file, localName ) = tempfile.mkstemp()
    except:
      localName = tempfile.mktemp()    
    tarFileName = os.path.join( '%s.tar.gz' % name )
    remoteName = '%s/%s' % ( self.URL, tarFileName )
    error = 'Retrieving file "%s"' % remoteName
    try:
      self.urlretrieveTimeout( remoteName, localName, timeout )
      error = 'Openning file "%s"' % localName
      import tarfile
      tar = tarfile.open( localName , 'r' )
      try:
        error = 'Extracting file "%s"' % localName
        tar.extractall( )
        os.remove( localName )
      except:
        for member in tar.getmembers():
          tar.extract( member )
        os.remove( localName )
  
    except Exception, x:
      try:
        error = 'Extracting file "%s"' % localName
        ret = os.system( 'tar xzf %s' % localName )
        if ret != 0:
          raise Exception( 'Fail to extract tarfile'  )        
        os.remove( localName )
        return
      except Exception, x:
        pass
      self.logERROR( error )
      self.logEXCEP( x )

  def _createTar( self, name, dirs):
    """
     Create distribution tar from given directory list
    """
    tarFileName = '%s.tar.gz' % name
    self.logINFO( '  Creating tar file %s'  % tarFileName )
    self.logDEBUG( '   including directories: %s' % str.join( ', ', dirs ) )
    try:
      import tarfile
      tarFile = tarfile.open( tarFileName, 'w:gz' )
      for dir in dirs:
        cleanDir( dir )
        tarFile.add( dir )
      tarFile.close()
    except Exception, x:
      ret = os.system( 'tar czf %s %s' % (tarFileName, str.join(' ',dirs) ) )
      if ret == 0: return
      self.logERROR( 'Failed to create tar' )
      self.logEXCEP( x )

  def urlretrieveTimeout( self, fname, lname, timeout ):
    """
     Retrive remore url to local file, with timeout wrapper
    """
    # NOTE: Not thread-safe, since all threads will catch same alarm.
    #       This is OK for dirac-install, since there are no threads.
    self.logDEBUG( 'Retrieving remote file "%s"' % fname )
  
    signal.signal(signal.SIGALRM, alarmHandler)
    # set timeout alarm
    signal.alarm(timeout)
    try:
      localname,headers = urllib.urlretrieve( fname, lname )
    except Exception, x:
      if x == 'TimeOut':
        self.logERROR( 'Timeout after %s seconds on transfer request for "%s"' % \
        (str(timeout), fname) )
      raise x
  
    # clear timeout alarm
    signal.alarm(0)

  def buildExternal( self ):
    """
     Build external packages for local platform
    """
    n = srcNo
    if not self.serverFlag:
      n -= 1
    if self.fromTar:
      self.logINFO( 'Donwloading src tar' )
      for i in range(n):
        tar = src_tars[i]
        name = tar['name']
        if i == 0 or name.find('external') != -1 :
          tarName = '%s-%s' % ( name, self.external )
          self._getTar( tarName, externalTimeout )
    else:
      self.logINFO( 'Donwloading src from CVS' )
      for i in range(n):
        tar = src_tars[i]
        name = tar['name']
        version = self.version
        if name.find('external') != -1 :
          version = self.external
          self._getCVS( version, tar['packages']  )
  
    n = binNo
    if not self.serverFlag:
      n -= 1
    for i in range(n):
      tar = bin_tars[i]
      name = tar['name']
      for j in range(len(tar['packages'])):
        dir = tar['packages'][j]
        self._diracMake( dir )
        if i == 0 and j == 0:
          self._diracMake( python[self.python] )


  def checkDiracVersion(self):
    """
     Check local DIRAC instalation a get version
    """
    localVersion  = None
    self.checkDiracCfg()
    try:
      from DIRACEnvironment import DIRAC
      localVersion = DIRAC.version
      self.logDEBUG( 'Currently installed DIRAC version is %s' % localVersion )
    except:
        self.logERROR( 'No working version of DIRAC installed' )
        pass
  
    return localVersion

  def checkDiracCfg(self):
    """
     Make sure that dirac.cfg file exists in the default location, even if empty
    """
    etcPath = os.path.join( self.__rootPath, 'etc')
    cfgPath = os.path.join( etcPath, 'dirac.cfg' )
    if not os.path.exists( etcPath ):
      try:
        os.mkdir( etcPath )
      except Exception, x :
        self.logERROR( 'Can not create "%s", check permissions' % etcPath )
        self.logEXCEP( x )
    
    if not os.path.exists( cfgPath ):
      try:
        file = open( cfgPath, 'w' )
      except Exception, x :
        self.logERROR( 'Can not create "%s", check permissions' % cfgPath)
        self.logEXCEP( x )
    elif not os.access( cfgPath, os.R_OK ):
      try:
        file =  open( cfgPath, 'r' )
      except Exception, x :
        self.logERROR( 'Can not read "%s", check permissions' % cfgPath )
        self.logEXCEP( x )

    return cfgPath

def alarmHandler(*args):
  """
   signal handler for SIGALRM, just raise an exception
  """
  raise Exception( 'TimeOut' )

def cleanDir( dir ):
  """
   Walk the given directory and remove all .pyc and .pyo files 
  """
  for d in os.walk( dir ):
    subDir = d[0]
    for f in d[2]:
      if f.endswith('.pyc'):
        os.remove( os.path.join(subDir,f) )
      elif f.endswith('.pyo'):
        os.remove( os.path.join(subDir,f) )
      