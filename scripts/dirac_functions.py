########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/scripts/Attic/dirac_functions.py,v 1.10 2008/03/26 15:46:58 rgracian Exp $
# File :   dirac-functions.py
# Author : Ricardo Graciani
########################################################################
__RCSID__   = "$Id: dirac_functions.py,v 1.10 2008/03/26 15:46:58 rgracian Exp $"
__VERSION__ = "$Revision: 1.10 $"
"""
    Some common functions used in dirac-distribution, dirac-update
"""
try:
  import getopt, sys, os, signal, shutil 
  import urllib, popen2, tempfile, tarfile, fileinput, re, datetime
except Exception, x:
  import sys
  print 'ERROR python interpreter does not support necessary modules'
  print 'ERROR', x
  print
  sys.exit(-1)

availablePlatforms = ['slc4_amd64_gcc34', 
                      'slc4_ia32_gcc34' ]

rootPath    = None
shortName   = None
scriptsPath = None

debugFlag   = False

defaultCVS      = ':pserver:anonymous@isscvs.cern.ch:/local/reps/dirac'
defaultURL      = 'http://cern.ch/lhcbproject/dist/DIRAC3'

defaultVersion  = 'HEAD'
externalVersion = defaultVersion
defaultPython   = 'python25'
python = {'python24':'external/Python-2.4.4',
          'python25':'external/Python-2.5.2' }

diracTimeout       = 120
externalTimeout    = 300

fromTar = False
fromCVS = True

buildFlag  = False
serverFlag = False

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
                            'external/Python-2.5.2'],
               'directories': ['contrib',
                               'external']
             },
             { 'name' : 'DIRAC-external',
               'packages': ['external/matplotlib',
                            'external/mod_python',
                            'external/MySQL',
                            'external/MySQL-python',
                            'external/Pylons',
                            'external/runit'],
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
                            'external/matplotlib',
                            'external/runit']
             },
           ]

srcNo = 4
binNo = 2


def debug():
  """
   Set debug Flag
  """
  global debugFlag
  debugFlag = True
  return debugFlag

def build():
  """
   Require local build
  """
  global buildFlag
  buildFlag = True

def tarFlag():
  """
   Require download from tar
  """
  global fromTar, fromCVS
  fromTar = True
  fromCVS = False

def cvsFlag():
  """
   Require download from CVS
  """
  global fromTar, fromCVS
  fromTar = False
  fromCVS = True

def requireServer():
  """
   Require full external
  """
  global serverFlag
  serverFlag = True

def requireClient():
  """
   Require full external
  """
  global serverFlag
  serverFlag = False

def logInit( fullName ):
  """
   From the location of the script define:
     shortName
     scriptsPath
     rootPath
  """
  global shortName, scriptsPath, rootPath
  shortName = os.path.basename( fullName )
  scriptsPath = os.path.realpath( os.path.dirname( fullName ) )
  rootPath    = os.path.dirname( scriptsPath )
  os.chdir( rootPath )
  return rootPath

def log( level, msg ):
  """
   Print log entries similar to DIRAC Logger
  """
  global shortName, debugFlag
  logTime = str( datetime.datetime.utcnow( ) ).split('.')[0] + " UTC"
  if level != 'DEBUG ' or debugFlag:
    for line in msg.split( '\n' ):
      print logTime, shortName, level, line

def logEXCEP( msg ):
  log( 'EXCEPT', str(msg) )
  sys.exit( -1 )

def logERROR( msg ):
  log( 'ERROR ', msg )

def logINFO( msg ):
  log( 'INFO  ', msg )

def logDEBUG( msg ):
  log( 'DEBUG ', msg )

def logHelp( help ):
  print
  print 'Usage: %s [options]' % shortName
  print help
  sys.exit(-1)

def tmpDir():
  """
   Create a TMP Directory to to prepare the distribution
  """
  global rootPath
  rootPath = tempfile.mkdtemp( 'tmp', 'DIRAC3')
  os.chdir( rootPath )
  return rootPath

def get_platform():
  """
   Use dirac-architecture script to retrieve the local platform
  """
  global localPlatform, reqPlatform
  dirac_architecture = os.path.join( scriptsPath, 'dirac-architecture' )
  (child_stdout, child_stdin) = popen2.popen2( dirac_architecture )
  localPlatform = child_stdout.read().strip()
  reqPlatform   = localPlatform
  child_stdout.close()
  if localPlatform == 'ERROR':
    logERROR( 'Can not determine local platform' )
    sys.exit(-1)
  return localPlatform

def set_platform( platform ):
  """
   Set required platform for installation
  """
  global reqPlatform
  reqPlatform = platform
  return reqPlatform

def set_python( ver ):
  global defaultPython
  defaultPython = 'python%s' % ver
  if not python.has_key( defaultPython ):
    logERROR( 'Python version "%s" not available' % defaultPython )
    sys.exit(-1)

def default_URL( url  ):
  global defaultURL
  defaultURL = url

def default_CVS( cvs  ):
  global defaultCVS
  defaultCVS = cvs

def version( ver ):
  """
   Set DIRAC version to install
  """
  global defaultVersion, externalVersion
  defaultVersion  = ver
  externalVersion = ver

def external( ver ):
  """
   Set external version, different from DIRAC version
  """
  global externalVersion
  externalVersion = ver

def dirac_magic( magic ):
  """
   Replace first magic line of all python scripts in scripts
  """
  os.chdir( os.path.join( rootPath, 'scripts' ) )
  files = os.listdir( '.' )
  for file in files:
    if file.find('CVS') != -1:
      files.remove(file)
  output = []
  input = fileinput.FileInput( files, inplace = 1 )
  for line in input:
    if input.isfirstline():
      if re.compile( '^#!.*python').match(line):
        output.append( input.filename() )
        print magic
      else:
        print line
    else:
      print line
  input.close()
  if output:
    logDEBUG( 'Magic first line updated in files:' )
    logDEBUG( '  %s' % str.join(', ', output ) )
  os.chdir( rootPath )


def create_src_tars():
  """
   Create distribution tars for source code 
  """
  tars = 0
  n = srcNo
  if not serverFlag:
    n -= 1
  if not fromTar:
    for i in range(n):
      tar = src_tars[i]
      name = tar['name']
      version = defaultVersion
      if name.find('external') != -1 :
        version = externalVersion
      get_cvs( version, tar['packages']  )
      tarName = '%s-%s' % ( name, version )
      os.chdir( os.path.join( rootPath, 'DIRAC3' ) )
      if name == 'DIRAC':
        dirac_make( 'DIRAC' )
      create_tar( tarName, tar['directories'] )
      os.chdir( rootPath )
      tars += 1
    os.chdir( os.path.join( rootPath, 'DIRAC3' ) )
  else:
    logINFO( 'Donwloading from tar, do not create src tars.' )
    for i in range(n):
      tar = src_tars[i]
      name = tar['name']
      if i == 0 or name.find('external') != -1 :
        tarName = '%s-%s' % ( name, externalVersion )
        get_tar( tarName, externalTimeout )
  return tars

def create_bin_tars():
  """
   Create distribution tars with compiled components
  """
  global buildFlag
  tars = 0
  if not buildFlag:
    return tars
  n = binNo
  if not serverFlag:
    n -= 1  
  for i in range(n):
    tar = bin_tars[i]
    name = tar['name']
    for j in range(len(tar['packages'])):
      dir = tar['packages'][j]
      dirac_make( dir )
      if i == 0 and j == 0:
        dirac_make( python[defaultPython] )
        # need to check zlib module
    tarName = '%s-%s-%s-%s' % ( name, externalVersion, localPlatform, defaultPython )
    create_tar( tarName, [localPlatform] )
    tars += 1
  return tars

def dirac_make( dir ):
  """
   Build packages using dirac-make script
  """
  logINFO( ' Building "%s"' % dir )
  diracMake = os.path.join( dir, 'dirac-make' )
  makeCmd = '%s 1>> %s.log 2>> %s.log' % ( diracMake, diracMake, diracMake )
  logDEBUG( makeCmd )
  ret = os.system ( makeCmd )
  if ret != 0:
    logERROR( 'Failed making %s' %dir )
    logERROR( 'Check log file at "%s.log"' % diracMake )
    sys.exit( -1 )

def get_cvs( tag, packages ):
  """
   Check access to CVS repository (retrieve scripts)
  """
  
  myPackages = '"DIRAC3/%s"' % str.join('" "DIRAC3/', packages )
  logINFO( 'Retrieving %s' % myPackages )

  cvsCmd = 'cvs -Q -d %s export -f -r %s %s' % ( defaultCVS, tag, myPackages )
  if debugFlag:
    cvsCmd = 'cvs -q -d %s export -f -r %s %s' % ( defaultCVS, tag, myPackages )
  logDEBUG( cvsCmd )
  
  ret = os.system( cvsCmd )
  if ret != 0:
    logEXCEP( 'Check your cvs installation' )

def create_tar( name, dirs):
  """
   Create distribution tar from given directories
  """
  tarFileName = os.path.join( rootPath, '%s.tar.gz' % name )
  logINFO( '  Creating tar file %s'  % os.path.basename( tarFileName ) )
  logDEBUG( '   including directories: %s' % str.join( ', ', dirs ) )
  try:
    tarFile = tarfile.open( tarFileName, 'w:gz' )
    for dir in dirs:
      tarFile.add( dir )
    tarFile.close()
  except Exception, x:
    logERROR( 'Failed to create tar' )
    logEXCEP( x )

def get_tar( name, timeout ):

  ( file, localName ) = tempfile.mkstemp()
  tarFileName = os.path.join( '%s.tar.gz' % name )
  remoteName = '%s/%s' % ( defaultURL, tarFileName )
  error = 'Retrieving file "%s"' % remoteName
  try:
    urlretrieveTimeout( remoteName, localName, timeout )
    error = 'Opening file "%s"' % localName
    tar = tarfile.open( localName , 'r' )
    try:
      error = 'Extracting file "%s"' % localName
      tar.extractall( rootPath )
      os.remove( localName )
    except:
      for member in tar.getmembers():
        tar.extract( member, rootPath )
      os.remove( localName )

  except Exception, x:
    try:
      os.remove( localName )
    except:
      pass
    logERROR( error )
    logEXCEP( x )



  
def urlretrieveTimeout( fname, lname, timeout ):
  """
   Retrive remore url to local file, with timeout wrapper
  """
  # NOTE: Not thread-safe, since all threads will catch same alarm.
  #       This is OK for dirac-install, since there are no threads.
  logDEBUG( 'Retrieving remote file "%s"' % fname )

  signal.signal(signal.SIGALRM, alarmHandler)
  # set timeout alarm
  signal.alarm(timeout)
  try:
    localname,headers = urllib.urlretrieve( fname, lname )
  except Exception, x:
    if x == 'TimeOut':
      logERROR( 'Timeout after %s seconds on transfer request for "%s"' % \
      (str(timeout), fname) )
    raise x

  # clear timeout alarm
  signal.alarm(0)
  
def alarmHandler(*args):
  """
   signal handler for SIGALRM, just raise an exception
  """
  raise Exception( 'TimeOut' )



def check_interpreter( python ):
  """
   Check if DIRAC version of python interpreter is installed and make sure 
   all scripts will make use of it
  """
  global localPython
  localPython = python
  python = sys.executable
  if python ==  localPython:
    logDEBUG( 'Using python interpreter "%s"' % python )
    return True
  else:
    return False

def install_external( localPlatform ):
  """
   Install external package for the requiered platform
  """
  # remove requested platform directory if it exists
  externalDir = os.path.join( rootPath, localPlatform )
  if os.path.isdir( externalDir ):
    try:
      shutil.rmtree( externalDir )
    except Exception, x:
      logERROR( 'Can not removed existing DIRAC-external distribution' )
      logEXCEP( x )

  if buildFlag:
    build_external( )
  else:
    if not localPlatform in availablePlatforms:
      logERROR( 'Platform "%s" not available, use --build flag' % localPlatform )
      sys.exit(-1)
    else:
      if serverFlag:
        name = 'DIRAC-external-%s-%s-%s.tar.gz' % ( externalVersion, localPlatform, defaultPython )
      else:
        name = 'DIRAC-external-client-%s-%s-%s.tar.gz' % ( externalVersion, localPlatform, defaultPython )
      install_tar ( name, externalTimeout )

  os.environ['PATH'] = '%s:%s' % ( os.path.join( externalDir, 'bin' ), os.environ['PATH'] )

def build_external():
  """
   Build external packages for local platform
  """
  n = srcNo
  if not serverFlag:
    n -= 1
  if not fromTar:
    for i in range(n):
      tar = src_tars[i]
      name = tar['name']
      version = defaultVersion
      if name.find('external') != -1 :
        version = externalVersion
        get_cvs( version, tar['packages']  )
    # Move directories down from DIRAC3
    dirs = os.listdir( 'DIRAC3' )
    for dir in dirs:
      os.rename( os.path.join( 'DIRAC3', dir ), dir )
  else:
    logINFO( 'Donwloading src tar' )
    for i in range(n):
      tar = src_tars[i]
      name = tar['name']
      if i == 0 or name.find('external') != -1 :
        tarName = '%s-%s' % ( name, externalVersion )
        get_tar( tarName, externalTimeout )

  n = binNo
  if not serverFlag:
    n -= 1
  for i in range(n):
    tar = bin_tars[i]
    name = tar['name']
    for j in range(len(tar['packages'])):
      dir = tar['packages'][j]
      dirac_make( dir )
      if i == 0 and j == 0:
        dirac_make( python[defaultPython] )
    
  pass

def install_tar( name, timeout ):

  ( file, localName ) = tempfile.mkstemp()
  remoteName = '%s/%s' % ( defaultURL, name )
  error = 'Retrieving file "%s"' % remoteName
  try:
    urlretrieveTimeout( remoteName, localName, timeout )
    error = 'Opening file "%s"' % localName
    tar = tarfile.open( localName , 'r' )
    try:
      error = 'Extracting file "%s"' % localName
      tar.extractall( rootPath )
      os.remove( localName )
    except:
      for member in tar.getmembers():
        tar.extract( member, rootPath )
      os.remove( localName )

  except Exception, x:
    try:
      os.remove( localName )
    except:
      pass
    logERROR( error )
    logEXCEP( x )

def check_dirac( defaultVersion ):
  """
   Compare required DIRAC version with installed one and update if necesary
  """
  global localPython
  localVersion = check_dirac_version()
  if defaultVersion == localVersion:
    logDEBUG( 'DIRAC version "%s" already installed' % localVersion )
  else:
    name = 'DIRAC-%s.tar.gz' % defaultVersion
    install_tar( name, diracTimeout )
    dirac_make = os.path.join( rootPath, 'DIRAC', 'dirac-make')
    ( child_stdout, child_stdin, child_stderr) = popen2.popen3( dirac_make )
    while child_stdout.read():
      pass
    for line in child_stderr.readlines():
      logERROR( line )
    child_stdout.close()
    child_stderr.close()
    dirac_version = os.path.join( scriptsPath, 'dirac-version' )
    ( child_stdout, child_stdin, child_stderr) = popen2.popen3( '%s %s' % ( localPython, dirac_version ) )
    localVersion = child_stdout.readline().strip()
    logDEBUG( 'DIRAC version "%s" installed' % localVersion )
    child_stdout.close()
    child_stderr.close()
    
  return localVersion

def check_dirac_version():
  """
   Check local DIRAC instalation a get version
  """
  localVersion  = None
  check_diraccfg()
  try:
    from DIRACEnvironment import DIRAC
    localVersion = DIRAC.version
    logDEBUG( 'Currently installed DIRAC version is %s' % localVersion )
  except:
      logERROR( 'No working version of DIRAC installed' )
      pass

  return localVersion

def check_diraccfg():
  """
   Make sure that dirac.cfg file exists in the default location, even if empty
  """
  etcPath = os.path.join( rootPath, 'etc')
  cfgPath = os.path.join( etcPath, 'dirac.cfg' )
  if not os.path.exists( etcPath ):
    try:
      os.mkdir( etcPath )
    except Exception, x :
      logERROR( 'Can not create "%s", check permissions' % etcPath )
      logEXCEP( x )
  
  if not os.path.exists( cfgPath ):
    try:
      file = open( cfgPath, 'w' )
    except Exception, x :
      logERROR( 'Can not create "%s", check permissions' % cfgPath)
      logEXCEP( x )
  elif not os.access( cfgPath, os.R_OK ):
    try:
      file =  open( cfgPath, 'r' )
    except Exception, x :
      logERROR( 'Can not read "%s", check permissions' % cfgPath )
      logEXCEP( x )


