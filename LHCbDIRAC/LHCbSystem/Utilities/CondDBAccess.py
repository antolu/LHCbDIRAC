########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Utilities/CondDBAccess.py,v 1.1 2009/06/15 12:18:03 paterson Exp $
# Author: Stuart Paterson
########################################################################

"""  The CondDB access module allows.
"""

__RCSID__ = "$Id: CondDBAccess.py,v 1.1 2009/06/15 12:18:03 paterson Exp $"

import string,os,shutil

import DIRAC

from DIRAC import gConfig, gLogger, S_OK, S_ERROR

#############################################################################
def getCondDBFiles():
  """ Function to set up the necessary CORAL XML files to bypass LFC access.
      Any project environment will pick up the latest AppConfig.
      Relies on the following APPCONFIG conventions:

      $APPCONFIGROOT/conddb/dblookup-<SITE>.xml
      $APPCONFIGROOT/conddb/authentication.xml
      $APPCONFIGOPTS/UseOracle.py
      $APPCONFIGOPTS/DisableLFC.py - Trigger in GaudiApplication for getting these files
  """
  gLogger.getSubLogger( "CondDBAccess" )
  ambiguousString = ['p', 'a', 's', 's', 'w', 'o', 'r', 'd', 'c', 'r', 'a', 'z', 'i', 'n', 'e', 's', 's']
  ambiguous = string.join(ambiguousString).replace(' ','').replace('craziness','')
  otherString = ['c', 'o', 'n', 'd', 'd', 'b', 'u', 's', 'e', 'r']
  other = string.join(otherString).replace(' ','').replace('conddb','')

  localSite = DIRAC.gConfig.getValue('/LocalSite/Site','LCG.CERN.ch')
  condDBSite = localSite.split('.')[1]
  gLogger.verbose('Running at site: %s, CondDB site is: %s' %(localSite,condDBSite))

  if os.environ.has_key('VO_LHCB_SW_DIR'):
    sharedArea = os.path.join(os.environ['VO_LHCB_SW_DIR'],'lib')
    gLogger.verbose( 'Using VO_LHCB_SW_DIR at "%s"' % sharedArea )
  elif DIRAC.gConfig.getValue('/LocalSite/SharedArea',''):
    sharedArea = DIRAC.gConfig.getValue('/LocalSite/SharedArea')
    gLogger.verbose( 'Using SharedArea at "%s"' % sharedArea )
  lbLogin = '%s/LbLogin' %sharedArea
  ret = DIRAC.Source( 60,[lbLogin], dict(os.environ))
  if not ret['OK']:
    gLogger.error('Error during lbLogin\n%s' %ret)
    return ret

  setupProject = ['%s/%s' %(os.path.dirname(os.path.realpath('%s.sh' %lbLogin)),'SetupProject')]
  setupProject.append( '--ignore-missing' )
  setupProject.append( 'Brunel' )

  ret = DIRAC.Source( 60, setupProject, ret['outputEnv'] )
  if not ret['OK']:
    gLogger.warn('Error during SetupProject\n%s' %ret)
    return ret

  appEnv = ret['outputEnv']
  if not appEnv.has_key('APPCONFIGROOT'):
    gLogger.error('APPCONFIGROOT is not defined in LbLogin / SetupProject environment')
    return S_ERROR('APPCONFIGROOT undefined')

  appConfigRoot = appEnv['APPCONFIGROOT']
  lookupFile = '%s/conddb/dblookup-%s.xml' %(appConfigRoot,condDBSite)
  defaultLookup = '%s/conddb/dblookup-CERN.xml' %(appConfigRoot)
  if not os.path.exists(lookupFile):
    gLogger.error('Could not find %s, reverting to CERN file' %lookupFile)
    lookupFile = defaultLookup
    if not os.path.exists(defaultLookup):
      gLogger.error('Could not find %s' %defaultLookup)
      return S_ERROR('Missing %s' %defaultLookup)

  #copy loal so as not to read from the shared area
  localLookup = '%s/dblookup.xml' %(os.getcwd())
  shutil.copy(lookupFile,localLookup)
  authFile = '%s/conddb/authentication.xml' %(appConfigRoot)
  localAuth = '%s/authentication.xml' %(os.getcwd())
  shutil.copy(authFile,localAuth)

  fopen = open(localAuth,'r')
  authString = fopen.read()
  fopen.close()

  fopen = open(localAuth,'w')
  fopen.write(authString.replace('attribute',ambiguous).replace('aid',other))
  fopen.close()

  return S_OK([localLookup,localAuth])

#############################################################################
def log( n, line ):
  gLogger.verbose( line )

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#