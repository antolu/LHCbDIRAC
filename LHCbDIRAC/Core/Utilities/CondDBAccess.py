########################################################################
# $HeadURL$
# Author: Stuart Paterson
########################################################################

"""  The CondDB access module allows to perform a nasty hack to disable
     the LFC lookup in CORAL used by Gaudi.
"""

__RCSID__ = "$Id$"

import string,os,shutil,re

import DIRAC

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities.Os import sourceEnv

gLogger = gLogger.getSubLogger( "CondDBAccess" )

#############################################################################
def getCondDBFiles(appConfigRoot,localSite='',directory=''):
  """ Function to set up the necessary CORAL XML files to bypass LFC access.
      Any project environment will pick up the latest AppConfig.
      Relies on the following APPCONFIG conventions:

      $APPCONFIGROOT/conddb/dblookup-<SITE>.xml
      $APPCONFIGROOT/conddb/authentication.xml
      $APPCONFIGOPTS/UseOracle.py
      $APPCONFIGOPTS/DisableLFC.py - Trigger in GaudiApplication for getting these files
  """
  if not localSite:
    localSite = DIRAC.gConfig.getValue('/LocalSite/Site','LCG.CERN.ch')
    
  if not directory:
    directory = os.getcwd()
    
  if not os.path.exists(appConfigRoot):
    return S_ERROR('APPCONFIGROOT ( %s ) does not exist!' %(appConfigRoot))    

  ambiguousString = ['p', 'a', 's', 's', 'w', 'o', 'r', 'd', 'c', 'r', 'a', 'z', 'i', 'n', 'e', 's', 's']
  ambiguous = string.join(ambiguousString).replace(' ','').replace('craziness','')
  otherString = ['c', 'o', 'n', 'd', 'd', 'b', 'u', 's', 'e', 'r']
  other = string.join(otherString).replace(' ','').replace('conddb','')
    
  condDBSite = localSite.split('.')[1]
  gLogger.verbose('Running at site: %s, CondDB site is: %s' %(localSite,condDBSite))
  lookupFile = '%s/conddb/dblookup-%s.xml' %(appConfigRoot,condDBSite)
  defaultLookup = '%s/conddb/dblookup-CERN.xml' %(appConfigRoot)
  if not os.path.exists(lookupFile):
    gLogger.error('Could not find %s, reverting to CERN file' %lookupFile)
    lookupFile = defaultLookup
    if not os.path.exists(defaultLookup):
      gLogger.error('Could not find %s' %defaultLookup)
      return S_ERROR('Missing %s' %defaultLookup)

  #copy local so as not to read from the shared area
  localLookup = '%s/dblookup.xml' %(directory)
  if not os.path.exists(localLookup):
    shutil.copy(lookupFile,localLookup)
  else:
    gLogger.debug('Local lookup file already present: %s' %localLookup)  

  authFile = '%s/conddb/authentication.xml' %(appConfigRoot)
  if not os.path.exists(authFile):
    gLogger.error('Could not find %s' %authFile)
    return S_ERROR('Missing %s' %(authFile))
  
  localAuth = '%s/authentication.xml' %(directory)
  if not os.path.exists(localAuth):
    shutil.copy(authFile,localAuth)
  else:
    gLogger.debug('Local authorization file already present: %s' %localAuth)
    return S_OK([localLookup,localAuth])
  
  fopen = open(localAuth,'r')
  authString = fopen.read()
  fopen.close()

  fopen = open(localAuth,'w')
  fopen.write(authString.replace('attribute',ambiguous).replace('aid',other))
  fopen.close()

  return S_OK([localLookup,localAuth])

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#