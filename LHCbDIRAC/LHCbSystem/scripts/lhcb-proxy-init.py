#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/scripts/lhcb-proxy-init.py,v 1.9 2008/08/03 11:33:09 acasajus Exp $
# File :   dirac-proxy-init.py
# Author : Adrian Casajus
########################################################################
__RCSID__   = "$Id: lhcb-proxy-init.py,v 1.9 2008/08/03 11:33:09 acasajus Exp $"
__VERSION__ = "$Revision: 1.9 $"

import sys
import os
import getpass
import imp
from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script

#Hack to load dirac-proxy-init because python grammar does not allow - in the module names
fd = file( "%s/DIRAC/FrameworkSystem/scripts/dirac-proxy-init.py" % DIRAC.rootPath, "r")
mod = imp.load_module( "dirac-proxy-init", fd, "", ( "", 'r', imp.PY_SOURCE ) )
CLIParams = getattr( mod, "CLIParams" )
generateProxy = getattr( mod, "generateProxy" )
fd = file( "%s/DIRAC/FrameworkSystem/scripts/dirac-proxy-upload.py" % DIRAC.rootPath, "r")
mod = imp.load_module( "dirac-proxy-upload", fd, "", ( "", 'r', imp.PY_SOURCE ) )
uploadProxy = getattr( mod, "uploadProxy" )

cliParams = CLIParams()
cliParams.registerCLISwitches()

Script.disableCS()
Script.parseCommandLine()

diracGroup = cliParams.getDIRACGroup()
time = cliParams.getProxyLifeTime()

retVal = generateProxy( cliParams )
if not retVal[ 'OK' ]:
  print "Can't create a proxy: %s" % retVal[ 'Message' ]
  sys.exit(1)

from DIRAC import gConfig
from DIRAC.Core.Security import CS, Properties
from DIRAC.Core.Security.Misc import getProxyInfo
from DIRAC.Core.Security.MyProxy import MyProxy
from DIRAC.Core.Security.VOMS import VOMS

def uploadProxyToMyProxy( params, DNAsUsername ):
  myProxy = MyProxy()
  if DNAsUsername:
    params.debugMsg( "Uploading pilot proxy with group %s to %s..." % ( params.getDIRACGroup(), myProxy.getMyProxyServer() ) )
  else:
    params.debugMsg(  "Uploading user proxy with group %s to %s..." % ( params.getDIRACGroup(), myProxy.getMyProxyServer() ) )
  retVal =  myProxy.getInfo( proxyInfo[ 'path' ], useDNAsUserName = DNAsUsername )
  if retVal[ 'OK' ]:
    remainingSecs = ( int( params.getProxyRemainingSecs() / 3600 ) * 3600 ) - 7200
    myProxyInfo = retVal[ 'Value' ]
    if 'timeLeft' in myProxyInfo and remainingSecs < myProxyInfo[ 'timeLeft' ]:
      params.debugMsg(  " Already uploaded" )
      return True
  retVal = generateProxy( params )
  if not retVal[ 'OK' ]:
    print " There was a problem generating proxy to be uploaded to myproxy: %s" % retVal[ 'Message' ]
    return False
  retVal = getProxyInfo( retVal[ 'Value' ] )
  if not retVal[ 'OK' ]:
    print " There was a problem generating proxy to be uploaded to myproxy: %s" % retVal[ 'Message' ]
    return False
  generatedProxyInfo = retVal[ 'Value' ]
  retVal = myProxy.uploadProxy( generatedProxyInfo[ 'path' ], useDNAsUserName = DNAsUsername )
  if not retVal[ 'OK' ]:
    print " Can't upload to myproxy: %s" % retVal[ 'Message' ]
    return False
  params.debugMsg( " Uploaded" )
  return True

def uploadProxyToDIRACProxyManager( params ):
  params.debugMsg(  "Uploading user pilot proxy with group %s..." % ( params.getDIRACGroup() ) )
  params.onTheFly = True
  retVal = uploadProxy( params )
  if not retVal[ 'OK' ]:
    print " There was a problem generating proxy to be uploaded proxy manager: %s" % retVal[ 'Message' ]
    return False
  return True

Script.enableCS()

retVal = getProxyInfo( retVal[ 'Value' ] )
if not retVal[ 'OK' ]:
  print "Can't create a proxy: %s" % retVal[ 'Message' ]
  sys.exit(1)

proxyInfo = retVal[ 'Value' ]
if 'username' not in proxyInfo:
  print "Not authorized in DIRAC"
  sys.exit(1)

retVal = CS.getGroupsForUser( proxyInfo[ 'username' ] )
if not retVal[ 'OK' ]:
  print "No groups defined for user %s" % proxyInfo[ 'username' ]
  sys.exit(1)
availableGroups = retVal[ 'Value' ]

pilotGroup = False
for group in availableGroups:
  groupProps = CS.getPropertiesForGroup( group )
  if Properties.PILOT in groupProps or Properties.GENERIC_PILOT in groupProps:
    pilotGroup = group
    break

if not pilotGroup:
  print "WARN: No pilot group defined for user %s" % proxyInfo[ 'username' ]
else:
  cliParams.setDIRACGroup( pilotGroup )
  issuerCert = proxyInfo[ 'chain' ].getIssuerCert()[ 'Value' ]
  remainingSecs = issuerCert.getRemainingSecs()[ 'Value' ]
  cliParams.setProxyRemainingSecs( remainingSecs - 300 )
  #uploadProxyToMyProxy( cliParams, True )
  uploadProxyToDIRACProxyManager( cliParams )

cliParams.setDIRACGroup( proxyInfo[ 'group' ] )
#uploadProxyToMyProxy( cliParams, False )
uploadProxyToDIRACProxyManager( cliParams )

finalChain = proxyInfo[ 'chain' ]

vomsMapping = CS.getVOMSAttributeForGroup( proxyInfo[ 'group' ] )
if vomsMapping:
  voms = VOMS()
  attr = vomsMapping[0]
  retVal = voms.setVOMSAttributes( finalChain, attr )
  if not retVal[ 'OK' ]:
    #print "Cannot add voms attribute %s to proxy %s: %s" % ( attr, proxyInfo[ 'path' ], retVal[ 'Message' ] )
    print "Cannot add voms attribute %s to proxy" % ( attr )
  else:
    finalChain = retVal[ 'Value' ]

retVal = finalChain.dumpAllToFile( proxyInfo[ 'path' ] )
if not retVal[ 'OK' ]:
  print "Cannot write proxy to file %s" % proxyInfo[ 'path' ]
  sys.exit(1)
cliParams.debugMsg(  "done" )
sys.exit(0)








