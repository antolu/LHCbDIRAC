#!/usr/bin/env python
########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/scripts/dirac-lhcb-remove-software.py,v 1.2 2009/07/17 14:02:08 joel Exp $
# File :   dirac-lhcb-remove-software
# Author : Stuart Paterson
########################################################################
__RCSID__   = "$Id: dirac-lhcb-remove-software.py,v 1.2 2009/07/17 14:02:08 joel Exp $"
__VERSION__ = "$Revision: 1.2 $"
try:
  from DIRAC.FrameworkSystem.Client.NotificationClient     import NotificationClient
except Exception,x:
  from DIRAC.WorkloadManagementSystem.Client.NotificationClient import NotificationClient

from DIRACEnvironment import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
from DIRAC                                                   import gConfig

import string

Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
diracAdmin = DiracAdmin()
modifiedCS = False
mailadress = 'lhcb-sam@cern.ch'

def usage():
  print 'Usage: %s <NAME> <VERSION>' %(Script.scriptName)
  DIRAC.exit(2)

def changeCS(path,val):
  val.sort()
  result = diracAdmin.csModifyValue(path,string.join(val,', '))
  print result
  if not result['OK']:
    print "Cannot modify value of %s" %path
    print result['Message']
    DIRAC.exit(255)

if len(args) < 2:
  usage()

softwareSection = '/Operations/SoftwareDistribution'
activeSection = '/Operations/SoftwareDistribution/Active'
osSection = '/Resources/Computing/OSCompatibility'
deprecatedSection = '/Operations/SoftwareDistribution/Deprecated'

subject = '%s %s remove to DIRAC CS' %(args[0],args[1])
msg = 'Grid removal required for %s %s' %(args[0],args[1])

packageNameVersion = '%s.%s' %(args[0],args[1])

#First check the Active list
activeList = gConfig.getValue(activeSection,[])
if not activeList:
  print 'ERROR: Could not get value for %s' %(activeSection)
  DIRAC.exit(255)

if packageNameVersion in activeList:
  print '==> %s will be removed from %s' %(packageNameVersion,activeSection)
  activeList.remove(packageNameVersion)
  changeCS(activeSection,activeList)
  modifiedCS=True
else:
  print '==> %s is not in %s' %(packageNameVersion,activeSection)

#Get the list of possible system configurations
systemConfigs = gConfig.getOptions(osSection)
if not systemConfigs['OK']:
  print 'ERROR: Could not get value for %s with message' %(osSection,result['Message'])
  DIRAC.exit(255)

#Prompt for system configurations to add the software for
for sc in systemConfigs['Value']:
  current = gConfig.getValue('%s/%s' %(softwareSection,sc),[])
  if packageNameVersion in current:
    result = diracAdmin._promptUser('Do you want to remove %s %s for system configuration %s?' %(args[0],args[1],sc))
    if result['OK']:
      current.remove(packageNameVersion)
      print 'Removing %s for system configuration %s' %(packageNameVersion,sc)
      changeCS('%s/%s' %(softwareSection,sc),current)
      modifiedCS = True
  else:
    print '==> %s is not present for system configuration %s' %(packageNameVersion,sc)

#Remove from the deprecated software list only if this version does not exist anywhere else
deprecatedList = gConfig.getValue(deprecatedSection,[])
if not deprecatedList:
  print 'ERROR: Could not get value for %s' %(deprecatedSection)
  DIRAC.exit(255)

if packageNameVersion in deprecatedList:
  print '==> %s is present in %s' %(packageNameVersion,deprecatedSection)
  result = diracAdmin._promptUser('Do you want to remove %s %s from the Deprecated software section?' %(args[0],args[1]))
  if result['OK']:
    deprecatedList.remove(packageNameVersion)
    changeCS(deprecatedSection,deprecatedList)
    modifiedCS = True
else:
  print '==> %s is not present in %s' %(packageNameVersion,deprecatedSection)
  result = diracAdmin._promptUser('Do you want to add %s %s to the Deprecated software section?' %(args[0],args[1]))
  if result['OK']:
    deprecatedList.append(packageNameVersion)
    changeCS(deprecatedSection,deprecatedList)
    modifiedCS = True

#Commit the changes if nothing has failed and the CS has been modified
if modifiedCS:
  result = diracAdmin.csCommitChanges(False)
  print result
  if not result[ 'OK' ]:
    print 'ERROR: Commit failed with message = %s' %(result[ 'Message' ])
    DIRAC.exit(255)
  else:
    print 'Successfully committed changes to CS'
    notifyClient = NotificationClient()
    print 'Sending mail for software installation %s' %(mailadress)
    res = notifyClient.sendMail(mailadress,subject,msg,'joel.closier@cern.ch',localAttempt=False)
    if not res[ 'OK' ]:
        print 'The mail could not be sent'
else:
  print 'No modifications to CS required'

DIRAC.exit(0)