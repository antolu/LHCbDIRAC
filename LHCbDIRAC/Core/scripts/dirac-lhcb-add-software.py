#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-lhcb-add-software
# Author :  Stuart Paterson
########################################################################
"""
  Modify Configuration to add a new SW in the automatic installation list
"""
__RCSID__ = "$Id$"

from DIRAC.FrameworkSystem.Client.NotificationClient     import NotificationClient
import DIRAC
from DIRAC.Core.Base import Script
from DIRAC.Interfaces.API.DiracAdmin                         import DiracAdmin
from DIRAC                                                   import gConfig

import string

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Name Version' % Script.scriptName,
                                     'Arguments:',
                                     '  Name:     Name of the LHCb software package',
                                     '  Version:  Version of the LHCb software package' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
diracAdmin = DiracAdmin()
modifiedCS = False
mailadress = 'lhcb-sam@cern.ch'

def changeCS( path, val ):
  val.sort()
  result = diracAdmin.csModifyValue( path, string.join( val, ', ' ) )
  print result
  if not result['OK']:
    print "Cannot modify value of %s" % path
    print result['Message']
    DIRAC.exit( 255 )

if len( args ) != 2:
  Script.showHelp()

softwareSection = '/Operations/SoftwareDistribution'
activeSection = '/Operations/SoftwareDistribution/Active'
osSection = '/Resources/Computing/OSCompatibility'
deprecatedSection = '/Operations/SoftwareDistribution/Deprecated'

packageNameVersion = '%s.%s' % ( args[0], args[1] )
subject = '%s %s add to DIRAC CS' % ( args[0], args[1] )
msg = 'Grid installation required for %s %s' % ( args[0], args[1] )
#First check the Active list
activeList = gConfig.getValue( activeSection, [] )
if not activeList:
  print 'ERROR: Could not get value for %s' % ( activeSection )
  DIRAC.exit( 255 )

if packageNameVersion in activeList:
  print '==> %s already in %s' % ( packageNameVersion, activeSection )
else:
  activeList.append( packageNameVersion )
  changeCS( activeSection, activeList )

#Get the list of possible system configurations
systemConfigs = gConfig.getOptions( osSection )
if not systemConfigs['OK']:
  print 'ERROR: Could not get value for %s with message' % ( osSection, result['Message'] )
  DIRAC.exit( 255 )

#Prompt for system configurations to add the software for
for sc in systemConfigs['Value']:
  current = gConfig.getValue( '%s/%s' % ( softwareSection, sc ), [] )
  if not packageNameVersion in current:
    result = diracAdmin._promptUser( 'Do you want to add %s %s for system configuration %s?' % ( args[0], args[1], sc ) )
    if result['OK']:
      current.append( packageNameVersion )
      print 'Adding %s for system configuration %s' % ( packageNameVersion, sc )
      changeCS( '%s/%s' % ( softwareSection, sc ), current )
      modifiedCS = True
  else:
    print '==> %s is already defined for system configuration %s' % ( packageNameVersion, sc )

#Check that the package name and version is not in the Deprecated software list
deprecatedList = gConfig.getValue( deprecatedSection, [] )
if not deprecatedList:
  print 'ERROR: Could not get value for %s' % ( deprecatedSection )
  DIRAC.exit( 255 )

if packageNameVersion in deprecatedList:
  deprecatedList.remove( packageNameVersion )
  changeCS( deprecatedSection, deprecatedList )
  modifiedCS = True

#Commit the changes if nothing has failed and the CS has been modified
if modifiedCS:
  result = diracAdmin.csCommitChanges( False )
  print result
  if not result[ 'OK' ]:
    print 'ERROR: Commit failed with message = %s' % ( result[ 'Message' ] )
    DIRAC.exit( 255 )
  else:
    print 'Successfully committed changes to CS'
    notifyClient = NotificationClient()
    print 'Sending mail for software installation %s' % ( mailadress )
    res = notifyClient.sendMail( mailadress, subject, msg, 'joel.closier@cern.ch', localAttempt = False )
    if not res[ 'OK' ]:
        print 'The mail could not be sent'
else:
  print 'No modifications to CS required'

DIRAC.exit( 0 )
