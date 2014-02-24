#!/usr/bin/env python
"""
  Remove software version from the list of packages to be maintained
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                     'Usage:',
                                     '  %s [option|cfgfile] ... Name Version' % Script.scriptName,
                                     'Arguments:',
                                     '  Name:     Name of the LHCb software package',
                                     '  Version:  Version of the LHCb software package' ] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()

import DIRAC
from DIRAC                                               import gConfig
from DIRAC.FrameworkSystem.Client.NotificationClient     import NotificationClient
from DIRAC.Interfaces.API.DiracAdmin                     import DiracAdmin
from DIRAC.Core.Utilities.PromptUser                     import promptUser
from DIRAC.ConfigurationSystem.Client.Helpers.Resources  import getDIRACPlatforms

diracAdmin = DiracAdmin()
modifiedCS = False
mailadress = 'lhcb-sam@cern.ch'

def usage():
  """ help function """
  print 'Usage: %s <NAME> <VERSION>' % ( Script.scriptName )
  DIRAC.exit( 2 )

def changeCS( path, val ):
  """ update the CS with the given values """
  val.sort()
  changeresult = diracAdmin.csModifyValue( path, ', '.join( val ) )
  print changeresult
  if not changeresult['OK']:
    print "Cannot modify value of %s" % path
    print changeresult['Message']
    DIRAC.exit( 255 )

if len( args ) < 2:
  Script.showHelp()

softwareSection = '/Operations/SoftwareDistribution'
activeSection = '/Operations/SoftwareDistribution/Active'
deprecatedSection = '/Operations/SoftwareDistribution/Deprecated'

subject = '%s %s remove to DIRAC CS' % ( args[0], args[1] )
msg = 'Grid removal required for %s %s' % ( args[0], args[1] )

packageNameVersion = '%s.%s' % ( args[0], args[1] )

#First check the Active list
activeList = gConfig.getValue( activeSection, [] )
if not activeList:
  print 'ERROR: Could not get value for %s' % ( activeSection )
  DIRAC.exit( 255 )

if packageNameVersion in activeList:
  print '==> %s will be removed from %s' % ( packageNameVersion, activeSection )
  activeList.remove( packageNameVersion )
  changeCS( activeSection, activeList )
  modifiedCS = True
else:
  print '==> %s is not in %s' % ( packageNameVersion, activeSection )

#Get the list of possible system configurations
systemConfigs = getDIRACPlatforms()
if not systemConfigs['OK']:
  print 'ERROR: Could not get value for %s with message' % ( osSection )
  DIRAC.exit( 255 )

#Prompt for system configurations to add the software for
for sc in systemConfigs['Value']:
  current = gConfig.getValue( '%s/%s' % ( softwareSection, sc ), [] )
  if packageNameVersion in current:
    result = promptUser( 'Do you want to remove %s %s for system configuration %s?' % ( args[0], args[1], sc ) )
    if not result['OK']:
      print result['Message']
      DIRAC.exit( 255 )
    if result['Value'] == 'y':
      current.remove( packageNameVersion )
      print 'Removing %s for system configuration %s' % ( packageNameVersion, sc )
      changeCS( '%s/%s' % ( softwareSection, sc ), current )
      modifiedCS = True
    else:
      print "Doing nothing"
  else:
    print '==> %s is not present for system configuration %s' % ( packageNameVersion, sc )

#Remove from the deprecated software list only if this version does not exist anywhere else
deprecatedList = gConfig.getValue( deprecatedSection, [] )
if not deprecatedList:
  print 'ERROR: Could not get value for %s' % ( deprecatedSection )
  DIRAC.exit( 255 )

if packageNameVersion in deprecatedList:
  print '==> %s is present in %s' % ( packageNameVersion, deprecatedSection )
  result = promptUser( 'Do you want to remove %s %s from the Deprecated software section?' % ( args[0], args[1] ) )
  if not result['OK']:
    print result['Message']
    DIRAC.exit( 255 )
  if result['Value'] == 'y':
    deprecatedList.remove( packageNameVersion )
    changeCS( deprecatedSection, deprecatedList )
    modifiedCS = True
  else:
    print "Doing nothing"
else:
  print '==> %s is not present in %s' % ( packageNameVersion, deprecatedSection )
  result = promptUser( 'Do you want to add %s %s to the Deprecated software section?' % ( args[0], args[1] ) )
  if not result['OK']:
    print result['Message']
    DIRAC.exit( 255 )
  if result['Value'] == 'y':
    deprecatedList.append( packageNameVersion )
    changeCS( deprecatedSection, deprecatedList )
    modifiedCS = True
  else:
    print "Doing nothing"

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
    userName = diracAdmin._getCurrentUser()
    if not userName['OK']:
      print 'ERROR: Could not obtain current username from proxy'
      exitCode = 2
      DIRAC.exit( exitCode )
    userName = userName['Value']
    print 'Sending mail for software installation %s' % ( mailadress )
    res = diracAdmin.sendMail( mailadress, subject, msg )

    if not res[ 'OK' ]:
      print 'The mail could not be sent'
else:
  print 'No modifications to CS required'

DIRAC.exit( 0 )
