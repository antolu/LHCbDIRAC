#!/usr/bin/env python
"""
  Modify Configuration to add a new SW in the automatic installation list
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
from DIRAC                            import gConfig
from DIRAC.Interfaces.API.DiracAdmin  import DiracAdmin
from DIRAC.Core.Utilities.PromptUser  import promptUser
from DIRAC.ConfigurationSystem.Client.Helpers.Resources import getDIRACPlatforms

diracAdmin = DiracAdmin()
modifiedCS = False
mailadress = 'lhcb-sam@cern.ch'

def changeCS( path, val ):
  """ update the Configuration service """
  val.sort()
  ret = diracAdmin.csModifyValue( path, ', '.join( val ) )
  if not ret['OK']:
    print "Cannot modify value of %s" % path
    print ret['Message']
    DIRAC.exit( 255 )

if len( args ) != 2:
  Script.showHelp()

softwareSection = '/Operations/SoftwareDistribution'
activeSection = '/Operations/SoftwareDistribution/Active'
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
systemConfigs = getDIRACPlatforms()
if not systemConfigs['OK']:
  print 'ERROR: Could not get value for with message %s' % systemConfigs['Message']
  DIRAC.exit( 255 )

#Prompt for system configurations to add the software for
for sc in systemConfigs['Value']:
  current = gConfig.getValue( '%s/%s' % ( softwareSection, sc ), [] )
  if not packageNameVersion in current:
    question = 'Do you want to add %s %s for system configuration %s?' % ( args[0], args[1], sc )
    result = promptUser( question )
    if not result['OK']:
      print result['Message']
      DIRAC.exit( 255 )
    if result['Value'] == 'y':
      current.append( packageNameVersion )
      print 'Adding %s for system configuration %s' % ( packageNameVersion, sc )
      changeCS( '%s/%s' % ( softwareSection, sc ), current )
      modifiedCS = True
    else:
      print "Doing nothing"
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
