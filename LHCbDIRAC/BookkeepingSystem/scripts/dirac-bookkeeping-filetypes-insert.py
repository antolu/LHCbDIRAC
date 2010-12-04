#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :   dirac-bookkeeping-filetypes-insert
# Author : Zoltan Mathe
########################################################################

__RCSID__ = "$Id$"

import sys, string, re
import DIRAC
from DIRAC.Core.Base import Script

from DIRAC.FrameworkSystem.Client.NotificationClient     import NotificationClient
from DIRAC.Interfaces.API.DiracAdmin                     import DiracAdmin
from DIRAC                                               import gConfig

Script.parseCommandLine( ignoreErrors = True )

diracAdmin = DiracAdmin()
modifiedCS = False
mailadress = 'lhcb-bookkeeping@cern.ch'

def changeCS( path, val ):
  val.sort()
  result = diracAdmin.csModifyValue( path, string.join( val, ', ' ) )
  print result
  if not result['OK']:
    print "Cannot modify value of %s" % path
    print result['Message']
    DIRAC.exit( 255 )

bookkeepingSection = '/Operations/Bookkeeping'
filetypeSection = '/Operations/Bookkeeping/FileTypes'

from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient import BookkeepingClient
bk = BookkeepingClient()

exitCode = 0

ftype = raw_input( "FileType: " )
desc = raw_input( "Description: " )
print 'Do you want to add this new file type? (yes or no)'
value = raw_input( 'Choice:' )
choice = value.lower()
if choice in ['yes', 'y']:
  res = bk.insertFileTypes( ftype.upper(), desc )
  if res['OK']:
    print 'The file types added successfully!'
    fileTypesList = gConfig.getValue( filetypeSection, [] )
    if not fileTypesList:
      print 'ERROR: Could not get value for %s' % ( filetypeSection )
      DIRAC.exit( 255 )

    if ftype.upper() in fileTypesList:
      print '==> %s already in %s' % ( ftype, filetypeSection )
    else:
      fileTypesList.append( ftype.upper() )
      changeCS( filetypeSection, fileTypesList )
      modifiedCS = True
  else:
    print "Error discovered!", res['Message']
elif choice in ['no', 'n']:
  print 'Aborded!'
else:
  print 'Unespected choice:', value

subject = 'new file type'
msg = '%s added to the bkk and CS!' % ( ftype )

if modifiedCS:
  result = diracAdmin.csCommitChanges( False )
  if not result[ 'OK' ]:
    print 'ERROR: Commit failed with message = %s' % ( result[ 'Message' ] )
    DIRAC.exit( 255 )
  else:
    print 'Successfully committed changes to CS'
    notifyClient = NotificationClient()
    print 'Sending mail for file type insert %s' % ( mailadress )
    res = notifyClient.sendMail( mailadress, subject, msg, 'zoltan.mathe@cern.ch', localAttempt = False )
    if not res[ 'OK' ]:
        print 'The mail could not be sent'
else:
  print 'No modifications to CS required'
DIRAC.exit( exitCode )
