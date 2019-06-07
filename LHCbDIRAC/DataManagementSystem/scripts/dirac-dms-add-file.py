#!/usr/bin/env python
###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
########################################################################
# $HeadURL$
# File :    dirac-dms-add-file
########################################################################
"""
  Upload a file to the grid storage and register it in the File Catalog
"""
__RCSID__ = "$Id$"

if __name__ == "__main__":

  from DIRAC.Core.Base import Script
  Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                       'Usage:',
                                       '  %s [option|cfgfile] ... LFN Path SE [GUID]' % Script.scriptName,
                                       'Arguments:',
                                       '  LFN:      Logical File Name',
                                       '  Path:     Local path of the file',
                                       '  SE:       DIRAC Storage Element',
                                       '  GUID:     GUID to use in the registration (optional)' ,
                                       '',
                                       ' ++ OR ++',
                                       '',
                                       'Usage:',
                                       '  %s [option|cfgfile] ... LocalFile' % Script.scriptName,
                                       'Arguments:',
                                       '  LocalFile: Path to local file containing all the above, i.e.:',
                                       '  lfn1 localfile1 SE [GUID1]',
                                       '  lfn2 localfile2 SE [GUID2]'] )
                          )

  Script.parseCommandLine( ignoreErrors = True )

  from LHCbDIRAC.DataManagementSystem.Client.ScriptExecutors import executeAddFile
  from DIRAC import exit
  exit( executeAddFile() )
