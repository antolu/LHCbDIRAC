#!/usr/bin/env python
#####################################
# Author: Joel Closier
# File : lhcb-restart-agent-service
#
#####################################
"""
  Restart any agent  and service installed on a VOBOX
"""
__RCSID__ = "$Id$"

import re, os, sys

if not os.environ.has_key( 'DIRAC' ):
  print "The DIRAC environment is not set"
  sys.exit( 0 )
else:
  diracroot = os.environ['DIRAC']

if os.path.isdir( os.path.join( diracroot, 'runit' ) ) and os.path.isdir( os.path.join( diracroot, 'startup' ) ):
  diracrunit = os.path.join( diracroot, 'runit' )
  diracstartup = os.path.join( diracroot, 'startup' )
  for link in os.listdir( diracstartup ):
    system = link.split( '_' )[0]
    agent = link.split( '_' )[1]
    if re.search( 'Agent', link ):
      if not os.path.isdir( os.path.join( diracrunit, system ) ):
        os.mkdir( os.path.join( diracrunit, system ) )

      diracsystem = os.path.join( diracrunit, system )
      if not os.path.isdir( os.path.join( diracroot, 'control', system, agent ) ):
        print diracsystem
        print agent
        os.mkdir( os.path.join( diracroot, 'control', system, agent ) )

      print 'Restart Agent ' + agent
      filename_stop = os.path.join( diracroot, 'control', system, agent, 'stop_agent' )
      print filename_stop
      fd = open( filename_stop, 'w' )
      fd.close()
    else:
      if re.search( 'Framework_SystemAdministrator', link ):
        print 'Skip Framework_SystemAdministrator'
      else:
        print 'Restart Service ' + os.path.join( diracstartup, link )
        os.system( 'runsvctrl t ' + os.path.join( diracstartup, link ) )

sys.exit( 0 )
