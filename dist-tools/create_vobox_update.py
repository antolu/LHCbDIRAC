#!/usr/bin/env python
#
# this script create a file to be executed by dirac-admin-sysadmin-cli
#  with the command execfile vobox_update_X
#

import os, re, sys

skel_commands = """
#
set host LHCB_MACHINE_NAME
show info
update LHCB_VERSION
exec lhcb-restart-agent-service
restart Framework SystemAdministrator
#
"""

if len( sys.argv ) != 2:
  print 'usage: ' + sys.argv[0] + '  LHCbDirac_version'
  sys.exit( 1 )

lhcbver = sys.argv[1]

HOME_DIR = os.path.join( os.environ['HOME'], 'DiracAdmin' )
file_skel = os.path.join( HOME_DIR, 'skel_vobox_update' )
file_T1 = os.path.join( HOME_DIR, 'vobox_update_T1' )
T1_list = ['voboxlhcb.gridpp.rl.ac.uk', 'voboxlhcb.pic.es', 'voboxlhcb.nikhef.nl', 'voboxlhcb.cr.cnaf.infn.it', 'voboxlhcb.in2p3.fr', 'voboxlhcb.gridka.de']
#file_B = os.path.join( HOME_DIR, 'vobox_update_B' )
#B_list = ['lbvobox27.cern.ch', 'lbvobox28.cern.ch','lbvobox18.cern.ch']
file_C = os.path.join( HOME_DIR, 'vobox_update_C' )
C_list = ['lbvobox30.cern.ch', 'lbvobox100.cern.ch','lbvobox101.cern.ch','lbvobox102.cern.ch','lbvobox103.cern.ch','lbvobox109.cern.ch','lbvobox200.cern.ch']
file_D = os.path.join( HOME_DIR, 'vobox_update_D' )
D_list = ['lbvobox06.cern.ch', 'lbvobox104.cern.ch','lbvobox105.cern.ch','lbvobox106.cern.ch','lbvobox107.cern.ch','lbvobox108.cern.ch']
file_E = os.path.join( HOME_DIR, 'vobox_update_E' )
E_list = ['lbvobox43.cern.ch', 'lbvobox46.cern.ch', 'lbvobox47.cern.ch', 'volhcb04.cern.ch', 'volhcb05.cern.ch']

def generateTemplate( hosts, filename ):
  fdw = open( filename, 'w' )
  for machine in hosts:
    print machine
    command = skel_commands.replace( 'LHCB_MACHINE_NAME', machine )
    command = command.replace( 'LHCB_VERSION', lhcbver )
    fdw.write( command )
  fdw.close()


if __name__ == '__main__':
  generateTemplate(T1_list, file_T1)
  #generateTemplate(B_list, file_B)
  generateTemplate(C_list, file_C)
  generateTemplate(D_list, file_D)
  generateTemplate(E_list, file_E)
