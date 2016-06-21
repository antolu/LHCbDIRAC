#!/usr/bin/env python
#
# this script create a file to be executed by dirac-admin-sysadmin-cli
#  with the command execfile vobox_update_X
#

import os, re, sys

if len( sys.argv ) != 2:
  print 'usage: ' + sys.argv[0] + '  LHCbDirac_version'
  sys.exit( 1 )

lhcbver = sys.argv[1]

HOME_DIR = os.path.join( os.environ['HOME'], 'DiracAdmin' )
file_skel = os.path.join( HOME_DIR, 'skel_vobox_update' )
file_T1 = os.path.join( HOME_DIR, 'vobox_update_T1' )
T1_list = ['voboxlhcb.gridpp.rl.ac.uk', 'voboxlhcb.pic.es', 'voboxlhcb.nikhef.nl', 'voboxlhcb.cr.cnaf.infn.it', 'voboxlhcb.in2p3.fr', 'voboxlhcb.gridka.de']
file_A = os.path.join( HOME_DIR, 'vobox_update_A' )
A_list = ['lbvobox12.cern.ch', 'lbvobox13.cern.ch', 'lbvobox15.cern.ch', 'lbvobox16.cern.ch', 'lbvobox17.cern.ch', 'lbvobox18.cern.ch', 'lbvobox19.cern.ch']
file_B = os.path.join( HOME_DIR, 'vobox_update_B' )
B_list = ['lbvobox20.cern.ch', 'lbvobox22.cern.ch', 'lbvobox23.cern.ch', 'lbvobox24.cern.ch', 'lbvobox25.cern.ch', 'lbvobox27.cern.ch', 'lbvobox28.cern.ch', 'lbvobox29.cern.ch']
file_C = os.path.join( HOME_DIR, 'vobox_update_C' )
C_list = ['lbvobox30.cern.ch', 'lbvobox31.cern.ch', 'lbvobox32.cern.ch', 'lbvobox33.cern.ch','lbvobox100.cern.ch','lbvobox101.cern.ch','lbvobox102.cern.ch','lbvobox103.cern.ch']
file_D = os.path.join( HOME_DIR, 'vobox_update_D' )
D_list = ['lbvobox06.cern.ch', 'lbvobox07.cern.ch', 'lbvobox08.cern.ch', 'lbvobox09.cern.ch','lbvobox104.cern.ch','lbvobox105.cern.ch','lbvobox106.cern.ch']
file_E = os.path.join( HOME_DIR, 'vobox_update_E' )
E_list = ['lbvobox40.cern.ch', 'lbvobox41.cern.ch', 'lbvobox42.cern.ch', 'lbvobox43.cern.ch', 'lbvobox44.cern.ch', 'lbvobox46.cern.ch', 'lbvobox47.cern.ch', 'lbvobox48.cern.ch', 'lbvobox49.cern.ch', 'lbvobox80.cern.ch', 'volhcb04.cern.ch', 'volhcb05.cern.ch']

def generateTemplate(hosts, filename):
  fdr = open( file_skel )
  lines = fdr.readlines()
  fdr.close()
  fdw = open( filename, 'w' ) 
  for machine in hosts:
    print machine    
    for line in lines:
      if 'LHCB_MACHINE_NAME' in line:
         newline = line.replace( 'LHCB_MACHINE_NAME', machine )
      elif 'LHCB_VERSION' in line:
         newline = line.replace( 'LHCB_VERSION', lhcbver )
      else:
         newline = line
      fdw.write( newline )
  fdw.close()
    

if __name__ == '__main__':
  generateTemplate(T1_list, file_T1)
  generateTemplate(A_list, file_A)
  generateTemplate(B_list, file_B)
  generateTemplate(C_list, file_C)
  generateTemplate(D_list, file_D)
  generateTemplate(E_list, file_E)

