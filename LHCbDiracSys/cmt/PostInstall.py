#!/usr/bin/env python

import os, commands, sys

#commands.getstatusoutput(join(os.environ['LHCBRELEASES'],os.environ['DIRACSYSROOT'],'configure-dirac'))

print " "
print "You can now set the variable DIRACSYSCONFIG for the configuration of your installation"
print "export DIRACSYSCONFIG=$MYLOCATION/etc/dirac.cfg \n"
print "if This dirac.cfg does not exist yet, you can create it by running "
print "the command $VO_LHCB_SW_DIR/lib/lhcb/DIRAC/DIRAC_v5r12p28/DiracSys/configure-dirac"
print "to configure dirac. "
print "Don't forget that you should have an area where you installed your certificates authorities"
print "and that you have to keep up to date with the command dirac-admin-get-CAs"
print "eg : export X509_CERT_DIR=$MYLOCATION/etc/grid-security/certificates"
print "export X509_VOMS_DIR=$MYLOCATION/etc/grid-security/vomsdir"
print " "

sys.exit( 0 )
