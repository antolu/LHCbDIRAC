=============================
Make a release for LHCbDirac
=============================

release for service and agent
-----------------------------

Prepare the versions.cfg located in svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/versions.cfg
Don't forget to modify the __init.py__

After you have committed your change, you should  modify the releases.cfg located in svn+ssh://svn.cern.ch/reps/dirac/trunk/releases.cfg

Then create the tag for LHCbDIRAC on lxplus. (The one for DIRAC should have been done by the DIRAC release manager)

::

  SetupProject LHCbDirac
  dirac-create-svn-tag -p LHCbDIRAC -v v7r0
  dirac-distribution -r LHCb-v7r1 -t client,server -El

Don't forget to read the last line of the previous command to copy the generated files at the right place. The format is something like

::

  ( cd /tmp/joel/tmpxg8UuvDiracDist ; tar -cf - *.tar.gz *.md5 *.cfg ) | ssh lhcbprod@lxplus.cern.ch 'cd /afs/cern.ch/lhcb/distribution/DIRAC3/tars &&  tar -xvf - && ls *.tar.gz > tars.list'



To install it on the VOBOXes from lxplus:

::

  lhcb-proxy-init  -g diracAdmin
  dirac-admin-sysadmin-cli --host volhcbXX.cern.ch
  >update LHCb-v7r1
  >restart *

if you modify the machine where run the Agents, you should modify the PilotVersion in the CS before you retsart the services.
The location in the CS is /Operations/lhcb/LHCb-<setup>/Version/PilotVersion

Agents run on volhcb20, and services on volhcb18 and volhcb17


release for client
-------------------
please refer to this TWIKI page https://twiki.cern.ch/twiki/bin/view/LHCb/ProjectRelease#LHCbDirac
a quick test to validate the installation is to run the SHELL script $LHCBRELEASE/LHCBDIRAC/LHCBDIRAC_vXrY/LHCbDiracSys/test/client_test.csh

If you need to install a new version in the development environment, follow these steps:

::

  cd $LHCBDEV
  setenv CMTPROJECTPATH ${LHCBDEV}:${CMTPROJECTPATH}
  getpack -Pr Dirac vArB
  cd $LHCBDEV/DIRAC/DIRAC_vArB
  make
  cd $LHCBDEV
  getpack -Pr LHCbDirac vXrY
  cd $LHCBDEV/LHCBDIRAC/LHCBDIRAC_vXrY
  make


