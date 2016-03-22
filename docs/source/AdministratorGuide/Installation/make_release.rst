==================
LHCbDIRAC Releases
==================

The following procedure applies fully to LHCbDIRAC production releases, like patches.
For pre-releases (AKA certification releases, there are some minor changes to consider).

Prerequisites
====================

The release manager needs to:

- be aware of the LHCbDIRAC repository structure and branching,
as highlighted in the  `contribution guide <https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC/blob/master/CONTRIBUTING.md>`_.
- have forked LHCbDIRAC on GitLab as a "personal project" (called "origin" from now on)
- have cloned origin locally
- have added `<https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC>`_ as "upstream" repository to the local clone
- have push access to the master branch of "upstream" (being part of the project "owners")
- have DIRAC installed
- have been grated write access to <webService>
- have "lhcb_admin" or "diracAdmin" role.
- have a Proxy

The release manager of LHCbDIRAC has the triple role of:

1. creating the release
2. making basic verifications
3. deploying it in production


1. Creating the release
=======================

Unless otherwise specified, (patch) releases of LHCbDIRAC are usually done "on top" of the latest production release of DIRAC.
The following of this guide assumes the above is true.

Creating a release of LHCbDIRAC means creating a tarball that contains the release code. This is done in 3 steps:

1. Merging "Merge Requests"
2. Propagating to the devel branch
3. Creating the release tarball, add uploading it to the LHCb web service

But before:

Pre
```

Verify what is the last tag of DIRAC::

  # it should be in this list:
  git describe --tags $(git rev-list --tags --max-count=10)

A tarball containing it is should be already
uploaded `here <http://lhcbproject.web.cern.ch/lhcbproject/dist/Dirac_project/installSource/>`_

You may also look inside the .cfg file for the DIRAC release you're looking for: it will contain an "Externals" version number,
that should also be a tarball uploaded in the same location as above.

If all the above is ok, we can start creating the LHCbDIRAC release.


Merging "Merge Requests"
````````````````````````

`Merge Requests (MR) <https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC/merge_requests>`_ that are targeted to the master branch
and that have been approved by a reviewer are ready to be merged

If there are no MRs, or none ready: please skip to the "update the CHANGELOG" subsection.

Otherwise, simply click the "Accept merge request" button for each of them.

Then, from the LHCbDIRAC local fork::

  # update your "local" /upstream/master branch
  git fetch upstream
  # determine the tag you're going to create by checking what was the last one from the following list (and 1 to the "p"):
  git describe --tags $(git rev-list --tags --max-count=5)
  # Update the version in the __init__ file:
  vim LHCbDIRAC/__init__.py
  # Update the version in the releases.cfg file:
  vim LHCbDIRAC/releases.cfg

For updating the CHANGELOG::

  # get what's changed since the last tag:
  t=$(git describe --abbrev=0 --tags); git --no-pager log ${t}..HEAD --no-merges --pretty=format:'* %s';
  # get what's in, add it to the CHANGELOG (please also add the DIRAC version):
  vim CHANGELOG
  # Commit
  git add -A && git commit -av -m "<YourNewTag>"
  # make the tag
  git -a -m <YourNewTag>


Propagate to the devel branch and push
``````````````````````````````````````

Now, you need to make sure that what's merged in master is propagated to the devel branch. From the local fork::

  # get the updated CHANGELOG
  git fetch upstream
  # create a "newDevel" branch which from the /upstream/devel branch
  git co -b newDevel upstream/devel
  # merge in newDevel the content of upstream/master (fix possible conflicts)
  git merge /upstream/master
  # push "newDevel" to upstream/devel
  git push --tags upstream newDevel:devel
  # delete your local newDevel
  git branch -d newDevel


Creating the release tarball, add uploading it to the LHCb web service
```````````````````````````````````````````````````````````````````````
  SetupProject LHCbDirac
  git archive --remote ssh://git@gitlab.cern.ch:7999/lhcb-dirac/LHCbDIRAC.git devel LHCbDIRAC/releases.cfg  | tar -x -v -f - --transform 's|^LHCbDIRAC/||' LHCbDIRAC/releases.cfg
  dirac-distribution -r v8r2p36 -l LHCb -C file:///`pwd`/releases.cfg

Don't forget to read the last line of the previous command to copy the generated files at the right place. The format is something like

::

  ( cd /tmp/joel/tmpxg8UuvDiracDist ; tar -cf - *.tar.gz *.md5 *.cfg ) | ssh lhcbprod@lxplus.cern.ch 'cd /afs/cern.ch/lhcb/distribution/DIRAC3/tars &&  tar -xvf - && ls *.tar.gz > tars.list'





Making basic verifications
==========================

<Jenkins stuff>



Deploying the release
==========================

VOBOXes
```````


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
`````````````````````

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
