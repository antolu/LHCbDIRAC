==================
LHCbDIRAC Releases
==================

The following procedure applies fully to LHCbDIRAC production releases, like patches.
For pre-releases (AKA certification releases, there are some minor changes to consider).

Prerequisites
====================

The release manager needs to:

- be aware of the LHCbDIRAC repository structure and branching as highlighted in the  `contribution guide <https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC/blob/master/CONTRIBUTING.md>`_.
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

Then, from the LHCbDIRAC local fork you need to update some files::

  # update your "local" upstream/master branch
  git fetch upstream
  # create a "newMaster" branch which from the upstream/master branch
  git checkout -b newMaster upstream/master
  # determine the tag you're going to create by checking what was the last one from the following list (add 1 to the "p"):
  git describe --tags $(git rev-list --tags --max-count=5)
  # Update the version in the __init__ file:
  vim LHCbDIRAC/__init__.py
  # Update the version in the releases.cfg file:
  vim LHCbDIRAC/releases.cfg
  # For updating the CHANGELOG, get what's changed since the last tag
  t=$(git describe --abbrev=0 --tags); git --no-pager log ${t}..HEAD --no-merges --pretty=format:'* %s';
  # copy the output, add it to the CHANGELOG (please also add the DIRAC version)
  vim CHANGELOG # please, remove comments like "fix" or "pylint" or "typo"...
  #modify the DIRAC version
  vim cmt/project.cmt
  
    project LHCbDirac
    use DIRAC DIRAC_v6r14p25
    #use DIRAC DIRAC_v6r15-pre*
    use LHCBGRID LHCBGRID_v9r*
    use LCGCMT LCGCMT_79
  
  # Commit in your local newMaster branch the 3 files you modified
  git add -A && git commit -av -m "<YourNewTag>"


Time to tag and push::

  # make the tag
  git tag -a <YourNewTag> -m <YourNewTag>
  # push "newMaster" to upstream/master
  git push --tags upstream newMaster:master
  # delete your local newMaster
  git branch -d newMaster


Remember: you can use "git status" at any point in time to make sure what's the current status.



Propagate to the devel branch
`````````````````````````````

Now, you need to make sure that what's merged in master is propagated to the devel branch. From the local fork::

  # get the updates (this never hurts!)
  git fetch upstream
  # create a "newDevel" branch which from the upstream/devel branch
  git checkout -b newDevel upstream/devel
  # merge in newDevel the content of upstream/master
  git merge upstream/master

The last operation may result in potential conflicts.
If happens, you'll need to manually update the conflicting files (see e.g. this `guide <https://githowto.com/resolving_conflicts>`_).
As a general rule, prefer the master fixes to the "HEAD" (devel) fixes. Remember to add and commit once fixed.

Conflicts or not, you'll need to push back to upstream::

  # push "newDevel" to upstream/devel
  git push upstream newDevel:devel
  # delete your local newDevel
  git branch -d newDevel
  # keep your repo up-to-date
  git fetch upstream


Creating the release tarball, add uploading it to the LHCb web service
```````````````````````````````````````````````````````````````````````
  Login on lxplus, run ::

  SetupProject LHCbDirac
  
  git archive --remote ssh://git@gitlab.cern.ch:7999/lhcb-dirac/LHCbDIRAC.git devel LHCbDIRAC/releases.cfg  | tar -x -v -f - --transform 's|^LHCbDIRAC/||' LHCbDIRAC/releases.cfg
  
  dirac-distribution -r v8r2p36 -l LHCb -C file:///`pwd`/releases.cfg

(this may take some time)

Don't forget to read the last line of the previous command to copy the generated files at the right place. The format is something like::

  ( cd /tmp/joel/tmpxg8UuvDiracDist ; tar -cf - *.tar.gz *.md5 *.cfg ) | ssh lhcbprod@lxplus.cern.ch 'cd /afs/cern.ch/lhcb/distribution/DIRAC3/tars &&  tar -xvf - && ls *.tar.gz > tars.list'





2. Making basic verifications
==============================

Once the tarball is done and uploaded, the release manager is asked to make basic verifications, via Jenkins,
if the release has been correctly created.

At this `link <https://lhcb-jenkins.cern.ch/jenkins/view/LHCbDIRAC/>`_ you'll find some Jenkins Jobs ready to be started.
Please start all of the Jenkins jobs whose name start with "RELEASE" and come back in about an hour to see the results for all of them.

1. https://lhcb-jenkins.cern.ch/jenkins/view/LHCbDIRAC/job/RELEASE__pylint_unit/

This job will: run pylint (errors only), run all the unit tests found in the system, assess the coverage.
The job should be considered successful if:

- the pylint error report didn't increase from the previous job run
- the test results didn't get worse from the previous job run
- the coverage didn't drop from the previous job run


2. https://lhcb-jenkins.cern.ch/jenkins/view/LHCbDIRAC/job/RELEASE__pilot/

This job will simply install the pilot. Please just check if the result does not show in an "unstable" status


3. https://lhcb-jenkins.cern.ch/jenkins/view/LHCbDIRAC/job/RELEASE__/

   TODO


3. Advertise the new release
==========================

Before you start the release you must write an Eloog entry 1 hour before you start the deployment.
You have to select Production and Release tick boxes. 
When the intervention is over you must notify the users (replay to the Eloog message). 


4. Deploying the release
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

    go to the web page : https://lhcb-jenkins.cern.ch/jenkins/job/lhcb-release/
    
    - in the field "Project list" put : "Dirac vNrMpK LHCbDirac vArBpC"
    
    - in the field "platforms" put : "x86_64-slc6-gcc48-opt x86_64-slc6-gcc49-opt"
    
    Then click on the "BUILD" button
    
    - within 10-15 min the build should start to appear in the nightlies page https://lhcb-nightlies.cern.ch/release/
    
    - if there is a problem in the build, it can be re-started via the dedicated button (it will not restart by itself after a retag)


If it is the production release, and only in this case, once satisfied by the build, take note of the build id (you can use the direct link icon) and make the request via https://sft.its.cern.ch/jira/browse/LHCBDEP

The LHCb Deployement shifter will deploy the release on AFS/CVMFS

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


