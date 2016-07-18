======================================================
LHCbDIRAC Certification (development) Releases
======================================================

The following procedure applies to pre-releases (AKA certification releases) 
and it is a simpler version of what applies to production releases. 

This page details the duty of the release manager. 
The certification manager duties are detailed in the next page.


What for
====================

The *release manager* of LHCbDIRAC has the role of:

1. creating the pre-release
2. making basic tests
3. deploying it in the certification setup

The *certification manager* would then follow-up on this by:
4. making even more tests

And, after several iterations of the above, before:
5. merging in the production branch

Points 4 and 5 won't anyway be part of this first document.


1. Creating the release
=======================

Unless otherwise specified, certification releases of LHCbDIRAC are done "on top" of the latest pre-release of DIRAC.
The following of this guide assumes the above is true.

Creating a pre-release of LHCbDIRAC means creating a tarball that contains the code to certify. This is done in 2 steps:

1. Merging "Merge Requests"
2. Creating the release tarball, add uploading it to the LHCb web service

But before:

Pre
```

If you use a version of git prior to 1.8, remove teh option *--pretty* in the command line

Verify what is the last tag of DIRAC::

  # it should be in this list:
  git describe --tags $(git rev-list --tags --max-count=10)

A tarball containing it is should be already
uploaded `here <http://lhcbproject.web.cern.ch/lhcbproject/dist/Dirac_project/installSource/>`_

You may also look inside the .cfg file for the DIRAC release you're looking for: it will contain an "Externals" version number,
that should also be a tarball uploaded in the same location as above.

If all the above is ok, we can start creating the LHCbDIRAC pre-release.


Merging "Merge Requests"
````````````````````````

`Merge Requests (MR) <https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC/merge_requests>`_ that are targeted to the devel branch
and that have been approved by a reviewer are ready to be merged

If there are no MRs, or none ready: please skip to the "update the CHANGELOG" subsection.

Otherwise, simply click the "Accept merge request" button for each of them.

Then, from the LHCbDIRAC local fork you need to update some files::

  # if you start from scratch otherwise skip the first 2 commands
  git clone https://:@gitlab.cern.ch:8443/lhcb-dirac/LHCbDIRAC.git
  git remote add upstream https://:@gitlab.cern.ch:8443/lhcb-dirac/LHCbDIRAC.git
  # update your "local" upstream/master branch
  git fetch upstream
  # create a "newDevel" branch which from the upstream/devel branch
  git checkout -b newDevel upstream/devel
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
  #If needed, change the versions of the packages  
  vim dist-tools/projectConfig.json
  # Commit in your local newDevel branch the 3 files you modified
  git add -A && git commit -av -m "<YourNewTag>"


Time to tag and push::

  # make the tag
  git tag -a <YourNewTag> -m <YourNewTag>
  # push "newDevel" to upstream/devel
  git push --tags upstream newDevel:devel
  # delete your local newDevel
  git branch -d newDevel


Remember: you can use "git status" at any point in time to make sure what's the current status.



Creating the release tarball, add uploading it to the LHCb web service
```````````````````````````````````````````````````````````````````````
Login on lxplus, run ::

  SetupProject LHCbDirac
  git archive --remote ssh://git@gitlab.cern.ch:7999/lhcb-dirac/LHCbDIRAC.git devel LHCbDIRAC/releases.cfg  | tar -x -v -f - --transform 's|^LHCbDIRAC/||' LHCbDIRAC/releases.cfg
  dirac-distribution -r v8r3-pre5 -l LHCb -C file:///`pwd`/releases.cfg (this may take some time)

Don't forget to read the last line of the previous command to copy the generated files at the right place. The format is something like::

  ( cd /tmp/joel/tmpxg8UuvDiracDist ; tar -cf - *.tar.gz *.md5 *.cfg ) | ssh $USER@lxplus.cern.ch 'cd /afs/cern.ch/lhcb/distribution/DIRAC3/tars &&  tar -xvf - && ls *.tar.gz > tars.list'

And just copy/paste/execute it.




2. Making basic verifications
==============================

Once the tarball is done and uploaded, the release manager is asked to make basic verifications, via Jenkins,
if the release has been correctly created.

At this `link <https://lhcb-jenkins.cern.ch/jenkins/view/LHCbDIRAC/>`_ you'll find some Jenkins Jobs ready to be started.
Please start the following Jenkins jobs and come back in about an hour to see the results for all of them.

1. https://lhcb-jenkins.cern.ch/jenkins/view/LHCbDIRAC/job/PRERELEASE__pylint_unit/

This job will: run pylint (errors only), run all the unit tests found in the system, assess the coverage.
The job should be considered successful if:

- the pylint error report didn't increase from the previous job run
- the test results didn't get worse from the previous job run
- the coverage didn't drop from the previous job run


2. https://lhcb-jenkins.cern.ch/jenkins/view/LHCbDIRAC/job/PRERELEASE__pilot/

This job will simply install the pilot. Please just check if the result does not show in an "unstable" status


3. https://lhcb-jenkins.cern.ch/jenkins/view/LHCbDIRAC/job/RELEASE__/

   TODO


3. Deploying the release
==========================

Deploying a release means deploying it for some installation::

* client
* server
* pilot


release for client
``````````````````

Please refer to this `TWIKI page <https://twiki.cern.ch/twiki/bin/view/LHCb/ProjectRelease#LHCbDirac>`_
a quick test to validate the installation is to run the SHELL script $LHCBRELEASE/LHCBDIRAC/LHCBDIRAC_vXrY/LHCbDiracSys/test/client_test.csh

go to this `web page <https://lhcb-jenkins.cern.ch/jenkins/job/lhcb-release/build/>`_ for asking to install the client release in AFS and CVMFS:
    
* in the field "Project list" put : "Dirac vNrMpK LHCbDirac vArBpC LHCbGrid vArB"
* in the field "platforms" put : "x86_64-slc6-gcc48-opt x86_64-slc6-gcc49-opt"
* inthe field "build_tool" put : "CMake"
* inthe field "scripts_version" put : "support-platform-indep-projects"

Then click on the "BUILD" button
    
* within 10-15 min the build should start to appear in the nightlies page https://lhcb-nightlies.cern.ch/release/   
* if there is a problem in the build, it can be re-started via the dedicated button (it will not restart by itself after a retag)


When the release is finished https://lhcb-nightlies.cern.ch/release/, you can deploy to the client. 

Note: Please execute the following commands sequentially.

The following commands used to prepare the RPMs::

    ssh lhcb-archive
    export build_id=1520
    lb-release-rpm /data/artifacts/release/lhcb-release/$build_id
    lb-release-rpm --copy /data/artifacts/release/lhcb-release/$build_id

If the rmps are created, you can deploy the release (Do not execute parallel the following commands)::
    
    ssh lxplus
    cd /afs/cern.ch/lhcb/software/lhcb_rpm_dev
    export MYSITEROOT=/afs/cern.ch/lhcb/software/lhcb_rpm_dev
    export MyProject=Dirac
    export MyVersion=vArBpC
    ./lbpkr rpm -- -ivh --nodeps /afs/cern.ch/lhcb/distribution/rpm/lhcb/${MyProject^^}_${MyVersion}*
    export MyProject=LHCbDirac
    export MyVersion=vArB-preC
    ./lbpkr rpm -- -ivh --nodeps /afs/cern.ch/lhcb/distribution/rpm/lhcb/${MyProject^^}_${MyVersion}*


Server
```````

To install it on the VOBOXes (certification only) from lxplus::

  lhcb-proxy-init  -g diracAdmin
  dirac-admin-sysadmin-cli --host volhcbXX.cern.ch
  >update LHCbDIRAC-v8r3-pre5
  >restart *

The (better) alternative is using the web portal.



Pilot
``````

Use the following script (from, e.g., lxplus after having run `lb-run --dev LHCbDIRAC bash`)::

  dirac-pilot-version

for checking and updating the pilot version. Note that you'll need a proxy that can write in the CS (i.e. lhcb-admin). 
This script will make sure that the pilot version is update BOTH in the CS and in the json file used by pilots started in the vacuum. The command to update is 
  dirac-pilot-version -S v8r3-pre5

Make sure that you are in the certification setup (e.g. check the content of your .dirac.cfg file)

