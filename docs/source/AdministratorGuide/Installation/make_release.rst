==================
LHCbDIRAC Releases
==================

The following procedure applies fully to LHCbDIRAC production releases, like patches.
For pre-releases (AKA certification releases, there are some minor changes to consider).

Prerequisites
=============

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
2. Propagating to the devel branch (for patches)
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

If you are making a Major release please merge devel to master follow the instruction: :ref:`devel_to_master`.

Then, from the LHCbDIRAC local fork you need to update some files::


  # if you start from scratch otherwise skip the first 2 commands
  mkdir $(date +20%y%m%d) && cd $(date +20%y%m%d)
  git clone https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC.git
  cd LHCbDIRAC
  git remote rename origin upstream
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
  # Update the version in the Dockerfile file:
  vim container/lhcbdirac/Dockerfile
  # For updating the CHANGELOG, get what's changed since the last tag
  t=$(git describe --abbrev=0 --tags); git --no-pager log ${t}..HEAD --no-merges --pretty=format:'* %s';
  # copy the output, add it to the CHANGELOG (please also add the DIRAC version)
  vim CHANGELOG # please, remove comments like "fix" or "pylint" or "typo"...
  # Change the versions of the packages
  vim dist-tools/projectConfig.json
  git add -A && git commit -av -m "<YourNewTag>"


Time to tag and push::


  # make the tag
  git tag -a <YourNewTag> -m <YourNewTag>
  # push "newMaster" to upstream/master
  git push --tags upstream newMaster:master
  # delete your local newMaster
  git checkout upstream/master
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
Note: For porting the LHCbDIRAC.init.py from master to devel, we prefer the HEAD version (only for this file!!!)

Plase fix the conflict if some files are conflicting. Do not forget to to execute the following::

  git add -A && git commit -m " message"

Conflicts or not, you'll need to push back to upstream::

  # push "newDevel" to upstream/devel
  git push upstream newDevel:devel
  # delete your local newDevel
  git checkout upstream/devel
  git branch -d newDevel
  # keep your repo up-to-date
  git fetch upstream


Creating the release tarball, add uploading it to the LHCb web service
``````````````````````````````````````````````````````````````````````
Automatic procedure
^^^^^^^^^^^^^^^^^^^
When a new git tag is pushed to the repository, a gitlab-ci job takes care of testing, creating the tarball, uploading it to the web service, and to build the docker image. You can check it in the pipeline page of the repository (https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC/pipelines).

It may happen that the pipeline fails. There are various reasons for that, but normally, it is just a timeout on the runner side, so just restart the job from the pipeline web interface. If it repeatedly fails building the tarball, try the manual procedure described bellow to understand.


Manual procedure
^^^^^^^^^^^^^^^^

**This should a priori not be used anymore. If the pipeline fails, you should rather investigate why.**

Login on lxplus, run ::

  lb-run LHCbDirac/prod bash -norc

  git archive --remote ssh://git@gitlab.cern.ch:8443/lhcb-dirac/LHCbDIRAC.git devel LHCbDIRAC/releases.cfg  | tar -x -v -f - --transform 's|^LHCbDIRAC/||' LHCbDIRAC/releases.cfg

  dirac-distribution -r v8r3p1 -l LHCb -C file:///`pwd`/releases.cfg (this may take some time)

Don't forget to read the last line of the previous command to copy the generated files at the right place. The format is something like::

  ( cd /tmp/joel/tmpxg8UuvDiracDist ; tar -cf - *.tar.gz *.md5 *.cfg ) | ssh lhcbprod@lxplus.cern.ch 'cd /afs/cern.ch/lhcb/distribution/DIRAC3/tars &&  tar -xvf - && ls *.tar.gz > tars.list'

And just copy/paste/execute it.

If you do not have access to lhcbprod, you can use your user name.


2. Making basic verifications
=============================

Once the tarball is done and uploaded, the release manager is asked to make basic verifications,
to see if the release has been correctly created.

2.1. GitLab-CI pipelines
````````````````````````

Within GitLab-CI, at https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC/pipelines we run simple "unit" tests.

These pipelines will: run pylint (errors only), run all the unit tests found in the system, assess the coverage.
If the GitLab-CI pipelines are successful, we can check the system tests.

2.2. Jenkins "integration" tests
````````````````````````````````

At this `link <https://jenkins-dirac.web.cern.ch/view/LHCbDIRAC/>`_ you'll find some Jenkins Jobs ready to be started.
Please start the following Jenkins jobs and verify their output.

1. LHCbPilot2

This jobs will simply install the pilot. Please just check if the result does not show in an "unstable" status.
Also, please grep its output for "ERROR:".

2. LHCbPilot2_integration_user

This job will install the pilot. Then it will run a couple jobs. Verify its output for "ERROR:" as in the previous step.

3. LHCbPilot2_regression_user

This job will install the pilot. Then it will run a couple jobs. Verify its output for "ERROR:" as in the previous step.





3. Advertise the new release
============================

Before you start the release you must write an Elog entry 1 hour before you start the deployment.
You have to select Production and Release tick boxes.

When the intervention is over you must notify the users (reply to the Elog message).


4. Deploying the release
========================

Deploying a release means deploying it for the various installations::

* client
* server
* pilot


release for client
``````````````````

Open a JIRA task: https://its.cern.ch/jira/projects/LHCBDEP.

* JIRA task: Summary:LHCbDirac vArBpC;  Description: Please release  LHCbDirac by following the instructions:: 

`link <https://lhcb-dirac.readthedocs.io/en/latest/AdministratorGuide/Installation/make_release.html#new-procedure-for-installing-on-cvmfs-lhcb`>_


Once the client has been deployed, you should setup the correct environment (lb-run LHCbDIRAC/<version> bash --norc), preferably on a CERNVM, on lxplus otherwise, and run the following two scripts:
  * Minimal test: https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC/blob/master/tests/System/Client/basic-imports.py
  * Bigger (certification like) test: https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC/blob/master/tests/System/Client/client_test.sh


new procedure for installing on cvmfs-lhcbdev
`````````````````````````````````````````````

You should member of the e-group lhcb-cvmfs-librarians.
you login to aivoadm.cern.ch and you follow the sequence::

  ssh cvmfs-lhcbdev
  sudo -i -u cvlhcbdev
  lbcvmfsinteractive.sh -m "install vArB-preC"
  (wait to get the prompt back)
  cd /cvmfs/lhcbdev.cern.ch/lib/lhcb/LHCBDIRAC/
  cvmfs_server transaction lhcbdev.cern.ch
  export DIRAC=/cvmfs/lhcbdev.cern.ch/lib/lhcb/LHCBDIRAC/vDrE-preF (vDrE-preF is the previous installation)
  source bashrc
  dirac-install -v -r vArB-preC -t server -l LHCb -e LHCb --createLink
  rm /cvmfs/lhcbdev.cern.ch/lib/lhcb/LHCBDIRAC/pro
  source lhcbdirac vArB-preC
  pip install --trusted-host files.pythonhosted.org --trusted-host pypi.org --upgrade pip
  pip install --trusted-host files.pythonhosted.org --trusted-host pypi.org ipython
  <deploy the test directory if it is needed>
  mkdir tmp
  cd tmp/
  mkdir LHCbDIRAC
  cd LHCbDIRAC/
  git init
  git remote add -f upstream https://:@gitlab.cern.ch:8443/lhcb-dirac/LHCbDIRAC.git
  git config core.sparsecheckout true
  echo tests/ >> .git/info/sparse-checkout
  git pull upstream devel
  rm -rf .git/
  cd ../../
  cp -r tmp/LHCbDIRAC/tests/ v9r3-pre20/LHCbDIRAC/
  rm -rf tmp/
  <end of tests directory deployment>
  cd /
  cvmfs_server publish lhcbdev.cern.ch
  exit
  exit


new procedure for installing on cvmfs-lhcb
``````````````````````````````````````````

You should member of the e-group lhcb-cvmfs-librarians.
The version to be deployed is vArBpC
you login to aivoadm.cern.ch and you follow the sequence::

  ssh cvmfs-lhcb
  sudo -i -u cvlhcb
  cd /cvmfs/lhcb.cern.ch/lib/lhcb/LHCBDIRAC/
  cvmfs_server transaction lhcb.cern.ch
  source lhcbdirac vDrEpF (vDrEpF is the actual version)
  export DIRAC=/cvmfs/lhcb.cern.ch/lib/lhcb/LHCBDIRAC/vDrEpF 
  dirac-install -v -r vArBpC -t server -l LHCb -e LHCb --createLink
  source lhcbdirac vArBpC
  pip install --trusted-host files.pythonhosted.org --trusted-host pypi.org --upgrade pip
  pip install --trusted-host files.pythonhosted.org --trusted-host pypi.org ipython
  cd /
  cvmfs_server publish lhcb.cern.ch
  exit
  exit



Server
``````

To install it on the VOBOXes from lxplus::

  lhcb-proxy-init -g lhcb_admin
  dirac-admin-sysadmin-cli --host volhcbXX.cern.ch
  >update LHCbDIRAC v8r3p32
  >restart *

The (better) alternative is using the web portal or using the following script: https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC/blob/devel/dist-tools/create_vobox_update.py


The recommended way is the following::

      ssh lxplus
      mkdir -p DiracInstall && cd  DiracInstall
      curl https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC/raw/devel/dist-tools/create_vobox_update.py -O
      python create_vobox_update.py vArBpC

This command will create 6 files called "vobox_update_MyLetter" then you can run in 6 windows the recipe for one single machine like that::

      ssh lxplus
      cd  DiracInstall ; lb-run LHCbDIRAC/prod bash -norc ; lhcb-proxy-init -g lhcb_admin; dirac-admin-sysadmin-cli
            and from the prompt ::
               [host] : execfile vobox_update_MyLetter
               [host] : quit

Note:

It is normal if you see the following errors::

   --> Executing restart Framework SystemAdministrator
   [ERROR] Exception while reading from peer: (-1, 'Unexpected EOF')


In case of failure you have to update the machine by hand.
Example of a typical failure::

         --> Executing update v8r2p42
         Software update can take a while, please wait ...
        [ERROR] Failed to update the software
        Timeout (240 seconds) for '['dirac-install', '-r', 'v8r2p42', '-t', 'server', '-e', 'LHCb', '-e', 'LHCb', '/opt/dirac/etc/dirac.cfg']' call

Login to the failing machine, become dirac, execute manually the update, and restart everything. For example::

      ssh lbvobox11
      sudo su - dirac
      dirac-install -r v8r2p42 -t server -e LHCb -e LHCb /opt/dirac/etc/dirac.cfg
      lhcb-restart-agent-service
      runsvctrl t startup/Framework_SystemAdministrator/

Specify that this error can be ignored (but should be fixed ! )::

      2016-05-17 12:00:00 UTC dirac-install [ERROR] Requirements installation script /opt/dirac/versions/v8r2p42_1463486162/scripts/dirac-externals-requirements failed. Check /opt/dirac/versions/v8r2p42_1463486162/scripts/dirac-externals-requirements.err

Using the web portal:
  * You cannot do all the machines at once. Select a bunch of them (between 5 and 10). Fill in the version number and click update.
  * Repeate until you have them all.
  * Start again selecting them by block, but this time, click on "restart" to restart the components.


WebPortal
`````````

When the web portal machine is updated then you have to compile the WebApp::

    ssh lhcb-portal-dirac.cern.ch
    sudo su - dirac
    #  (for example: dirac-install -r v8r4p2 -t server -l LHCb -e LHCb,LHCbWeb,WebAppDIRAC /opt/dirac/etc/dirac.cfg)
    dirac-install -r VERSIONTOBEINSTALLED -t server -l LHCb -e LHCb,LHCbWeb,WebAppDIRAC /opt/dirac/etc/dirac.cfg
    

When the compilation is finished::

    lhcb-restart-agent-service
    runsvctrl t startup/Framework_SystemAdministrator/


TODO
````

When the machines are updated, then you have to go through all the components and check the errors. There are two possibilities:
   1. Use the Web portal (SystemAdministrator)

   2. Command line::

       for h in $(grep 'set host' vobox_update_* | awk {'print $NF'}); do echo "show errors" | dirac-admin-sysadmin-cli -H $h; done | less

Pilot
`````

Update the pilot version from the CS, keeping 2 pilot versions, for example:

   /Operation/LHCb-Production/Pilot/Version = v8r2p42, v8r241

The newer version should be the first in the list

for checking and updating the pilot version. Note that you'll need a proxy that can write in the CS (i.e. lhcb-admin).
This script will make sure that the pilot version is update BOTH in the CS and in the json file used by pilots started in the vacuum.


.. _devel_to_master:

Basic instruction how to merge the devel branch into master (NOT for PATCH release)
```````````````````````````````````````````````````````````````````````````````````

Our developer model is to keep only two branches: master and devel. When we make a major release, we have to merge devel to master.
Before the merging,  create a new branch based on master using the web interface of GitLab.
This is for safety: save the in a new branch, named e.g. "v9r1" the last commit done for "v9r1" branch.

After, you can merge devel to master (the following does it in a new directory, for safety)::

    mkdir $(date +20%y%m%d) && cd $(date +20%y%m%d)
    git clone ssh://git@gitlab.cern.ch:8443/lhcb-dirac/LHCbDIRAC.git
    cd LHCbDIRAC
    git remote rename origin upstream
    git fetch upstream
    git checkout -b newMaster upstream/master
    git merge upstream/devel
    git push upstream newMaster:master

After when you merged devel to master, the 2 branches will be strictly equivalent.
You can make the tag for the new release starting from the master branch.

5. Mesos cluster
========================

Mesos is currently only used for the certification.
In order to push a new version on the Mesos cluster, 3 steps are needed:

- Build the new image
- Push it the lhcbdirac gitlab repository
- Update the version of the running containers


Automatic procedure
````````````````````

The first two steps should be automatically done by the gitlab-ci of the LHCbDIRAC repository.
The last step will be taken care of by the gitlab-ci of the MesosClusterConf repository (https://gitlab.cern.ch/lhcb-dirac/MesosClusterConf)
For a simple version upgrade, edit directly on the gitlab web page the file clusterConfiguration.json and replace the "version" attribute with what you want. Of course add a meaningful commit message.

Manual procedure
````````````````

This should in principle not happen. Remember that any manual change of the mesos cluster will be erased next time the gitlab-ci of the MesosClusterConf repository will run.
However, you can do all the above step manually.

All these functionalities have been wrapped up in a script (dirac-docker-mgmt), available on all the lbmesosadm* machines (01, 02)

The next steps are the following::

    # build the new image
    # this will download the necessary files, and build
    # the image localy
    dirac-docker-mgmt.py -v v8r5 --build

    # Push it to the remote lhcbdirac registry
    # Your credentials for gitlab will be asked
    dirac-docker-mgmt.py -v v8r5 --release

    # Update the version of the running containers
    # The services and number of instances running
    # will be preserved
    dirac-docker-mgmt.py -v v8r5 --deploy
