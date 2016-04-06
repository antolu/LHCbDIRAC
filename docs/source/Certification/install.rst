======================================================
LHCbDIRAC Certification (development) Releases
======================================================

The following procedure applies to pre-releases (AKA certification releases) 
and it is a simpler version of what applies to production releases.

What for
====================

The certification manager of LHCbDIRAC has the role of:

1. creating the release
2. making basic tests
3. deploying it in the certification setup
4. making even more tests

Several iterations of the above, before:

5. merging in the production branch


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
  dirac-distribution -r v8r2p36 -l LHCb -C file:///`pwd`/releases.cfg (this may take some time)

Don't forget to read the last line of the previous command to copy the generated files at the right place. The format is something like::

  ( cd /tmp/joel/tmpxg8UuvDiracDist ; tar -cf - *.tar.gz *.md5 *.cfg ) | ssh lhcbprod@lxplus.cern.ch 'cd /afs/cern.ch/lhcb/distribution/DIRAC3/tars &&  tar -xvf - && ls *.tar.gz > tars.list'

And just copy/paste/execute it.

