LHCbDIRAC is the LHCb extension of [DIRAC](https://github.com/DIRACGrid/DIRAC).

Repository structure
====================

Due to the fact that we support only the production and the development versions,
only 2 branchs are present: *master*, and *devel*.

* *master* is the stable branch. Production tags are created starting from this branch.
* *devel* is the development branch. Tags created starting from this branch are subject to a certification process.

The following diagram highlights the interactions between the branches and the merging and tagging strategy:
![LHCbDIRAC branches](https://docs.google.com/drawings/d/14UPBPGW2R8d7JBO9eHWw2tyD3ApEuUBmlDEFicoBs1U/pub?w=1011&h=726)


Repositories
============

Developers should have 2 remote repositories (which is the typical GitHub/GitLab workflow):

* *origin* : cloned from your private fork done on GitLab
* *upstream* : add it via git remote add upstream and pointing to the blessed repository (https://gitlab.cern.ch/lhcb-dirac/LHCbDIRAC.git)


Issue Tracking
==============

Issue tracking is [LHCbDIRAC JIRA](https://its.cern.ch/jira/browse/LHCBDIRAC)


Code quality
============

The contributions are subject to reviews.

Pylint is run regularly on the source code. The .pylintrc file defines the expected coding rules and peculiarities (e.g.: tabs consists of 2 spaces instead of 4)


Testing
======

Unit tests are provided within the source code. Integration, regression and system tests are instead in the tests directory.

