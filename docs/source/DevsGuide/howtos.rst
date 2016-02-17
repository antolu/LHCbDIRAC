HOW TOs
-------

.. toctree::
   :maxdepth: 2


Browsing the code running in production
----------------------------------------

If you want to browse the DIRAC (and LHCbDIRAC) code running in production you'll first of all have to know which version is installed. 
Announcements of new deployements are done via the LHCb operations `eLog <http://lblogbook.cern.ch/Operations/>`_. 
The code is also always installed in the afs release area (`$LHCb_release_area/DIRAC/DIRAC_vX5rYpZ/DIRAC`) but you can normally use git to switch from one to another.


I developed something, I want it in the next release
----------------------------------------------------

Just open a merge request to the devel branch of LHCbDirac: all the releases (minor and major) are created branching from this branch.


Asking for a LHCbDIRAC patch
------------------------------

Just open a merge request to the master branch of LHCbDirac. If in a hurry, drop an e-mail to the lhcb-dirac mailing list. 
