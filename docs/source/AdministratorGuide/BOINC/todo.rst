=====
TO-DO
=====

Stalled jobs
==================
In the current setup:

- The VM is running for a set amount of time (currently 24 hours)
- The JobAgent has no limit on its number of cycles

BOINC will kill the VM at the end of the set time, and any job that was running at the time will stall.

Some possible solutions are:

1) Put a limit of one cycle on the JobAgent, and shutdown the VM at the end of this cycle. This will end the BOINC task, and a new one will begin.
2) Make use of the planned $MACHINEFEATURES and $JOBFEAUTURES to signal that we have 24 hours of computing available and let the jobs adapt.

InputData
=========

If we need input data, there is currently no way: the real storages are define so that the upload fail and we create a request. The downloading would need to go throught a proxy. Maybe we could have a read only proxy, and the result would be the same as for the sandbox?

SandboxStore
============

At the moment, the output sandbox are not used at all. They are uplaoded on the BOINC gateway, and stay there forever.  We can:
* not upload them (point to WMSSecureGW and always return S_OK)
* forward them (either directly (not good) or by adding an extra step in the SecureOutput)


StorageElement
==============

The disk often fills up because things are not cleaned.

Find the files and sort them by date::

  find -type f -printf '%a %p\n'| sort -k1


Find files older than 60 days and sort::

  find -mtime +60 -type f -printf '%a %p\n' | sort -k1

We can assume that after being there 60 days, they will not be touched anymore...

To remove, append to your find command::

  -exec rm {} \;


Alternative (quicker find)::

  find -mtime +60 -type f -print0 | xargs -0 ls -lt

to keep only the path, append::
  | awk -F' ' '{print $9}'

to remove, append to the previous::
  | xargs rm -f



Accounting
==========

BOINC credits
"""""""""""""
The BOINC accounting system is active and grants credits to the volunteers based on the time the VM ran and their CPU power.

This is not in most cases the actual work done on DIRAC jobs. This is maybe not an issue, as it still credit volunteers for the time they give us (after all, it's our problem if we don't use it).

Note that BOINC only gives credit at the end of a succesfull run. This has two constraints:

- VMs must have a set run time
- A failed or cancelled VM run on BOINC side will not grant credits, regardless of what happened inside the VM (eg. successful jobs)

An alternate method of granting credits on the run exist but is considered deprecated by the BOINC developers.
Having this credit system enabled is pretty much mandatory to attract people within the BOINC community, especially the competitive minded volunteers.


LHCb accounting
"""""""""""""""


Volunteers accounting
*********************
The Test4Theory project uses a second accounting method, based on the number of event produced. While not a good metric in our case as all events are not the same, a similar system could be implemented.

Currently, BOINC VMs hostname are set to boinc[host id]. This enables a basic job accounting, since the hostname will appear in the parameters of the jobs.
Philippe Charpentier made a script to select jobs based on this: ``WorkloadManagementSystem/scripts/dirac-wms-get-wn.py``. Because the hostname is not a primary key, this is slow.
A web interface could be made available to volunteers to access this information.

Alternative? Use a noSQL db to store ~json~ data with job parameters (yet another accounting)


New VM contextualization / merge BOINC-specific changes ?
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""
Quite a lot of BOINC-specific additions have been introduced in the contextualization and would need to be properly merged in the new contextualization. They currently reside in a branch at: http://svnweb.cern.ch/world/wsvn/dirac/LHCbVMDIRAC/branches/LHCbVMDIRAC_v0_BOINC_branch/.


Web page
""""""""
The project web page is pretty much just the default BOINC page, this would probably need some additions.

Security aspects
""""""""""""""""
The VM is now accessible as boinc:boinc:boinc. See the amiconfig file.

The certificate is in the image that is created. Use the SSH contextualization?


WMSSecureGW development
=======================

Some developments are clearly needed

* Do something sensible in the SecureOutput operation
* Set the owner DN to the real owner of the job, not the shifter
* Keep a mapping of "boincID <-> JobID" if possible


Other stuff
"""""""""""


Laurence said that we could submit directly to his HTCondor, so we would not need anymore his intervention. Much better.
We would need a MrBoinc pilot proxy, and a MrBoinc user proxy (could technicaly be the same)
And then a site director on the BOINC gateway
Luca should do that, with the help of Andrew.
