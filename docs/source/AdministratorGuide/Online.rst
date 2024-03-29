.. _online_steps:

============
ONLINE steps
============

Installation of LHCbDirac
-------------------------

The machine running the transfers from the pit is lbdirac, and is in the online network.
This machine runs:

  * A complete RMS: ReqManager (url: RequestManagement/onlineGateway), a ReqProxy (known only from inside) and a RequestExecutingAgent
  * The RAWIntegrity system: the RAWIntegrityHandler and RAWIntegrityAgent

A special catalog is defined in the local configuration in order to keep track of the files transfered::

  RAWIntegrity
  {
    AccessType = Read-Write
    Status = Active
  }


We also have two special configuration for StorageElements::

  # Setting it to NULL to transfer without
  # checking the checksum, since it is already done by
  # the DataMover and the RAWIntegrityAgent
  # It should avoid the double read on the local disk
  ChecksumType=NULL
  # Setting this to True is dangerous...
  # If we have a SRM_FILE_BUSY, we remove the file
  # But we have enough safety net for the transfers from the pit
  SRMBusyFilesExist = True

Finally, you need to overwrite the URLS of the RMS to make sure that they use the internal RMS::

  URLs
  {
    ReqManager = dips://lbdirac.cern.ch:9140/RequestManagement/ReqManager
    ReqProxyURLs = dips://lbdirac.cern.ch:9161/RequestManagement/ReqProxy
  }

Installation/update of LHCbDirac version
----------------------------------------
Instructions to install or update a new version of LHCbDirac ::

  ssh lhcbprod@lbgw.cern.ch
  ssh store06
  cd /sw/dirac/run2
  source /sw/dirac/run2/bashrc
  dirac-install -v -r vArBpC -t server -l LHCb -e LHCb
  rm /sw/dirac/run2/pro ; ln -s versions/vArBpC_hhhhhh pro
  cd /sw/dirac/run2/pro


  foreach i (`ls`)
  if -l $i then
    echo $i
    rm $i
    ln -s ../../$i
  endif
  end
  

Workflow
--------

The DataMover is the Online code responsible for the interraction with the BKK (register the run, the files, set the replica flag), to request the physical transfers, and to remove the file of the Online storage when properly transfered.

The doc is visible here: https://lbdokuwiki.cern.ch/online_user:rundb_onlinetoofflinedataflow

The DataMover registers the Run and the files it already knows about in the BKK.
Then it creates for each file a request with a PutAndRegister operation. The target SE is CERN-RAW, the Catalog is RAWIntegrity.
The RequestExecutingAgent will execute the copy from the local online storage to CERN-RAW, and register it in the RAWIntegrity DB.

The RAWIntegrityAgent looks at all the files in the DB that are in status 'Active'.

For each of them, it will check if the file is already on tape, and if so, compare the checksum.

If the checksum is incorrect, the file remains in status 'Active', and will require manual intervention.
If the checksum is correct, we attempt to register the file in the DFC only.

If the registration fails, the file goes into 'Copied' status in the DB, c
If the registration works, we attempt to remove the file from the Online storage.
This removal Request sends a signal to the DataMover, which will mark the file for removal (garbage collection), and the replica flag to yes in the BKK.

If the removal fails, the file status is set to 'Registered' in the DB, and will be reattempted from there at the next loop.
If the removal works, the file is set to 'Done' in the DB.


HLTFram
-------

The Productions, Monte Carlo or Other, are run under the ONLINE account lhcbprod.
Under its home directory, a sub-directory called production contains all the scripts and tools to start the agent launch by PVSS.
PVSS called the script `/home/lhcbprod/production/launch_agent.sh`. which will call the DIRAC Pilot (`/home/lhcbprod/production/dirac-pilot-3.sh`).
The code for the Pilot itself is store in `/home/lhcbprod/production/Pilot3`. The content of this directory is updated every day by a cron run on the machine cron01 as lhcbprod.
The script which update it is `/home/lhcbprod/production/dirac-pilot3-cron.sh`.
