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


Workflow
--------

The DataMover is the Online code responsible for the interraction with the BKK (register the run, the files, set the replica flag), to request the phusical transfers, and to remove the file of the Online storage when properly transfered.

The doc is visible here: https://lbdokuwiki.cern.ch/online_user:rundb_onlinetoofflinedataflow

The DataMover registers the Run and the files it already knows about in the BKK.
Then it creates for each file a request with a PutAndRegister operation. The target SE is CERN-RAW, the Catalog is RAWIntegrity.
The RequestExecutingAgent will execute the copy from the local online storage to CERN-RAW, and register it in the RAWIntegrity DB.

The RAWIntegrityAgent looks at all the files in the DB that are in status 'Active'.

For each of them, it will check if the file is already on tape, and if so, compare the checksum.

If the checksum is correct, the file is registered in the DFC only, a removal request is sent to the local ReqManager, and the status set to 'Done'.
This removal Request sends a signal to the DataMover, which will mark the file for removal (garbage collection), and the replica flag to yes in the BKK

If the checksum is not correct, the file is removed from CERN-RAW, the status set to 'Failed' and a retransfer request is put to the ReqManager (this last part will change soon because does not make sense).

In case of any error in the process (cannot get metadata, cannot send request, etc), no change is done to the RAWIntegrityDB concerning that file, and it will be part of the next cycle.
