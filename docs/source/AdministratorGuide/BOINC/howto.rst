======
HOW-TO
======

Process Completed Jobs
======================

From the gateway (``lbboinc01.cern.ch``) have a look at the requests

   - ``dirac-rms-request --Job <JobIDs>``
   - check if output files are registered and/or do exists at SE with ``dirac-dms-pfn-exists <LFNs>``
   - check if output files that do not exists @CERN have descendants with (on lxplus) ``dirac-bookkeeping-get-file-descendants --All --Depth=100 <LFNs>``
   - if files have already descendants
      * update the job status to 'Done'
      * remove the files @BOINC-SE-01 if needed with
        ``dirac-dms-remove-files -o "/DIRAC/Security/UseServerCertificate=No" <LFN>``,
        using a valid Proxy (must be in ``/tmp/x509up_uNNNNN`` and exported to the env ``export X509_USER_PROXY=/tmp/x509up_uNNNNN``)
   - if files are @CERN
      * update the job status to 'Done'
      * DO NOT REMOVE the files!
   - if files have neither descendants nor are @CERN nor @BOINC-SE
      * KILL the job
   - dispose the LOG files properly (simply remove them if still registered @BOINC)



Remove files (triggered by Vlad)
================================

From lbboinc01.cern.ch

   - double check if files are registered in the FC and/or do exists at the SE with
     ``dirac-dms-lfn-replicas <LFNs>`` and/or ``dirac-dms-pfn-exists <LFNs>``

   - if files do not exists (anymore) @CERN-BUFFER, double check if they have descendants with
     (on lxplus) ``dirac-bookkeeping-get-file-descendants --All --Depth=100 <LFNs>``

   - double check the status of the Job(s) and update it accordingly

   - if everything is ok, then

Using a valid Proxy (must be in ``/tmp/x509up_uNNNNN`` and exported to the env ``export X509_USER_PROXY=/tmp/x509up_uNNNNN``):

   - ``dirac-dms-remove-files -o "/DIRAC/Security/UseServerCertificate=No" <LFN>``

this will remove file(s) both from SE and catalog.

If the file(s) is not registered in the catalog but exists on SE (or for checking if it does exist in the SE)
   - use the script ``remove-LFN-from-SE.py``


remove-LFN-from-SE.py
^^^^^^^^^^^^^^^^^^^^^
::

   from DIRAC.Core.Base.Script import parseCommandLine
   parseCommandLine()
   from DIRAC.Resources.Storage.StorageElement import StorageElement
   se = StorageElement('BOINC-SE-01')
   lfns = [<list of LFNs>]
   #se.exists(lfns)
   #se.removeFile(lfns)

