======
HOW-TO
======

Remove files (triggered by Vlad)
================================

From lbboinc01.cern.ch

Using a valid Proxy:

	- ``dirac-dms-remove-files -o "/DIRAC/Security/UseServerCertificate=No" <LFN>``

this will remove file(s) both from SE and catalog.

If the file(s) is not registered in the catalog but exists on SE (or for checking if it does exist in the SE)
	- use the script ``remove-LFN-from-SE.py``

If the file(s) is not in the SE but it is registered in the catalog:
	- use the script ``remove-LFN-from-DFC.py``


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


remove-LFN-from-DFC.py
^^^^^^^^^^^^^^^^^^^^^^
::

    from DIRAC.Core.Base.Script import parseCommandLine
    parseCommandLine()
    from DIRAC.Resources.Catalog.FileCatalog import FileCatalog
    lfns = [<list of LFNs>]
    fc = FileCatalog()
    fc.removeFile(lfns)
