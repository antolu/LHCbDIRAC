ONLINE steps
===================

Installation of LHCbDirac
-------------------------


files are send to $DIRAC/requestDB/transfer/Todo, the move to $DIRAC/requestDB/transfer/Assigned
The $DIRAC/DataManagement/TransferAgent will handled the request and copy the file to CASTOR.
When it is done the request is move to $DIRAC/requestDB/transfer/Done

A file registration is then issue in $DIRAC/requestDB/register/toDo Then move in $DIRAC/requestDB/register/Assigned.
The $DIRAC/DataManagement/RegsitrationAgent is run to treat the request and the registration to the various catalog is performed.
When it is done, the request is move to $DIRAC/requestDB/register/Done.


install agent DataManagement RAWIntegrityAgent
install db RAWIntegrityDB
install service DataManagement RAWIntegrity