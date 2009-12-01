from Gaudi.Configuration import *
from GaudiConf.Configuration import *

from Configurables import LoadDDDB
from Configurables import COOLConfSvc
COOLConfSvc(UseLFCReplicaSvc = True)



# ---------- option to use Oracle CondDB instead of SQLDDDB
LHCbApp().useOracleCondDB = True
LHCbApp().DDDBtag = LHCbApp().condDBtag = '2008-default'

importOptions( "$DDDBROOT/options/DDDB.py" )

CondDBAccessSvc("ONLINE",
                ConnectionString = "sqlite_file:$SQLDDDBROOT/db/ONLINE-200808.db/ONLINE")

ApplicationMgr().EvtSel     = "NONE"
ApplicationMgr().EvtMax     = 1

ApplicationMgr().TopAlg  = [ "GaudiSequencer" ]
GaudiSequencer().Members += [ "LoadDDDB" ]
GaudiSequencer().MeasureTime = True

# ---------- option to select only a subtree
#LoadDDDB().Node = "/dd/Geometry*"


LHCbApp().applyConf()
