# dirac job created by ganga 
from LHCbDIRAC.Interfaces.API.LHCbJob import LHCbJob
from LHCbDIRAC.Interfaces.API.DiracLHCb import DiracLHCb
j = LHCbJob()
dirac = DiracLHCb()

# default commands added by ganga
j.setName("DVT4mc05GeV2__{Ganga_DaVinci_(184)_v32r2p1}")
j.setInputSandbox(['/afs/cern.ch/user/o/ookhrime/gangadir/shared/ookhrime/conf-c5beccc1-1b8c-43f6-a0d5-7a2025c2023a/inputsandbox/_input_sandbox_conf-c5beccc1-1b8c-43f6-a0d5-7a2025c2023a.tgz', '/afs/cern.ch/user/o/ookhrime/gangadir/workspace/ookhrime/LocalXML/184/input/_input_sandbox_184.tgz', '/afs/cern.ch/user/o/ookhrime/gangadir/workspace/ookhrime/LocalXML/184/input/dirac-script.py'])
j.setOutputSandbox(['__parsedxmlsummary__', 'summary.xml', 'DVHistos_4.root'])
j.setApplicationScript("DaVinci","v32r2p1","/afs/cern.ch/user/o/ookhrime/gangadir/workspace/ookhrime/LocalXML/184/input/gaudi-script.py",logFile="Ganga_DaVinci_v32r2p1.log")
j.setInputData(['/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000020_1.dimuon.dst', '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000007_1.dimuon.dst', '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000046_1.dimuon.dst', '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000069_1.dimuon.dst', '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000033_1.dimuon.dst', '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000076_1.dimuon.dst', '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000059_1.dimuon.dst'])
j.setAncestorDepth(0)
j.setSystemConfig('x86_64-slc5-gcc46-opt')

# <-- user settings 
j.setCPUTime(172800)
# user settings -->


# submit the job to dirac
result = dirac.submit(j)
