#Ganga# File created by Ganga - Thu Nov 22 01:28:37 2012
#Ganga#
#Ganga# Object properties may be freely edited before reloading into Ganga
#Ganga#
#Ganga# Lines beginning #Ganga# are used to divide object definitions,
#Ganga# and must not be deleted

#Ganga# Job object (category: jobs)
Job (
 comment = '' ,
 do_auto_resubmit = False ,
 name = 'DVT6100' ,
 outputsandbox = [] ,
 backend = Dirac (
    settings = {'CPUTime': 172800} ,
    diracOpts = '' ,
    inputSandboxLFNs = [ ] 
    ) ,
 merger = None ,
 application = DaVinci (
    is_prepared = None ,
    extraopts = None ,
    args = ['-T'] ,
    package = 'Phys' ,
    platform = 'x86_64-slc5-gcc46-opt' ,
    version = 'v32r2p1' ,
    setupProjectOptions = '' ,
    masterpackage = None ,
    user_release_area = '/afs/cern.ch/user/o/ookhrime/cmtuser' ,
    optsfile = [ File (
       name = '/afs/cern.ch/user/o/ookhrime/cmtuser/DaVinci_v32r2p1/Tutorial/Analysis/solutions/DaVinci6/DVTutorial_6.py' ,
       subdir = '.' 
       ) , ] 
    ) ,
 inputdata = LHCbDataset (
    depth = 0 ,
    persistency = None ,
    files = [ LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000069_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000105_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000118_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000170_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000059_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000178_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000124_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000097_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000199_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000138_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000153_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000144_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000090_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000193_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000165_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000033_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000076_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000148_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000189_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000020_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000122_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000046_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000195_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000143_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000203_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000163_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000132_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000151_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000126_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000007_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000183_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000120_1.dimuon.dst' 
       ) , LogicalFile (
       name = '/lhcb/LHCb/Collision12/DIMUON.DST/00020241/0000/00020241_00000174_1.dimuon.dst' 
       ) , ] ,
    XMLCatalogueSlice = File (
       name = '' ,
       subdir = '.' 
       ) 
    ) ,
 outputfiles = [ ] ,
 info = JobInfo (
    monitor = None 
    ) ,
 splitter = None ,
 inputsandbox = [ ] ,
 outputdata = None 
 ) 

