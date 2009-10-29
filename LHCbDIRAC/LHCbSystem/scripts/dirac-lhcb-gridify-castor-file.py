#! /usr/bin/env python
from DIRAC.Core.Base                                      import Script
se = 'CERN-USER'
Script.registerSwitch( "S:", "SE=","The destination storage element. Possibilities are CERN-USER,CNAF-USER,GRIDKA-USER,IN2P3-USER,NIKHEF-USER,PIC-USER,RAL-USER. [%s]" % se)
Script.parseCommandLine(ignoreErrors = True)
castorFiles = Script.getPositionalArgs()
for switch in Script.getUnprocessedSwitches():
  if switch[0].lower() == "s" or switch[0].lower() == "SE":
    se = switch[1]
import DIRAC
from DIRAC                                                import gLogger
from DIRAC.Core.Security.Misc                             import getProxyInfo
from DIRAC.LHCbSystem.Client.LHCbAPI                      import LHCbAPI
from DIRAC.DataManagementSystem.Client.ReplicaManager     import ReplicaManager
import re,os

lhcb = LHCbAPI()
replicaManager = ReplicaManager()

res = getProxyInfo(False,False)
if not res['OK']:
  gLogger.error("Failed to get client proxy information.",res['Message'])
  DIRAC.exit(2)
proxyInfo = res['Value']
username = proxyInfo['username']
if not castorFiles:
  gLogger.info("No files suppied")
  gLogger.info("Usage: dirac-lhcb-gridify-castor-file castorpfn1 castorpfn2")
  gLogger.info("Try dirac-lhcb-gridify-castor-file  --help for options")
  DIRAC.exit(0)
exp = re.compile(r'/castor/cern.ch/user/[a-z]/[a-z]*/(\S+)$')
for physicalFile in castorFiles:
  if not physicalFile.startswith("/castor/cern.ch/user"):
    gLoger.info("%s is not a Castor user file (e.g. /castor/cern.ch/user/%s/%s). Ignored." % (physicalFile,username[0],username))
    continue
  if not re.findall(exp,physicalFile):
    gLogger.info("Failed to determine relative path for file %s. Ignored." % physicalFile)
    continue
  relativePath =  re.findall(exp,physicalFile)[0]
  gLogger.verbose("Found relative path of %s to be %s" % (physicalFile,relativePath))
  res = replicaManager.getStorageFile(physicalFile,'CERN-USER',singleFile=True)
  if not res['OK']:
    gLogger.info("Failed to get local copy of %s" % physicalFile, res['Message'])
    continue
  localFile = os.path.basename(relativePath)
  gLogger.verbose("Obtained local copy of %s at %s" % (physicalFile,localFile))
  lfn = '/lhcb/user/%s/%s/Migrated/%s' % (username[0],username,relativePath)
  res = lhcb.addRootFile(lfn,localFile,se)
  if not res['OK']:
    gLogger.error("Failed to upload %s to grid." % physicalFile,res['Message'])
    continue
  gLogger.info("Successfully uploaded %s to Grid. The corresponding LFN is %s" % (physicalFile,lfn))
DIRAC.exit(0)
