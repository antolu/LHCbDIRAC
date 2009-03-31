########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Client/ProductionTemplates.py,v 1.1 2009/03/31 22:39:32 paterson Exp $
# File :   ProductionTemplates.py
# Author : Stuart Paterson
########################################################################

""" A collection of production templates.  In the first iteration
    (at 00:30 ;) this should be copied to the directory of choice
    and executed as a python script.

"""

__RCSID__ = "$Id: ProductionTemplates.py,v 1.1 2009/03/31 22:39:32 paterson Exp $"

import os

try:
  from LHCbSystem.Client.Production import *
except Exception,x:
  from DIRAC.LHCbSystem.Client.Production import *

from DIRAC import gLogger

gLogger.info('Creating worklfows for simulation productions in:\n%s' %os.getcwd())

gaussGun = Production()
gaussGun.setProdType('MCSimulation')
gaussGun.setWorkflowName('Production_GaussGun')
gaussGun.setWorkflowDescription('An example of Gauss particle guns.')
gaussGun.setBKParameters('test','2008','MC-Test-v1','Beam7TeV-VeloClosed-MagDown')
gaussGun.setDBTags('sim-20090112','head-20090112')
gaussOpts = 'Gauss-2008.py;Beam7TeV-VeloClosed-MagDown.py;$DECFILESROOT/options/@{eventType}.opts'
gaussGun.addGaussStep('v36r2','57000001','Pythia','2',gaussOpts,'AppConfig.v2r2')
gaussGun.addFinalizationStep(sendBookkeeping=True,uploadData=True,uploadLogs=True,sendFailover=True)
gaussGun.banTier1s()
gaussGun.setWorkflowLib('v9r9')
gaussGun.setFileMask('sim;root')
gaussGun.createWorkflow()

gaussBoole = Production()
gaussBoole.setProdType('MCSimulation')
gaussBoole.setWorkflowName('Production_GaussBoole')
gaussBoole.setWorkflowDescription('An example of Gauss + Boole, saving all outputs.')
gaussBoole.setBKParameters('test','2008','MC-Test-v1','Beam7TeV-VeloClosed-MagDown')
gaussBoole.setDBTags('sim-20090112','head-20090112')
gaussOpts = 'Gauss-2008.py;Beam7TeV-VeloClosed-MagDown.py;$DECFILESROOT/options/@{eventType}.opts'
gaussBoole.addGaussStep('v36r2','57000001','Pythia','2',gaussOpts,'AppConfig.v2r2')
gaussBoole.addBooleStep('v17r2p1','digi','30000000','Boole-2008.py','AppConfig.v2r2')
gaussBoole.addFinalizationStep(sendBookkeeping=True,uploadData=True,uploadLogs=True,sendFailover=True)
gaussBoole.banTier1s()
gaussBoole.setWorkflowLib('v9r9')
gaussBoole.setFileMask('sim;digi;root')
gaussBoole.createWorkflow()

gaussBooleBrunel = Production()
gaussBooleBrunel.setProdType('MCSimulation')
gaussBooleBrunel.setWorkflowName('Production_GaussBooleBrunel')
gaussBooleBrunel.setWorkflowDescription('An example of Gauss + Boole, saving all outputs.')
gaussBooleBrunel.setBKParameters('test','2008','MC-Test-v1','Beam7TeV-VeloClosed-MagDown')
gaussBooleBrunel.setDBTags('sim-20090112','head-20090112')
gaussOpts = 'Gauss-2008.py;Beam7TeV-VeloClosed-MagDown.py;$DECFILESROOT/options/@{eventType}.opts'
gaussBooleBrunel.addGaussStep('v36r2','57000001','Pythia','2',gaussOpts,'AppConfig.v2r2')
gaussBooleBrunel.addBooleStep('v17r2p1','digi','30000000','Boole-2008.py','AppConfig.v2r2')
gaussBooleBrunel.addBrunelStep('v34r1p1','dst','57000001','Brunel-2008-MC.py','',inputDataType='digi')
gaussBooleBrunel.addFinalizationStep(sendBookkeeping=True,uploadData=True,uploadLogs=True,sendFailover=True)
gaussBooleBrunel.banTier1s()
gaussBooleBrunel.setWorkflowLib('v9r9')
gaussBooleBrunel.setFileMask('sim;digi;dst;root')
gaussBooleBrunel.createWorkflow()

gaussSpillover = Production()
gaussSpillover.setProdType('MCSimulation')
gaussSpillover.setWorkflowName('Production_GaussSpillover')
gaussSpillover.setWorkflowDescription('An example of Gauss + Boole, saving all outputs.')
gaussSpillover.setBKParameters('test','2008','MC-Test-v1','Beam7TeV-VeloClosed-MagDown')
gaussSpillover.setDBTags('sim-20090112','head-20090112')
gaussOpts = 'Gauss-2008.py;Beam7TeV-VeloClosed-MagDown.py;$DECFILESROOT/options/@{eventType}.opts'
gaussSpillover.addGaussStep('v36r2','57000001','Pythia','2',gaussOpts,'AppConfig.v2r2')
gaussSpillover.addGaussStep('v36r2','30000000','Pythia','2',gaussOpts,'AppConfig.v2r2')
gaussSpillover.addGaussStep('v36r2','30000000','Pythia','2',gaussOpts,'AppConfig.v2r2')
gaussSpillover.addBooleStep('v17r2p1','digi','57000001',"Boole-2008.py",'AppConfig.v2r2',spillover=True,pileup=True)
gaussSpillover.addBrunelStep('v34r1p1','dst','57000001','Brunel-2008-MC.py','AppConfig.v2r2',inputDataType='digi')
gaussSpillover.addFinalizationStep(sendBookkeeping=True,uploadData=True,uploadLogs=True,sendFailover=True)
gaussSpillover.banTier1s()
gaussSpillover.setWorkflowLib('v9r9')
gaussSpillover.setFileMask('sim;digi;dst;root')
gaussSpillover.createWorkflow()

gLogger.info('Creating test script for running XML files locally (in the DIRAC environment)')
gLogger.info('Usage: python runLocalWorkflow.py <XML FILE NAME>')
fopen = open('runLocalWorkflow.py','w')
fopen.write("""import sys
xmlfile = str(sys.argv[1])
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.LHCbSystem.Client.LHCbJob import LHCbJob
d = Dirac()
j = LHCbJob(xmlfile)
print d.submit(j,mode='local')
""")

