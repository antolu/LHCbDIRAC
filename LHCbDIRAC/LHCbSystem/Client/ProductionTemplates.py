########################################################################
# $Header: /tmp/libdirac/tmp.stZoy15380/dirac/DIRAC3/LHCbSystem/Client/ProductionTemplates.py,v 1.2 2009/04/05 15:20:41 paterson Exp $
# File :   ProductionTemplates.py
# Author : Stuart Paterson
########################################################################

""" A collection of production templates.  In the first iteration
    (at 00:30 ;) this should be copied to the directory of choice
    and executed as a python script.

    MCSimulation and reconstruction workflows are currently supported.

    Notes:

     - A local testing script (XML) is written by the below Production objects
     - To perform a test that writes files etc. must set JOBID env var to an integer

    History:

    4/4/9 - Updated interface
            - histogram files are not stored by default e.g. histograms = False
            - for event types defaults to first step value unless explicitly specified
            - API version stored in workflow
            - production creation with BK pass publishing

    31/3/9 - Initial iteration supporting MC simulation workflows
"""

__RCSID__ = "$Id: ProductionTemplates.py,v 1.2 2009/04/05 15:20:41 paterson Exp $"

import os

try:
  from LHCbSystem.Client.Production import *
except Exception,x:
  from DIRAC.LHCbSystem.Client.Production import *

from DIRAC import gLogger

#############################################################################
#Simulation workflow templates
#############################################################################
gLogger.info('Creating worklfows for simulation productions in:\n%s' %os.getcwd())

#############################################################################
gaussGun = Production()
gaussGun.setProdType('MCSimulation')
gaussGun.setWorkflowName('Production_GaussGun')
gaussGun.setWorkflowDescription('An example of Gauss particle guns.')
gaussGun.setBKParameters('test','2008','MC-Test-v1','Beam7TeV-VeloClosed-MagDown')
gaussGun.setDBTags('sim-20090112','head-20090112')
gaussOpts = 'Gauss-2008.py;Beam7TeV-VeloClosed-MagDown.py;$DECFILESROOT/options/@{eventType}.opts'
gaussGun.addGaussStep('v36r2','Pythia','2',gaussOpts,eventType='57000001',extraPackages='AppConfig.v2r2')
gaussGun.addFinalizationStep(sendBookkeeping=True,uploadData=True,uploadLogs=True,sendFailover=True)
gaussGun.banTier1s()
gaussGun.setWorkflowLib('v9r9')
gaussGun.setFileMask('sim')
gaussGun.createWorkflow()

#############################################################################
gaussBoole = Production()
gaussBoole.setProdType('MCSimulation')
gaussBoole.setWorkflowName('Production_GaussBoole')
gaussBoole.setWorkflowDescription('An example of Gauss + Boole, saving all outputs.')
gaussBoole.setBKParameters('test','2008','MC-Test-v1','Beam7TeV-VeloClosed-MagDown')
gaussBoole.setDBTags('sim-20090112','head-20090112')
gaussOpts = 'Gauss-2008.py;Beam7TeV-VeloClosed-MagDown.py;$DECFILESROOT/options/@{eventType}.opts'
gaussBoole.addGaussStep('v36r2','Pythia','2',gaussOpts,eventType='57000001',extraPackages='AppConfig.v2r2')
gaussBoole.addBooleStep('v17r2p1','digi','Boole-2008.py',extraPackages='AppConfig.v2r2')
gaussBoole.addFinalizationStep(sendBookkeeping=True,uploadData=True,uploadLogs=True,sendFailover=True)
gaussBoole.banTier1s()
gaussBoole.setWorkflowLib('v9r9')
gaussBoole.setFileMask('sim;digi')
gaussBoole.createWorkflow()

#############################################################################
gaussBooleBrunel = Production()
gaussBooleBrunel.setProdType('MCSimulation')
gaussBooleBrunel.setWorkflowName('Production_GaussBooleBrunel')
gaussBooleBrunel.setWorkflowDescription('An example of Gauss + Boole, saving all outputs.')
gaussBooleBrunel.setBKParameters('test','2008','MC-Test-v1','Beam7TeV-VeloClosed-MagDown')
gaussBooleBrunel.setDBTags('sim-20090112','head-20090112')
gaussOpts = 'Gauss-2008.py;Beam7TeV-VeloClosed-MagDown.py;$DECFILESROOT/options/@{eventType}.opts'
gaussBooleBrunel.addGaussStep('v36r2','Pythia','2',gaussOpts,eventType='57000001',extraPackages='AppConfig.v2r2')
gaussBooleBrunel.addBooleStep('v17r2p1','digi','Boole-2008.py',extraPackages='AppConfig.v2r2')
gaussBooleBrunel.addBrunelStep('v34r1p1','dst','Brunel-2008-MC.py',extraPackages='AppConfig.v2r2',inputDataType='digi')
gaussBooleBrunel.addFinalizationStep(sendBookkeeping=True,uploadData=True,uploadLogs=True,sendFailover=True)
gaussBooleBrunel.banTier1s()
gaussBooleBrunel.setWorkflowLib('v9r9')
gaussBooleBrunel.setFileMask('sim;digi;dst')
gaussBooleBrunel.createWorkflow()

#############################################################################
gaussSpillover = Production()
gaussSpillover.setProdType('MCSimulation')
gaussSpillover.setWorkflowName('Production_GaussSpillover')
gaussSpillover.setWorkflowDescription('An example of Gauss + Boole, saving all outputs.')
gaussSpillover.setBKParameters('test','2008','MC-Test-v1','Beam7TeV-VeloClosed-MagDown')
gaussSpillover.setDBTags('sim-20090112','head-20090112')
gaussOpts = 'Gauss-2008.py;Beam7TeV-VeloClosed-MagDown.py;$DECFILESROOT/options/@{eventType}.opts'
gaussSpillover.addGaussStep('v36r2','Pythia','2',gaussOpts,eventType='57000001',extraPackages='AppConfig.v2r2')
gaussSpillover.addGaussStep('v36r2','Pythia','2',gaussOpts,eventType='30000000',extraPackages='AppConfig.v2r2')
gaussSpillover.addGaussStep('v36r2','Pythia','2',gaussOpts,eventType='30000000',extraPackages='AppConfig.v2r2')
gaussSpillover.addBooleStep('v17r2p1','digi','Boole-2008.py',extraPackages='AppConfig.v2r2',spillover=True,pileup=True)
gaussSpillover.addBrunelStep('v34r1p1','dst','Brunel-2008-MC.py',extraPackages='AppConfig.v2r2',inputDataType='digi')
gaussSpillover.addFinalizationStep(sendBookkeeping=True,uploadData=True,uploadLogs=True,sendFailover=True)
gaussSpillover.banTier1s()
gaussSpillover.setWorkflowLib('v9r9')
gaussSpillover.setFileMask('sim;digi;dst')
gaussSpillover.createWorkflow()

#############################################################################
#Data Reconstruction workflow templates (initially FULL FEST and EXPRESS Stream)
#############################################################################

gLogger.info('Creating worklfows for reconstruction productions in:\n%s' %os.getcwd())

#############################################################################
expressStream = Production()
expressStream.setProdType('DataReconstruction')
expressStream.setWorkflowName('Production_EXPRESS_FEST')
expressStream.setWorkflowDescription('An example of the FEST EXPRESS stream production')
expressStream.setBKParameters('Fest','Fest','FEST-Reco-v1','DataTaking6153')
expressStream.setDBTags('head-20090112','head-20090112')
brunelOpts = '$APPCONFIGOPTS/Brunel/FEST-200903.py' #;$APPCONFIGOPTS/UseOracle.py'
brunelEventType = '91000000'
brunelData='LFN:/lhcb/data/2009/RAW/EXPRESS/FEST/FEST/44878/044878_0000000002.raw'
brunelSE='CERN-RDST'
expressStream.addBrunelStep('v34r2','rdst',brunelOpts,extraPackages='AppConfig.v2r2',eventType=brunelEventType,inputData=brunelData,inputDataType='mdf',outputSE=brunelSE,histograms=True)
dvOpts = '$APPCONFIGOPTS/DaVinci/DVMonitorDst.py'
expressStream.addDaVinciStep('v22r1','dst',dvOpts,extraPackages='AppConfig.v2r2',histograms=True)
expressStream.addFinalizationStep(sendBookkeeping=True,uploadData=True,uploadLogs=True,sendFailover=True)
expressStream.setWorkflowLib('v9r9')
expressStream.setFileMask('rdst;root')
expressStream.setProdPriority('9')
expressStream.createWorkflow()

#############################################################################
fullStream = Production()
fullStream.setProdType('DataReconstruction')
fullStream.setWorkflowName('Production_FULL_FEST')
fullStream.setWorkflowDescription('An example of the FEST EXPRESS stream production')
fullStream.setBKParameters('Fest','Fest','FEST-Reco-v1','DataTaking6153')
fullStream.setDBTags('head-20090112','head-20090112')
brunelOpts = '$APPCONFIGOPTS/Brunel/FEST-200903.py' #;$APPCONFIGOPTS/UseOracle.py'
brunelEventType = '90000000'
brunelData='LFN:/lhcb/data/2009/RAW/FULL/FEST/FEST/43041/043041_0000000002.raw'
fullStream.addBrunelStep('v34r2','rdst',brunelOpts,extraPackages='AppConfig.v2r2',eventType=brunelEventType,inputData=brunelData,inputDataType='mdf',histograms=True)
dvOpts = '$APPCONFIGOPTS/DaVinci/DVMonitorDst.py'
fullStream.addDaVinciStep('v22r1','dst',dvOpts,extraPackages='AppConfig.v2r2',histograms=True)
fullStream.addFinalizationStep(sendBookkeeping=True,uploadData=True,uploadLogs=True,sendFailover=True)
fullStream.setWorkflowLib('v9r9')
fullStream.setFileMask('rdst;root')
fullStream.setProdPriority('8')
fullStream.createWorkflow()

#############################################################################
#Local testing script
#############################################################################

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

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#