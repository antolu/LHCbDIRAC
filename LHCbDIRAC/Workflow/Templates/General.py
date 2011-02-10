########################################################################
# $HeadURL$
########################################################################

"""  The General.py Template will try to create a general template, to be used for all kind of requests
     
"""

__RCSID__ = "$Id$"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################
import string
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

import DIRAC

from DIRAC import gLogger, gConfig

from LHCbDIRAC.NewBookkeepingSystem.Client.BookkeepingClient import BookkeepingClient

BKClient = BookkeepingClient()


#gLogger = gLogger.getSubLogger( 'MC_Simulation_run.py' )

#################################################################################
# Below here is the actual production API script with notes
#################################################################################
from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

###########################################
# Configurable and fixed parameters
###########################################

appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. slc4_ia32_gcc34#x86_64-slc5-gcc43-opt}}'
destination = '{{WorkflowDestination#GENERAL: Workflow destination site e.g. LCG.CERN.ch/ALL/NoTier1#ALL}}'
publishFlag = '{{WorkflowTestFlag#GENERAL: Publish production to the production system True/False#True}}'
testFlag = '{{WorkflowTestProduction#GENERAL: Testing flag, e.g. for certification True/False#False}}'

configName = '{{BKConfigName#GENERAL: BK configuration name e.g. MC #MC}}'
configVersion = '{{BKConfigVersion#GENERAL: BK configuration version e.g. MC09, 2009, 2010#2010}}'

###FROM MC
#banTier1s = '{{WorkflowBanTier1s#GENERAL: Workflow ban Tier-1 sites for jobs Boolean True/False#True}}'
outputFileMask = '{{WorkflowOutputDataFileMask#GENERAL: Workflow file extensions to save (comma separated) e.g. DST,DIGI#DST}}'
outputsCERN = '{{WorkflowCERNOutputs#GENERAL: Workflow upload workflow output to CERN#False}}'
sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc43-opt#ANY}}'

events = '{{MCNumberOfEvents#PROD-MC: Number of events per job#1000}}'
cpu = '{{MCMaxCPUTime#PROD-MC: Max CPU time in secs#1000000}}'
priority = '{{MCPriority#PROD-MC: Production priority#4}}'
extend = '{{MCExtend#PROD-MC: extend production by this many jobs#100}}'
finalAppType = '{{MCFinalAppType#PROD-MC: final file type to produce and merge e.g. DST,XDST,GEN,SIM...#DST}}'

mergingFlag = '{{MergingEnable#PROD-Merging: enable flag Boolean True/False#True}}' #True/False
mergingPlugin = '{{MergingPlugin#PROD-Merging: plugin e.g. Standard, BySize#BySize}}'
mergingGroupSize = '{{MergingGroupSize#PROD-Merging: Group Size e.g. BySize = GB file size#5}}'
mergingPriority = '{{MergingPriority#PROD-Merging: Job Priority e.g. 8 by default#8}}'

transformationFlag = '{{TransformationEnable#PROD-Replication: flag Boolean True/False#True}}'

evtType = '{{eventType}}'
#Often MC requests are defined with many subrequests but we want to retain
#the parent ID for viewing on the production monitoring page. If a parent
#request is defined then this is used.
requestID = '{{ID}}'
parentReq = '{{_parent}}'
eventNumberTotal = '{{EventNumberTotal}}'

if not parentReq:
  parentReq = requestID



##FROM ReStripping 

#stripp params
stripping_priority = '{{priority#PROD-Stripping: priority#7}}'
strippCPU = '{{StrippMaxCPUTime#PROD-Stripping: Max CPU time in secs#1000000}}'
strippPlugin = '{{StrippPluginType#PROD-Stripping: plugin name#Standard}}'
strippAncestorProd = '{{StrippAncestorProd#PROD-Stripping: ancestor production if any#0}}'
strippDataSE = '{{StrippDataSE#PROD-Stripping: Output Data Storage Element#Tier1-RDST}}'
strippFilesPerJob = '{{StrippFilesPerJob#PROD-Stripping: Group size or number of files per job#1}}'
strippFileMask = '{{StrippOutputDataFileMask#PROD-Stripping: file extns to save (comma separated)#DST,ROOT}}'
stripping_transformationFlag = '{{StrippTransformation#PROD-Stripping: distribute output data True/False (False if merging)#False}}'
strippStartRun = '{{StrippRunStart#PROD-Stripping: run start, to set the start run#0}}'
strippEndRun = '{{StrippRunEnd#PROD-Stripping: run end, to set the end of the range#0}}'
unmergedStreamSE = '{{StrippStreamSE#PROD-Stripping: unmerged stream SE#Tier1-DST}}'




BKClient.getStepOutputFiles( step )
