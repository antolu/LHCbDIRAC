########################################################################
# $HeadURL: svn+ssh://svn.cern.ch/reps/dirac/LHCbDIRAC/trunk/LHCbDIRAC/Workflow/Templates/MC_Simulation_run.py $
########################################################################

"""  The MC Simulation Template creates workflows for the following simulation
     use-cases:
     
     - Gauss [ + Merging, + Transformation ]
     - Gauss -> Boole [ + Merging, + Transformation ]
     - Gauss -> Boole -> Brunel [ + Merging, + Transformation ]
     - Gauss -> Boole -> Moore -> Brunel [ + Merging, + Transformation ]
     
     with the following parameters being configurable via the interface:
     
     - number of events
     - CPU time in seconds
     - final output file type 
     - number of jobs to extend the initial MC production 
     - resulting WMS priority for each job
     - BK config name and version
     - system configuration
     - output file mask (e.g. to retain intermediate step output)
     - merging priority, plugin and group size
     - whether or not to ban Tier-1 sites as destination
     - whether or not a merging production should be created
     - whether or not a transformation should be created
     - whether outputs should be uploaded to CERN only for testing
     - string to append to the production name
     
     The template explicitly forces the tags of the CondDB / DDDB to be set
     at each step.  
     
"""

__RCSID__ = "$Id:$"

#################################################################################
# Some import statements and standard DIRAC script preamble
#################################################################################
import string
from DIRAC.Core.Base import Script
Script.parseCommandLine()
args = Script.getPositionalArgs()

import DIRAC

from DIRAC import gLogger, gConfig
gLogger = gLogger.getSubLogger( 'MC_Simulation_run.py' )

#################################################################################
# Below here is the actual production API script with notes
#################################################################################
from LHCbDIRAC.Interfaces.API.Production import Production
from LHCbDIRAC.Interfaces.API.DiracProduction import DiracProduction

###########################################
# Configurable and fixed parameters
###########################################

appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

certificationFlag = '{{certificationFLAG#GENERAL: Set True for certification test#False}}'
localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'

configName = '{{BKConfigName#GENERAL: BK configuration name e.g. MC #MC}}'
configVersion = '{{BKConfigVersion#GENERAL: BK configuration version e.g. MC09, 2009, 2010#MC10}}'

banTier1s = '{{WorkflowBanTier1s#GENERAL: Workflow ban Tier-1 sites for jobs Boolean True/False#True}}'
outputFileMask = '{{WorkflowOutputDataFileMask#GENERAL: Workflow file extensions to save (comma separated) e.g. DST,DIGI#ALLSTREAMS.DST}}'
outputsCERN = '{{WorkflowCERNOutputs#GENERAL: Workflow upload workflow output to CERN#False}}'
sysConfig = '{{WorkflowSystemConfig#GENERAL: Workflow system config e.g. x86_64-slc5-gcc43-opt, ANY#slc4_ia32_gcc34}}'

events = '{{MCNumberOfEvents#PROD-MC: Number of events per job#1000}}'
cpu = '{{MCMaxCPUTime#PROD-MC: Max CPU time in secs#1000000}}'
priority = '{{MCPriority#PROD-MC: Production priority#4}}'
extend = '{{MCExtend#PROD-MC: extend production by this many jobs#100}}'
finalAppType = '{{MCFinalAppType#PROD-MC: final file type to produce and merge e.g. DST,XDST,GEN,SIM...#ALLSTREAMS.DST}}'
defaultOutput = '{{DefaultOutput#Prod-MC: upload un-merged output (leave blank for default)#}}'

mergingFlag = '{{MergingEnable#PROD-Merging: enable flag Boolean True/False#True}}' #True/False
mergingPlugin = '{{MergingPlugin#PROD-Merging: plugin e.g. Standard, BySize#BySize}}'
mergingGroupSize = '{{MergingGroupSize#PROD-Merging: Group Size e.g. BySize = GB file size#5}}'
mergingPriority = '{{MergingPriority#PROD-Merging: Job Priority e.g. 8 by default#8}}'


replicationFlag = '{{TransformationEnable#PROD-Replication: flag Boolean True/False#True}}'
replicationPlugin = '{{ReplicationPlugin#PROD-Replication: ReplicationPlugin#LHCbMCDSTBroadcast}}'

evtType = '{{eventType}}'
#Often MC requests are defined with many subrequests but we want to retain
#the parent ID for viewing on the production monitoring page. If a parent
#request is defined then this is used.
requestID = '{{ID}}'
parentReq = '{{_parent}}'
eventNumberTotal = '{{EventNumberTotal}}'

if not parentReq:
  parentReq = requestID

###########################################
# LHCb conventions implied by the above
###########################################

replicationFlag = eval( replicationFlag )
mergingFlag = eval( mergingFlag )
banTier1s = eval( banTier1s )
outputsCERN = eval( outputsCERN )
certificationFlag = eval( certificationFlag )
localTestFlag = eval( localTestFlag )

stepsList = []
for i in range(1,8):
  stepsList.append(int('{{p%dStep}}' %i))
  