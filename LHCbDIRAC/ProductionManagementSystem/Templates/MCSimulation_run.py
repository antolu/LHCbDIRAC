"""  The MC Simulation Template creates workflows for the following simulation
     use-cases:
      WORKFLOW1: Simulation+Selection+Merge
      WORKFLOW2: Simulation+Selection+MCMerge
      WORKFLOW3: Simulation+Selection
      WORKFLOW4: Simulation+MCMerge
      WORKFLOW5: Simulation

    Exotic things you might want to do:
    * run a local test:
      - of the MC: just set the localTestFlag to True
      - of the merging/stripping: set pr.prodsToLaunch to, e.g., [2], and adjust the pr.inputs at the end of the script
    * run only part of the request on the Grid:
      - for the MC: just set pr.prodsToLaunch = [1]
      - for the merge and/or stripping: set pr.prodsToLaunch, then set pr.previousProdID
"""

import ast

from DIRAC.Core.Base import Script
Script.parseCommandLine()

from DIRAC import gConfig, gLogger, exit as DIRACexit
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest import ProductionRequest

__RCSID__ = "$Id$"

#
# Link compression level to the output file visibility. If any file types is visible, the corresponding
# step takes the maximum compression level
# Input : dicts including the visibility of output files per step
# Output: modified list of compression levels
#
def modifyCompression( dict1, dict2, lis1 ):

  for k, v in dict1.items():
    if v == 'Y':
      lis1[int( k ) - 1] = 'HIGH'

    else:
      if k in dict2.keys():
        for _, v2 in dict2[k].items():
          if v2 == 'Y':
            lis1[int( k ) - 1] = 'HIGH'
  return lis1

#
# Simple "check" in case the visibility includes only one element
# Input : dict of visibility
# Output: modified dict of visibility
#
def fillVisList( vdict, num ):

  # Assuming that, if there's only one element in the list of output visibility flags, every step will catch that flag
  if len( vdict ) == 1:
    # '1' key is given by default by the template
    val = vdict['1']
    vdict = dict( [( str( i ), val ) for i in range(num[0] + 1) if i] )
  # Another assumption: if the number of steps is bigger than that of vis flags, then extend the list with the last flag available
  # to fill the "holes"
  # if len(vlist) < len(slist):
  #  vlist.extend( vlist[-1] * (len(slist) - len(vlist)) )

  return vdict

gLogger = gLogger.getSubLogger( 'MCSimulation_run.py' )
currentSetup = gConfig.getValue( 'DIRAC/Setup' )

pr = ProductionRequest()

stepsList = ['{{p1Step}}']
stepsList.append( '{{p2Step}}' )
stepsList.append( '{{p3Step}}' )
stepsList.append( '{{p4Step}}' )
stepsList.append( '{{p5Step}}' )
stepsList.append( '{{p6Step}}' )
stepsList.append( '{{p7Step}}' )
stepsList.append( '{{p8Step}}' )
stepsList.append( '{{p9Step}}' )
stepsList.append( '{{p10Step}}' )
stepsList.append( '{{p11Step}}' )
stepsList.append( '{{p12Step}}' )
stepsList.append( '{{p13Step}}' )
stepsList.append( '{{p14Step}}' )
stepsList.append( '{{p15Step}}' )
stepsList.append( '{{p16Step}}' )
stepsList.append( '{{p17Step}}' )
stepsList.append( '{{p18Step}}' )
stepsList.append( '{{p19Step}}' )
stepsList.append( '{{p20Step}}' )
pr.stepsList = stepsList

###########################################
# Configurable and fixed parameters
###########################################

pr.appendName = '{{WorkflowAppendName#GENERAL: Workflow string to append to production name#1}}'

w = '{{w#----->WORKFLOW: choose one below#}}'
w1 = '{{w1#-WORKFLOW1: Simulation#False}}'
w2 = '{{w2#-WORKFLOW2: Simulation(Gauss) + AllOthers#False}}'
w3 = '{{w3#-WORKFLOW3: Simulation(Gauss) + AllOthers + Merge#False}}'

localTestFlag = '{{localTestFlag#GENERAL: Set True for local test#False}}'
validationFlag = '{{validationFlag#GENERAL: Set True for validation prod - will create histograms#False}}'

pr.configName = '{{BKConfigName#GENERAL: BK configuration name e.g. MC #MC}}'
extraOptions = '{{extraOptions#GENERAL: extra options as python dict stepID:options#}}'

targets = '{{Target#PROD-1:MC: Target for MC (e.g. Tier2, ALL, LCG.CERN.cern or BAN:site1:site2#ALL}}'
eventsPerJob = '{{eventsPerJob#PROD-1:MC: Number of events per job#-1}}'
MCPriority = '{{MCPriority#PROD-1:MC: Production priority#0}}'
MCmulticoreFlag = '{{MCMulticoreFLag#PROD-1: multicore flag#True}}'
simulationCompressionLvl = '{{simulationCompressionLvl#PROD-1: Compression level#LOW}}'
simulationOutputVisFlag = ast.literal_eval( '{{simulationOutputVisFlag#PROD-1: Simulation visibility flag dictionary (one flag per step {"step":"Y|N"}) # {} }}' )
try:
  simulationOutputVisFlagSpecial = ast.literal_eval( '{{simulationOutputVisFlagSpecial#PROD-1: Special Visibility flag of output files (a dictionary {"step n":{("FType":flag)}} )#}}' )
except SyntaxError:
  simulationOutputVisFlagSpecial = {}

selectionPlugin = '{{selectionPlugin#PROD-2:Selection: plugin e.g. Standard, BySize#BySize}}'
selectionGroupSize = '{{selectionGroupSize#PROD-2:Selection: input files total size (we\'ll download)#5}}'
selectionPriority = '{{selectionPriority#PROD-2:Selection: Job Priority e.g. 8 by default#8}}'
selectionCPU = '{{selectionCPU#PROD-2:Selection: Max CPU time in secs#100000}}'
removeInputSelection = '{{removeInputSelection#PROD-2:Selection: remove inputs#True}}'
selmulticoreFlag = '{{selMulticoreFLag#PROD-2:Selection: multicore flag#True}}'
selectionCompressionLvl = '{{selectionCompressionLvl#PROD-2:Selection: Compression level#LOW}}'
selectionOutputVisFlag = ast.literal_eval( '{{selectionOutputVisFlag#PROD-2: Selection visibility flag dictionary ({"step n": "Y|N"})# {} }}' )
try:
  selectionOutputVisFlagSpecial = ast.literal_eval( '{{selectionOutputVisFlagSpecial#PROD-2: Special Visibility flag of output files (a dictionary {"step n":{"FType":flag}} )#}}' )
except SyntaxError:
  selectionOutputVisFlagSpecial = {}

mergingPlugin = '{{MergingPlugin#PROD-3:Merging: plugin e.g. Standard, BySize#BySize}}'
mergingGroupSize = '{{MergingGroupSize#PROD-3:Merging: Group Size e.g. BySize = GB file size#5}}'
mergingPriority = '{{MergingPriority#PROD-3:Merging: Job Priority e.g. 8 by default#8}}'
mergingCPU = '{{mergingCPU#PROD-3:Merging: Max CPU time in secs#100000}}'
removeInputMerge = '{{removeInputMerge#PROD-3:Merging: remove inputs#True}}'
mergemulticoreFlag = '{{mergeMulticoreFLag#PROD-3:Merging: multicore flag#True}}'

mergeCompressionLvl = '{{mergeCompressionLvl#PROD-3:Merging: Compression level#HIGH}}'
mergeOutputVisFlag = ast.literal_eval( '{{mergeOutputVisFlag#PROD-3: Merge visibility flag dictionary ({"step":"Y|N"}) # {} }}' )
try:
  mergeOutputVisFlagSpecial = ast.literal_eval( '{{mergeOutputVisFlagSpecial#PROD-3: Special Visibility flag of output files (a dictionary {"step n":{"FType":flag}} )#}}' )
except SyntaxError:
  mergeOutputVisFlagSpecial = {}

pr.configVersion = '{{mcConfigVersion}}'
pr.eventType = '{{eventType}}'
# Often MC requests are defined with many subrequests but we want to retain
# the parent ID for viewing on the production monitoring page. If a parent
# request is defined then this is used.
pr.parentRequestID = '{{_parent}}'
pr.requestID = '{{ID}}'

if extraOptions:
  pr.extraOptions = ast.literal_eval( extraOptions )
pr.prodGroup = '{{pDsc}}'
pr.dataTakingConditions = '{{simDesc}}'

MCPriority = int( MCPriority )
selectionPriority = int( selectionPriority )
mergingPriority = int( mergingPriority )

removeInputMerge = ast.literal_eval( removeInputMerge )
removeInputSelection = ast.literal_eval( removeInputSelection )

###########################################
# LHCb conventions implied by the above
###########################################

localTestFlag = ast.literal_eval( localTestFlag )
validationFlag = ast.literal_eval( validationFlag )

if localTestFlag:
  pr.testFlag = True
  pr.publishFlag = False
  pr.prodsToLaunch = [1]

pr.outConfigName = pr.configName

w1 = ast.literal_eval( w1 )
w2 = ast.literal_eval( w2 )
w3 = ast.literal_eval( w3 )

if not w1 and not w2 and not w3:
  gLogger.error( 'Vladimir, I told you to select at least one workflow!' )
  DIRACexit( 2 )

elif w1:
  pr.prodsTypeList = ['MCSimulation']
  pr.outputSEs = ['Tier1_MC-DST']

  pr.stepsInProds = [range( 1, len( pr.stepsList ) + 1 )]
  pr.removeInputsFlags = [False]
  pr.priorities = [MCPriority]
  pr.cpus = [100000]
  pr.outputFileSteps = [str( len( pr.stepsList ) )]
  pr.targets = [targets]
  pr.events = [eventsPerJob]
  pr.groupSizes = [1]
  pr.plugins = ['']
  pr.inputDataPolicies = ['']
  pr.bkQueries = ['']
  pr.multicore = [MCmulticoreFlag]

  pr.compressionLvl = [simulationCompressionLvl] * len( pr.stepsInProds[0] )
  simulationOutputVisFlag = fillVisList( simulationOutputVisFlag, pr.stepsInProds[0] )
  pr.compressionLvl = modifyCompression( simulationOutputVisFlag, simulationOutputVisFlagSpecial, pr.compressionLvl )

  pr.outputVisFlag = [simulationOutputVisFlag]
  pr.specialOutputVisFlag = [simulationOutputVisFlagSpecial]

  # pr.resolveSteps()

elif w2:
  pr.prodsTypeList = ['MCSimulation', 'MCReconstruction']
  pr.outputSEs = ['Tier1-Buffer', 'Tier1_MC-DST']

  pr.stepsInProds = [[1, ] , xrange( 2, len( pr.stepsList ) + 1 )]
  pr.outputFileSteps = [str( len( pr.stepsInProds[0] ) ),
                        str( len( pr.stepsInProds[1] ) )]

  pr.removeInputsFlags = [False, removeInputSelection]
  pr.priorities = [MCPriority, selectionPriority]
  pr.cpus = [100000, selectionCPU]
  pr.targets = [targets, '']
  pr.events = [eventsPerJob,-1]
  pr.groupSizes = [1, selectionGroupSize]
  pr.plugins = ['', selectionPlugin]
  pr.inputDataPolicies = ['', 'download']
  pr.bkQueries = ['', 'fromPreviousProd']
  pr.multicore = [MCmulticoreFlag, selmulticoreFlag]
  pr.compressionLvl = [simulationCompressionLvl] * len( pr.stepsInProds[0] ) + \
                      [selectionCompressionLvl] * len( pr.stepsInProds[1] )

  simulationOutputVisFlag = fillVisList( simulationOutputVisFlag, pr.stepsInProds[0] )
  selectionOutputVisFlag = fillVisList( selectionOutputVisFlag, pr.stepsInProds[1] )

  temp1 = simulationOutputVisFlag
  temp1.update( selectionOutputVisFlag )
  temp2 = simulationOutputVisFlagSpecial
  temp2.update( selectionOutputVisFlagSpecial )
  pr.compressionLvl = modifyCompression( temp1, temp2, pr.compressionLvl )

  # pr.compressionLvl[0] = modifyCompression(simulationOutputVisFlag, simulationOutputVisFlagSpecial, pr.compressionLvl[0])
  # pr.compressionLvl[1] = modifyCompression(selectionOutputVisFlag, selectionOutputVisFlagSpecial, pr.compressionLvl[1])

  pr.outputVisFlag = [simulationOutputVisFlag, selectionOutputVisFlag]
  pr.specialOutputVisFlag = [simulationOutputVisFlagSpecial, selectionOutputVisFlagSpecial]

  # pr.resolveSteps()

elif w3:
  pr.prodsTypeList = ['MCSimulation', 'MCReconstruction', 'MCMerge']
  pr.outputSEs = ['Tier1-Buffer', 'Tier1-Buffer', 'Tier1_MC-DST']

  pr.stepsInProds = [ [1, ], xrange( 2, len( pr.stepsList ) ), [len( pr.stepsList )]]
  pr.outputFileSteps = [ '1', str( len( pr.stepsInProds[1] ) ), '1']

  pr.removeInputsFlags = [False, removeInputSelection, removeInputMerge]
  pr.priorities = [MCPriority, selectionPriority, mergingPriority]
  pr.cpus = [100000, selectionCPU, mergingCPU]
  pr.targets = [targets, '', '']
  pr.events = [eventsPerJob,-1,-1]
  pr.groupSizes = [1, selectionGroupSize, mergingGroupSize]
  pr.plugins = ['', selectionPlugin, mergingPlugin]
  pr.inputDataPolicies = ['', 'download', 'download']
  pr.bkQueries = ['', 'fromPreviousProd', 'fromPreviousProd']
  pr.multicore = [MCmulticoreFlag, selmulticoreFlag, mergemulticoreFlag]

# Temporary solution: should depend from the output file visibility
  # pr.compressionLvl = [compressionLvlDefault]*(len( pr.stepsList )-1) + [compressionLvlLast]

  simulationOutputVisFlag = fillVisList( simulationOutputVisFlag, pr.stepsInProds[0] )
  selectionOutputVisFlag = fillVisList( selectionOutputVisFlag, pr.stepsInProds[1] )
  mergeOutputVisFlag = fillVisList( mergeOutputVisFlag, pr.stepsInProds[2] )

  temp1 = simulationOutputVisFlag
  temp1.update( selectionOutputVisFlag )
  temp1.update( mergeOutputVisFlag )
  temp2 = simulationOutputVisFlagSpecial
  temp1.update( selectionOutputVisFlagSpecial )
  temp1.update( mergeOutputVisFlagSpecial )
  pr.compressionLvl = modifyCompression( temp1, temp2, pr.compressionLvl )

  # pr.compressionLvl[0] = modifyCompression(simulationOutputVisFlag, simulationOutputVisFlagSpecial, pr.compressionLvl[0])
  # pr.compressionLvl[1] = modifyCompression(selectionOutputVisFlag, selectionOutputVisFlagSpecial, pr.compressionLvl[1])
  # pr.compressionLvl[2] = modifyCompression(mergeOutputVisFlag, mergeOutputVisFlagSpecial, pr.compressionLvl[2])

  pr.outputVisFlag = [simulationOutputVisFlag, selectionOutputVisFlag, mergeOutputVisFlag]
  pr.specialOutputVisFlag = [simulationOutputVisFlagSpecial, selectionOutputVisFlagSpecial, mergeOutputVisFlagSpecial]

# In case we want just to test, we publish in the certification/test part of the BKK
if currentSetup == 'LHCb-Certification' or pr.testFlag:
  pr.outConfigName = 'certification'
  pr.configVersion = 'test'

if pr.testFlag:
  pr.extend = '10'
  mergingGroupSize = '1'
  MCCpu = '50000'
  pr.previousProdID = 0  # set this for, e.g., launching only merging

# Validation implies few things, like saving all the outputs, and adding a GAUSSHIST
if validationFlag:
  pr.resolveSteps()
  pr.outConfigName = 'validation'
  # Adding GAUSSHIST to the list of outputs to be produced (by the first step, which is Gauss)
  if 'GAUSSHIST' not in pr.stepsListDict[0]['fileTypesOut']:
    pr.stepsListDict[0]['fileTypesOut'].append( 'GAUSSHIST' )
  pr.outputFileSteps = [''] * len( pr.prodsTypeList )


res = pr.buildAndLaunchRequest()
if not res['OK']:
  gLogger.error( "Errors with submission: %s" % res['Message'] )
  DIRACexit( 2 )
else:
  gLogger.always( "Submitted %s" % str( res['Value'] ) )
