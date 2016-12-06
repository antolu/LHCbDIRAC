""" This script submits a test production
"""

import time
import os

from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from LHCbDIRAC.ProductionManagementSystem.Client.Production import Production
from LHCbDIRAC.TransformationSystem.Client.Transformation import Transformation
from LHCbDIRAC.TransformationSystem.Client.TransformationClient import TransformationClient

#Let's first create the production
prodType = 'Merge'
transName = 'testProduction_'  + str(int(time.time()))
desc = 'just test'

prod = Production()
prod.setParameter( 'eventType', 'string', 'TestEventType', 'Event Type of the production' )
prod.setParameter( 'numberOfEvents', 'string', '-1', 'Number of events requested' )
prod.setParameter( 'ProcessingType', 'JDL', str( 'Test' ), 'ProductionGroupOrType' )
prod.setParameter( 'Priority', 'JDL', str( 9 ), 'UserPriority' )
prod.setBKParameters( configName = 'outConfigName', configVersion = 'configVersion',
                      groupDescription = 'Test', conditions = 'dataTakingConditions' )
prod.LHCbJob.setType( prodType )
prod.LHCbJob.workflow.setName(transName)
prod.LHCbJob.workflow.setDescrShort( desc )
prod.LHCbJob.workflow.setDescription( desc )
prod.LHCbJob.setCPUTime( 86400 )
prod.LHCbJob.setInputDataPolicy( 'Download' )

stepsInProd = [{'StepId': 123897, 'StepName': 'MergeMDF', 'ApplicationName': 'MergeMDF', 'ApplicationVersion': '',
                'ExtraPackages': '', 'ProcessingPass': 'Merging', 'Visible': 'Y', 'Usable': 'Yes',
                'DDDB': '', 'CONDDB': '', 'DQTag': '', 'OptionsFormat': '',
                'OptionFiles': '', 'SystemConfig': '', 'mcTCK': '', 'ExtraOptions': '',
                'isMulticore': 'N',
                'fileTypesIn':['TXT'],
                'fileTypesOut':['TXT']}]
stepName = prod.addApplicationStep( stepDict = stepsInProd[0],
                                    modulesList = ['MergeMDF', 'BookkeepingReport'] )
prod.gaudiSteps.append( stepName )
prod.addFinalizationStep()

prod.LHCbJob._addParameter( prod.LHCbJob.workflow, 'gaudiSteps', 'list', prod.gaudiSteps, 'list of Gaudi Steps' )
prod.LHCbJob._addParameter( prod.LHCbJob.workflow, 'outputSEs', 'dict', {'RAW': 'CERN-BUFFER'}, 'dictionary of output SEs' )

#Let's submit the production now
#result = prod.create()


name = prod.LHCbJob.workflow.getName()
name = name.replace( '/', '' ).replace( '\\', '' )
prod.LHCbJob.workflow.toXMLFile( name )

print 'Workflow XML file name is: %s' % name

workflowBody = ''
if os.path.exists( name ):
  with open( name, 'r' ) as fopen:
    workflowBody = fopen.read()
else:
  print 'Could not get workflow body'

# Standard parameters
transformation = Transformation()
transformation.setTransformationName( name )
transformation.setTransformationGroup( 'Test' )
transformation.setDescription( desc )
transformation.setLongDescription( desc )
transformation.setType( 'Merge' )
transformation.setBody( workflowBody )
transformation.setPlugin( 'LHCbStandard' )
transformation.setTransformationFamily( 'Test' )
transformation.setGroupSize( 2 )
transformation.setOutputDirectories([ '/lhcb/outConfigName/configVersion/LOG/00000000',
                                      '/lhcb/outConfigName/configVersion/RAW/00000000',
                                      '/lhcb/outConfigName/configVersion/CORE/00000000'])

result = transformation.addTransformation()
if not result['OK']:
  print result
  exit(1)

transID = result['Value']
with open('TransformationID', 'w') as fd:
  fd.write(str(transID))
print "Created %s, stored in file 'TransformationID'" % transID
