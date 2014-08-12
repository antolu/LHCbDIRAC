"""
An agent to check for MCSimulation productions that have undergone the testing phase.
Productions that have the status Idle and are also in the table StoredJobDescription have undergone testing.
A report is created by the agent from the results of the test phase and emailed to the Production Manager

Author: Simon Bidwell
"""

__RCSID__ = "$Id$"

AGENT_NAME = 'Transformation/MCSimulationTestingAgent'

from DIRAC                                                                import S_OK, S_ERROR
from DIRAC.Core.Base.AgentModule                                          import AgentModule
from DIRAC.Core.Workflow.Workflow                                         import fromXMLString
from LHCbDIRAC.TransformationSystem.Client.TransformationClient           import TransformationClient
from LHCbDIRAC.BookkeepingSystem.Client.BookkeepingClient                 import BookkeepingClient
from DIRAC.FrameworkSystem.Client.NotificationClient                      import NotificationClient
from DIRAC.ConfigurationSystem.Client.Helpers.Operations                  import Operations
from DIRAC.ConfigurationSystem.Client.Helpers.Registry                    import getUserOption
from LHCbDIRAC.ProductionManagementSystem.Client.Production               import Production
from LHCbDIRAC.ProductionManagementSystem.Client.ProductionRequest        import getEventsToProduce

class MCSimulationTestingAgent ( AgentModule ):
  '''An agent to check for MCSimulation productions that have undergone the testing phase.
     Productions that have the status Idle and are also in the table StoredJobDescription have undergone testing.
     A report is created by the agent from the results of the test phase and emailed to the Production Manager
  '''
  
  def __init__( self, *args, **kwargs ):
    """ c'tor
    """
    AgentModule.__init__( self, *args, **kwargs )
    self.transClient = None
    self.bkClient = None
    self.notifyClient = None
    self.operations = None

  def initialize( self ):
    self.transClient = TransformationClient()
    self.bkClient = BookkeepingClient()
    self.notifyClient = NotificationClient()
    self.operations = Operations()
    return S_OK()


  def execute( self ):
    # get all the idle transformations
    res = self.transClient.getTransformations( condDict = {"Status": "Idle", "Type": "MCSimulation"} )
    if res['OK']:
      idleTransformations = res['Value']
      idleTransformations = [d.get( "TransformationID" ) for d in idleTransformations]
    else:
      message = "Call to Transformation Client service failed : %s" % res['Message']
      self.log.error( message )
      return S_ERROR( message )

    # get all the IDs of transformations undergoing a testing phase
    res = self.transClient.getStoredJobDescriptionIDs()
    if res['OK']:
      testingSimulations = res['Value']
      testingSimulations = [pair[0] for pair in testingSimulations]
    else:
      message = "Call to Transformation Client service failed : %s" % res['Message']
      self.log.error( message )
      return S_ERROR( message )

    # get the IDs that occur in both idle transformations and testing phase
    idleSimulations = set( testingSimulations ).intersection( idleTransformations )
    for transID in idleSimulations:
      tasks = self.transClient.getTransformationTasks( condDict = {"TransformationID" : transID} )
      if tasks['OK']:

        tasks = tasks['Value']
        numberOfTasks = len( tasks )
        numberOfDoneTasks = sum( 1 for d in tasks if d.get( "ExternalStatus" ) == "Done" )
        if numberOfTasks == numberOfDoneTasks:
          # all tasks have passed so the request can be accepted and the transformation table accepted
          parameters = self._calculate_parameters( tasks )
          if parameters['OK']:
            parameters = parameters['Value']
            workflow = self._update_workflow( transID, parameters['CPUe'], parameters['max_e'] )
            if workflow['OK']:
              workflow = workflow['Value']
              res = self._update_transformations_table( transID, workflow )
              if res['OK']:
                self.log.info( "Transformation " + str( transID ) + "passed the testing phase and is now set to active" )

        else:
          numberOfFailedTasks = sum( 1 for d in tasks if d.get( "ExternalStatus" ) == "Failed" )
          if numberOfFailedTasks == numberOfTasks:
            # all tasks have failed so the request can be rejected and an email report sent
            report = self._create_report( tasks )
            self._send_report( report )
            self.log.info( "Transformation " + str( transID ) + "failed the testing phase" )
          else:
            # only some tasks have failed so extend the failed tasks to repeat them
            self._extend_failed_tasks( transID, numberOfFailedTasks )
            self.log.info( str( numberOfFailedTasks ) + "tasks of Transformation " + str( transID ) + "failed the testing phase, so the transformation has been extended" )

      else:
        message = "Call to Transformation Client service failed : %s" % res['Message']
        self.log.info( message )

    return S_OK()

  def _create_report( self, tasks ):
    '''creates a report from a failed task to email to the production manager
    '''
    dateformat = '%d/%m/%Y %H:%M'
    transformationID = tasks[0]["TransformationID"]
    transformation = self.transClient.getTransformations( condDict = {"TransformationID" : transformationID} )
    transformation = transformation['Value'][0]
    subject = "MCSimulation Test Failure Report. TransformationID: " + str( transformationID )
    body = [subject]
    body.append("")
    body.append("Transformation:")
    body.append("----------------------------------------------------------------------")
    body.append( "TransformationID: " + str( transformation["TransformationID"] ) )
    body.append( "TransformationName: " + transformation["TransformationName"] )
    body.append( "LastUpdate: " + transformation["LastUpdate"].strftime( dateformat ) )
    body.append( "Status: " + transformation["Status"] )
    body.append( "Description: " + transformation["Description"] )
    body.append( "TransformationFamily: " + str( transformation["TransformationFamily"] ) )
    body.append( "Plugin: " + transformation["Plugin"] )
    body.append( "Type: " + transformation["Type"] )
    body.append( "AgentType: " + transformation["AgentType"] )
    body.append( "GroupSize: " + str( transformation["GroupSize"] ) )
    body.append( "MaxNumberOfTasks: " + str( transformation["MaxNumberOfTasks"] ) )
    body.append( "AuthorDN: " + transformation["AuthorDN"] )
    body.append( "TransformationGroup: " + transformation["TransformationGroup"] )
    body.append( "InheritedFrom: " + str( transformation["InheritedFrom"] ) )
    body.append( "CreationDate: " + transformation["CreationDate"].strftime( dateformat ) )
    body.append( "FileMask: " + transformation["FileMask"] )
    body.append( "EventsPerTask: " + str( transformation["EventsPerTask"] ) )
    body.append( "AuthorGroup: " + transformation["AuthorGroup"] )
    body.append( "" )
    body.append( "Number of Tasks: " + str( len( tasks ) ) )
    body.append("Tasks:")
    body.append("----------------------------------------------------------------------")
    for task in tasks:
      body.append( "TaskID: " + str( task['TaskID'] ) )
      body.append( "TargetSE: " + task['TargetSE'] )
      body.append( "LastUpdateTime: " + task['LastUpdateTime'].strftime( dateformat ) )
      body.append( "RunNumber: " + str( task['RunNumber'] ) )
      body.append( "CreationTime: " + task['CreationTime'].strftime( dateformat ) )
      body.append( "ExternalID: " + str( task['ExternalID'] ) )
      body.append( "ExternalStatus: " + task['ExternalStatus'] )
      body.append( "" )
    return {'subject': subject, 'body': body}

  def _send_report( self, report ):
    '''sends a given report to the production manager
    '''
    username = self.operations.getValue("Shifter/ProductionManager/User")
    email = getUserOption( username, "Email" )
    body = '\n'.join( report['body'] )
    res = self.notifyClient.sendMail( email, report['subject'], body,
                                      email )
    if not res['OK']:
      self.log.error( res['Message'] )
      return S_ERROR( res['Message'] )
    else:
      self.log.info( 'Mail summary sent to production manager' )
      return res

  def _calculate_parameters( self, tasks ):
    '''calculates the CPU time per event for a successful task.
    '''
    job_id = tasks[0]['ExternalID']
    res = self.bkClient.bulkJobInfo( {'jobId':[job_id]} )
    if not res['OK']:
      self.log.error( res['Message'] )
      return S_ERROR( res['Message'] )
    successful = res['Value']['Successful']
    key = successful.keys()[0]
    cpuTime = successful[key]['ExecTime']
    events = successful[key]['NumberOfEvents']
    CPUe = cpuTime / events
    max_e = getEventsToProduce( CPUe )
    return S_OK( {'CPUe' : CPUe, 'max_e': max_e} )

  def _update_workflow( self, transID, CPUe, max_e ):
    '''updates the workflow of a savedProductionDescription to reflect the calculated CPUe
    '''
    res = self.transClient.getStoredJobDescription( transID )
    if res['OK']:
      workflow = fromXMLString( res['Value'][0][1] )
      prod = Production()
      prod.LHCbJob.workflow = workflow
      prod.setParameter( 'CPUe', 'string', str( CPUe ), 'CPU time per event' )
      prod.setParameter( 'maxNumberOfEvents', 'string', str( max_e ), 'Maximum number of events to produce (Gauss only)' )
      self.log.info( "Transformation " + str( transID ) + "Calculated CPUe: " + str( CPUe ) + ". Calculated max_e: " + str( max_e ) )
      return S_OK( prod.LHCbJob.workflow.toXML() )
    else:
      message = "Call to Transformation Client service failed : %s" % res['Message']
      self.log.error( message )
      return S_ERROR( message )

  def _update_transformations_table( self, transID, workflow ):
    '''puts the modified workflow from the savedProductionDescription table into the transformations table
       and removes it from the savedProductionDescription table.
    '''
    transformation = self.transClient.getTransformations( condDict = {"TransformationID" : transID} )
    if transformation['OK']:
      body = self.transClient.setTransformationParameter( transID, "Body", workflow )
      status = self.transClient.setTransformationParameter( transID, "Status", "Active" )
      if body['OK'] and status['OK']:
        self.transClient.removeStoredJobDescription( transID )
        message = "Transformation " + str( transID ) + " has an updated body and Status set to active"
        self.log.info( message )
        return S_OK( message )
      else:
        # one of the updates has failed so set them both back to the previous value to ensure atomicity
        print transformation['Value'][0]['Body']
        self.transClient.setTransformationParameter( transID, "Body", transformation['Value'][0]['Body'] )
        self.transClient.setTransformationParameter( transID, "Status", transformation['Value'][0]['Status'] )
        message = "One of the updates for transformation " + str( transID ) + " has failed."
        self.log.error( message )
        return S_ERROR( message )
    else:
      message = "Call to Transformation Client service failed : %s" % transformation['Message']
      self.log.error( message )
      return S_ERROR( message )

  def _extend_failed_tasks( self, transID, numberOfFailedTasks ):
    '''takes the number of failed tasks of a testing phase and extends the production by that number to
       repeat the test
    '''
    res = self.transClient.extendTransformation( transID, numberOfFailedTasks )
    if not res['OK']:
      message = 'Failed to extend transformation %d : %s' % ( transID, res['Message'] )
      self.log.error( message )
      return S_ERROR( message )
    else:
      message = "Successfully extended transformation %d by %d tasks" % ( transID, numberOfFailedTasks )
      self.log.info( message )
      return S_OK( message )

