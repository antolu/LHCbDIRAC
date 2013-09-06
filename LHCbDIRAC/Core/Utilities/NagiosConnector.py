""" NagiosConnector is a utility to publish checks on job results to the
    IT-based SAM/Nagios framework. The publishing is done by messaging
    via an ActiveMQ-Broker specified in the configuration 
    (It expects values for MsgBroker, MsgPort,  MsgQueue and NagiosName). 
    For the message to arrive, it is essential to publish to the right queue,
    which is also specified in the configuration.
    The message is built using a dictionary passed to one of the methods, 
    which should contain the keys 'SAMResults' 'SAMDetails' 'GridRequiredCEs'.

LHCbDirac/Core/Utilities
"""


from DIRAC import S_OK, S_ERROR, gLogger, gConfig

import datetime, stomp
from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations


class NagiosConnector():
  """Bundles functions in the stomp library for sending SAMJob-Results to 
  SAM-Nagios.""" 
  def __init__( self ):
    self.config = {}
    self.opsHelper = Operations()
    self.conn = None
    self.message = ''

  def readConfig( self ):
    """get Message Broker and Queue from Configuration file."""

    self.config = {}
    for item in ['MsgBroker', 'MsgQueue', 'MsgPort', 'NagiosName']:
      path = self.opsHelper.generatePath( '/NagiosConnector/%s' % item )
      self.config[ item ] = gConfig.getValue( path)
      if not self.config[item]:
        gLogger.exception('Required Configuration Value is empty: %s' % item)
      gLogger.verbose( 'Read Config Values for %s: %s' % (item, self.config[item] ) )
                       
    self.config['MsgPort'] = int( self.config['MsgPort']  )

    
  def useDebugMessage( self ):
    """Load a sample message for debugging""" 
    # at the moment just a test message
    self.message = """hostName: ce.hpc.iit.bme.hu
metricStatus: OK
timestamp: 2013-11-09T17:59:19Z
nagiosName: org.lhcb.DiracTest-lhcb
summaryData: publishing by Dirac successful
serviceURI:   ce.hpc.iit.bme.hu
serviceFlavour: CE
siteName: myTestSite
metricName: org.lhcb.DiracTest
gatheredAt: sam-developers-machine
role: site
voName: lhcb
serviceType: org.lhcb.CE
detailsData: further details here:
EOT"""

  def assembleMessage( self, msginfos):
    """Extract infos from workflow_commons (passed as msginfos) 
    and bring them in the right format."""
    #gLogger.verbose(msginfos['JobReport'].dump())
    
    try:
      for field in ['SAMResults', 'SAMDetails', 'GridRequiredCEs', 'JOB_ID', 'logURL' ]:
        gLogger.verbose( '%s information present in workflow_commons' % field )
    except KeyError, e:
      return S_ERROR( 'Required information missing in workflow_commons: %s' %e)
    finally:
      if 'CRITICAL' in msginfos[ 'SAMResults' ].values():
        status = 'CRITICAL'
      else:
        status = 'OK'      
      details = ' '.join( msginfos[ 'SAMDetails' ].values() ) 
    
      message = ""
      message += "serviceURI: %s\n" % msginfos['GridRequiredCEs']
      message += "metricStatus: %s\n" % status
      message += "summaryData: Job_ID: %s. Logfile: %s Details: %s \n" % (msginfos[ 'JOB_ID' ], 
                        msginfos.setdefault('logURL',"could not find logURL"), details)
      currentTime = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
      message += "timestamp: %s\n" % currentTime
      message += "nagiosName: %s\n" % self.config[ 'NagiosName']
      #all other fields are not used in SAM-Nagios
      #they are left here for compatibility / legacy
      #message += "gatheredAt: %s\n" % msginfos['ce']
      #message += "metricName: org.lhcb.WN-%s-lhcb\n" % msginfos[ 'JobName' ]
      #message += "serviceFlavour: %s\n" % msginfos['serviceFlavour']
      #message += "role: site\n"
      #message += "voName: lhcb\n"
      #message += "serviceType: org.lhcb.WN\n"
      #message += "detailsData: %s\n" % msginfos[ 'testDetails' ]
      message += "EOT\n"
      self.message = message

  def initializeConnection( self ):
    """Connect the conn object with the Broker read from configuration."""
    try:
      self.conn = stomp.Connection( [ ( self.config['MsgBroker'], 
                                        self.config['MsgPort'] ) ] )
      # There may be a need to receive messages.
      # In this case there should be a class with an on_message method
      # conn.set_listener('',MyListener())
      self.conn.start()
      self.conn.connect()
    except stomp.exception.ConnectFailedException, e:
      gLogger.error( 'Error establishing connection: %s' % e )
      return S_ERROR( 'Error establishing connection: %s' % e )

  def sendMessage( self ):
    """Use the conn object to send a message to the broker.
     If the format and the configurations are correct,
    the message content will appear in SAM/Nagios."""

    if not self.message:
      gLogger.error(  'The message string is empty!' )
      return S_ERROR( 'The message string is empty!')
    self.conn.send( self.message, destination = self.config[ 'MsgQueue' ] )
    gLogger.verbose( 'Message sent to %s on %s' % ( self.config[ 'MsgBroker' ], 
                                                    self.config[ 'MsgQueue'] ) )
    gLogger.verbose( 'Message content %s' %  self.message )

    
  def endConnection( self ):
    """Call disconnect() on the conn object."""
    self.conn.disconnect()
    return S_OK( 'Connection succesfully terminated' )
  
  def publishNagios( self, msginfos ):
    """Bundle all separate functions to publish a SAM-Job Result to 
    SAM-Nagios."""
    self.readConfig()
    self.assembleMessage(msginfos)
    self.initializeConnection()
    self.sendMessage()
    self.endConnection()


