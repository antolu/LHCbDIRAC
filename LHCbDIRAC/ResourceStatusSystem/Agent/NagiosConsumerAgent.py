#"""
#LHCbDIRAC/ResourceStatusSystem/Agent/NagiosConsumerAgent.py
#"""
#
#__RCSID__ = "$Id$"
#
## First, pythonic stuff
## External package that allows to connect to ActiveMQ using stomp protocol
#import stomp
#import Queue
#
#from datetime import datetime
#
## Second, DIRAC stuff
#from DIRAC                                      import S_OK, S_ERROR
#from DIRAC.Core.Base.AgentModule                import AgentModule
#from DIRAC.ResourceStatusSystem.Utilities.Utils import where
#
## Third, LHCbDIRAC stuff
#from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
#
#AGENT_NAME = "ResourceStatusSystem/NagiosConsumerAgent"
#
## We need this global variable to communicate between threads.
## It is dirty... Future me, forgive me for this.
#__MsgQueue = Queue.Queue()
#
#################################################################################
## CUSTOM STOMP LISTENER
#################################################################################
#
#class CustomStompListener( stomp.listener.ConnectionListener ):
#  '''
#  Class CustomStompListener, an extension of ConnectionListener.
#
#  The extension overwrites the following methods:
#
#  - on_message
#  '''
#
#################################################################################
#
#  def on_message( self, headers, message ):
#    '''
#    Overwrites the method on_message of ConnectionListener to parse the
#    messages into a dictionary.
#
#    :params:
#      :attr: `headers` : string - message headers
#
#      :attr: `message` : string - message content
#    '''
#
#    message = message.replace( 'EOT\n','' )
#    message = message.split( '\n' )
#
#    messageDict = {}
#
#    for m in message:
#      if ':' in m:
#
#        key, value = m.split( ':', 1 )
#        #Remove initial space if any
#        if value[ 0 ] == ' ':
#          value = vavlue[ 1: ]
#        messageDict[ key ] = value
#
#    __MsgQueue.put( [ headers, messageDict ] )
#
#################################################################################
## END OF CUSTOM STOMP LISTENER
#################################################################################
#
#class NagiosConsumerAgent( AgentModule ):
#  '''
#  Class NagiosConsumerAgent. This agent is in charge of connecting to ActiveMQ
#  making use of the stomp protocol, and feed the table MonitoringTests of the
#  ResourceManagementDB database with the selected data.
#
#  Only a snapshot with the latest values of every metric at every site is kept
#  on the database.
#
#  The agent overwrites the parent methods:
#
#  - initialize
#  - execute
#  - finalize
#
#  And adds the auxiliar method:
#
#  - __checkParams
#  '''
#
#  # Too many public methods
#  # pylint: disable-msg=R0904
#
#  # Default Stomp values to connect to ActiveMQ ( development )
#  __STOMP__ = {
#               'HOST' : "dev.msg.cern.ch",
#               'PORT' : "6163",
#               'DEST' : "/queue/Consumer.LHCbDIRAC.grid.probe.metricOutput.EGEE.vo.*",
#               'SELC' : "nagios_host='sam-lhcb.cern.ch'"
#              }
#
#################################################################################
#
#  def initialize( self ):
#    '''
#    Method executed when the agent is launched.
#    It connects to ActiveMQ using the given or by default parameters, and starts
#    the listener - CustomStompListener -, that will run on a separate process
#    populating the queue __MsgQueue with the incoming messages.
#    '''
#
#    # Attribute defined outside __init__  
#    # pylint: disable-msg=W0201
#
#    self.log.info( 'NagiosConsumerAgent' )
#
#    try:
#
#      self.rmClient      = ResourceManagementClient()
#
#      # Get stomp parameters from CS
#      self.sHOST = self.am_getOption( 'STOMP/HOST', self.__STOMP__[ 'HOST' ] )
#      self.sPORT = int(self.am_getOption( 'STOMP/PORT', self.__STOMP__[ 'PORT' ] ))
#      self.sDEST = self.am_getOption( 'STOMP/DEST', self.__STOMP__[ 'DEST' ] )
#      self.sSELC = self.am_getOption( 'STOMP/SELC', self.__STOMP__[ 'SELC' ] )
#
#      host_and_ports = [( self.sHOST, self.sPORT )]
#      self.log.info( 'Connecting to %s:%d' % ( self.sHOST, self.sPORT ) )
#
#      self.sConn = stomp.Connection( host_and_ports, prefer_localhost = False )
#
#      # We connect to the virtual destination, and the listener, on a separate
#      # thread will push into the queue
#      self.sConn.set_listener('', CustomStompListener())
#      self.log.info( 'Added CustomStompListener' )
#
#      self.sConn.start()
#      self.log.info( 'STOMP connection started' )
#
#      self.sConn.connect()
#      self.log.info( 'STOMP connection ready' )
#
#      self.sConn.subscribe( destination = self.sDEST,
#                            ack         = 'auto',
#                            headers     = { 'selector' : self.sSELC }
#                            )
#
#      return S_OK()
#
#    except Exception:
#      errorStr = "NagiosConsumerAgent initialization"
#      self.log.exception( errorStr )
#      try:
#        # Tries to kill the process with the listener
#        self.sConn.stop()
#        self.log.info( 'STOMP has been successfully disconnected' )
#      except:
#        pass
#      return S_ERROR( errorStr )
#
#################################################################################
#
#  def execute( self ):
#    '''
#    At every execution this method will take an aproximate length of the
#    queue - L, and will get L messages from the queue. If there are messages left,
#    them will be processed on next round.
#
#    Processing messages is understood as taking getting them from que queue to
#    either be added or updated on the MonitoringTest table on the ResouceManagementDB.
#    '''
#
#    try:
#
#      # This value is not accurate, but it is close enough.
#      msgs = __MsgQueue.qsize()
#
#      self.log.info( '%d message(s) to be processed' % msgs )
#
#      # Let's try without threads, and see the polling time.
#      for _m in xrange(msgs):
#        
#        _head, msg = __MsgQueue.get()              
#
#        params = self.__checkParams( msg )
#
#        if params:
#          res = self.rmClient.addOrModifyMonitoringTest( **params )
#          if not res[ 'OK' ]:
#            self.log.error( 'Error adding %s' % str( params ) )  
#
#      return S_OK()
#
#    except Exception, e:
#      errorStr = where( self, self.execute )
#      self.log.exception( errorStr, lException = e )
#      return S_ERROR( errorStr )
#
#################################################################################
#
#  def finalize( self ):
#    '''
#    If the agent is about to expire, we ensure we kill the connection.
#    The new agent will pick all the messages stored on the queue on
#    the mean time.
#    '''
#
#    try:
#
#      self.log.info( "Agent is finalizing it's last cycle, disconnecting STOMP" )
#      # Disconnect and waits for the receiver thread to exit
#      self.sConn.stop()
#      self.log.info( "Done" )
#
#      # We may have messages on the queue, so we try to empty it, once we know
#      # the connection is closed.
#      return self.execute()
#
#    except Exception, e:
#      #It may fail because it crashed on initialization
#      self.log.error( "Failed %s" % e )
#      return S_OK()
#
#################################################################################
## AUXILIAR FUNCTIONS
#################################################################################
#
#  def __checkParams( self, msg ):
#    '''
#    Given a SAM/Nagios message,
#    returns a dictionary with the interesting parameters.
#    Otherwise, prints error and returns {}
#
#    :params:
#      :attr: `msg` : dictionary - message on dictionary format ( key, value )
#    '''
#
#    # Parameters we want to extract from the message
#    __PARAMS__ = [ 'siteName', 'timestamp', 'metricName', 'metricStatus',
#                   'serviceURI', 'summaryData', 'serviceFlavour']
#
#    params = { 'lastCheckTime' : None }
#
#    for param in __PARAMS__:
#      if not msg.has_key( param ):
#        self.log.error( 'Ugly message, parameter %s missing' % param )
#        self.log.error( msg )
#        params = {}
#        break
#      else:
#        if param == 'timestamp':
#          timeFormat = '%Y-%m-%dT%H:%M:%SZ'
#          msg[ param ] = datetime.strptime( msg[param], timeFormat )
#        params[ param ] = msg[ param ]
#
#    return params
#
#################################################################################
##EOF
