import asyncore, socket, time, urllib, sys, os
import Queue
import re
import logging
import traceback


destination_pattern=re.compile('/(.+)/(.+)')
url_pattern=re.compile('((\S+)://)?(([^:]+)(:([^:]+))?@)?([^:/]+)(:(\d+))?(/.*)?')
response_pattern=re.compile('HTTP/\S+ (\d+) ([^\n]*\n).*')

#logging.basicConfig()
#logging.root.setLevel(logging.WARN)
logger=logging.getLogger('msg.robustpublisher')

class PublishersPool:
    ''' Holds the pool of Publishers and Messages to be sent: 
        - self.messages        : list of messages to be sent.
        - self.active_messages : list of messages being sent from a publisher.
        - self.publishers      : list of all currently available publishers.
        - self.idle_publishers : list of publishers gone idle.'''

    def __init__(self,messages=[]):
        self.messages=messages
        self.active_messages=[]
        self.publishers=[]
        self.idle_publishers=[]
    
    def addPublisher(self,publisher):
        ''' Adds a publisher to the publishers pool. '''
        logger.debug( "Adding publisher at endpoint %s " % publisher.url )
        self.publishers.append(publisher)
        
    def removePublisher(self,publisher):
        ''' Removes a publisher from the publishers and idle publishers pool.'''
        logger.debug( "Removing publisher for endpoint %s " % publisher.url )
        if publisher in self.publishers :
            self.publishers.remove(publisher)
        if publisher in self.idle_publishers :
            self.idle_publishers.remove(publisher)
                
    def publisherIdle(self,publisher):
        ''' Sets publisher status to Idle. '''
        logger.debug( "Publisher gone idle %s " % publisher.url)
        self.idle_publishers.append(publisher)
        
    def getMessage(self):
        ''' Gets a message from pool for processing. '''
        logger.debug( "Getting message from pool : " )
        if self.messages:
            return self.messages[0]
        else:
            return None
        
    def publishingStart(self,msg):
        ''' Starts publishing message.
            - This implies setting message status to active, after checking if message is not being processed,
            returns  '''
        logger.debug( "Appending message to active sending message list." + str(id(msg)))  
        if msg in self.active_messages:
            return False
        elif msg not in self.messages:
            return False
        else:
            self.messages.remove(msg)
            self.active_messages.append(msg)
            return True
            
    def _publishingSuccess(self,msg):
        logger.debug( "Message %s published with success. Removing from active." % str(id(msg)))
        self.active_messages.remove(msg)
    
    def _publishingFailed(self,msg):
        logger.debug( "Message %s publishing failed. Notifying idle publishers. {%s}" % ( id(msg),str( self.idle_publishers)) )
        if msg in self.active_messages :
            self.active_messages.remove(msg)
            self.messages.insert(0,msg)
        self.notifyPublishers()
   
    def addMessages(self,messages):
        logger.debug( "Adding messages. Notifying idle publishers." )
        self.messages+=messages
        self.notifyPublishers()
        if len(self.publishers) < 1 :
            raise PublisherPoolException( 'Messages Added to the pool, but no publishers exist.' )
        
    def notifyPublishers(self) :
        if not self.active_messages and not self.messages:
            asyncore.close_all()
        # if retries
        # if timeout
        if len( self.idle_publishers ) < 1 :
            raise asyncore.ExitNow('No idle publishers left to notify')
            #log.error( "No idle publishers left to notify. Messages To Send: %s. \
            #            MessagesActive: %s" %(len(self.messages),len(self.active_messages)) )
            #asyncore.close_all()
            #raise PublisherPoolException('No idle publishers left to notify. pool exception')
        for publisher in list(self.idle_publishers) : 
            publisher.notify()
            self.idle_publishers.remove( publisher )
            
def parseURL(url):
    _,proto,_,_,_,_,host,_,port,path=url_pattern.match(url).groups()
    return proto,host,port,path
    
class RestPublisher(asyncore.dispatcher):

    def __init__(self, url, pool):
        logger.debug( "Initiating RestPublisher." )
        asyncore.dispatcher.__init__(self)
        self.url=url
        
        proxy_url=os.environ.get('http_proxy',os.environ.get('HTTP_PROXY'))
        if proxy_url:
            logger.info('Using http_proxy=%s'%proxy_url)
        else:
            proxy_url=url
        _,self.host,self.port,self.path=parseURL(proxy_url)
        if self.port:
            self.port=int(self.port)
        else:
            self.port=80
        self.pool=pool
        self.pool.addPublisher(self)
        self.message=None
        try:
            self._connect()
        except Exception, e:
            logger.warn("%s Couldn't connect, got: %s."%(self.url,str(e)))
            self.pool.removePublisher(self)
            self.close()
    
    def _connect(self):
        self.buffer=''
        self.read_buffer=''
        self.message=self.pool.getMessage()
        if self.message:
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connect( (self.host, self.port) )
            logger.info("%s Connecting. "%(self.url)) 
        else:
            self.pool.publisherIdle(self)

        
    def handle_connect(self):
        logger.info("Handling Rest Connect")
        t,d=destination_pattern.match(self.message['destination']).groups()
        self.msgenc=urllib.urlencode({'type':t,'destination':d,'body':self.message['body']})
        self.buffer='POST %s HTTP/1.1\r\n'\
                    'Host: %s\r\n'\
                    'Connection: close\r\n'\
                    'Content-Type: application/x-www-form-urlencoded\r\n'\
                    'Content-Length: %d\r\n\r\n'%(self.url,self.host,len(self.msgenc)) 

    def handle_close(self):
        logger.info("Handling Rest Close")
        if self.message:
            code,response=response_pattern.match(self.read_buffer).groups()
            if code=='200':
                logger.info("%s Message sent sucessfully"%self.url)
                self.pool._publishingSuccess(self.message)
                self.message=None
                self.close()
                self._connect()
            else:
                logger.error("%s Error: %s %s"%(self.url,code,response))
                if code == '502' :
                    logger.info( "Detailed Error Information:\n %s" %str(self.read_buffer) )           
                self.pool.removePublisher(self)
                self.pool._publishingFailed(self.message)
                self.close()
   
    def handle_read(self):
        try:
            receivedBytes=self.recv(8192)    #CRUCIAL! Don't try to reduce code to one line: 
            self.read_buffer+=receivedBytes  #unexpected behaviour may happen with a internal  
                                             #race condition on read_buffer.
        except Exception, e:
            logger.warn("%s Error when handling read: %s.."%(self.url,type(e)))
            logger.info("%s Removing current Publisher and resending failed message." %(self.url))
            self.pool.removePublisher(self)
            self.pool._publishingFailed(self.message)
            self.close()
             
    def handle_write(self):
        if self.buffer:
            sent=0
            try:
                sent = self.send(self.buffer)
            except socket.error, e:
                if e.args[0]!=57:
                    logger.warn("%s Error on send: %s "%(self.url,e.args)  )
                    logger.info("%s Removing current Publisher and resending failed message." %(self.url))
                    self.pool.removePublisher(self) 
                    self.pool._publishingFailed(self.message)
                    self.close()
            self.buffer = self.buffer[sent:]
            if (not self.buffer) and self.msgenc:
                if self.pool.publishingStart(self.message):
                    self.buffer=self.msgenc
                    self.msgenc=None
                else:
                    logger.info("%s: message already picked up by another publisher!"%self.url)
                    self.close()
                    self.pool.publisherIdle(self)
    def notify(self):
        try:
            self._connect()
        except Exception, e:
            logger.warn("%s Couldn't connect, got: %s."%(self.url,str(e)))
            self.pool.removePublisher(self)
            self.close()


class StompPublisher(asyncore.dispatcher):

    def __init__(self, url, pool):
        logger.debug( "Initiating StompPublisher" )
        asyncore.dispatcher.__init__(self)
        self.url=url
        _,self.host,self.port,_=parseURL(url)
        if self.port:
            self.port=int(self.port)
        else:
            self.port=6163
        self.pool=pool
        self.pool.addPublisher(self)
        self.message=None
        try:
            self._connect()
        except Exception, e:
            logger.warn("%s Couldn't connect, got: %s."%(self.url,str(e)))
            self.pool.removePublisher(self)
            self.close()
    
    def _encodeMessage(self):
        logger.debug("Encoding Message")
        header=dict(self.message.get('header',{}))
        header['content-length']=len(self.message['body'])
        header['persistent']='true'
        header['msg-encodedTime']=repr(time.time())
        self.msgenc='SEND\ndestination:%s\n%s\n%s\0\n'%\
            (self.message['destination'],
             ''.join(['%s:%s\n'%(k,str(v)) for (k,v) in header.items()]),
             self.message['body'])
            
    def _connect(self):
        self.buffer=''
        self.read_buffer=''
        self.stompActive=False
        self.message=self.pool.getMessage()
        if self.message:
            self._encodeMessage()
            self.create_socket(socket.AF_INET, socket.SOCK_STREAM)
            self.connect( (self.host, self.port) )
            logger.info("%s Connecting..."%self.url) 
        else:
            self.pool.publisherIdle(self)

        
    def handle_connect(self):
        logger.debug("handling connect" )
        self.stompActive=True
        self.buffer="CONNECT\n\n\0\n"

    def handle_close(self):
        self.stompActive=False

    def handle_read(self):
        try: 
            self.read_buffer+=self.recv(8192)
        except Exception, e:
            logger.warn("%s Error on read: %s %s"%(self.url, type(e), e.args))
            self.pool.removePublisher(self)
            if( self.message ) :
                self.pool._publishingFailed(self.message)  
            self.close()
            return
        if len(self.read_buffer)>=9:
            if self.read_buffer[:9]!='CONNECTED':            
                logger.warn("%s ERROR %s"%(self.url,self.read_buffer))
                self.pool.removePublisher(self)
                if self.message and not self.msgenc:
                    self.pool._publishingFailed(self.message)
                self.close()
            else:
                logger.info("%s Connected"%self.url)
                self.stompActive=True
                self.read_buffer=''

    def handle_write(self):
        if self.buffer:
            sent=0
            try:
                sent = self.send(self.buffer)
            except Exception, e:
                logger.warn("%s Error on send:"%self.url)
                self.pool.removePublisher(self)
                if self.message and not self.msgenc:
                     self.pool._publishingFailed(self.message)
                self.close()
            self.buffer = self.buffer[sent:]
        if not self.buffer and self.stompActive:
            if self.msgenc:
                if self.pool.publishingStart(self.message):
                    self.buffer=self.msgenc
                    self.msgenc=None
                else:
                    logger.info("%s: message already picked up by another publisher!"%self.url)
                    self.close()
                    self.pool.publisherIdle(self)
            elif self.message:
                logger.info("%s Message sent sucessfully"%self.url)
                self.pool._publishingSuccess(self.message)
                self.message=self.pool.getMessage()
                if self.message:
                    self._encodeMessage()
                else:
                    logger.info("%s: No more messages!"%self.url)
                    self.close()
                    self.pool.publisherIdle(self)
        if not self.stompActive :
            self.pool.removePublisher(self)
            self.pool._publishingFailed(self.message)
            self.close()
                    
    def notify(self):
        try:
            self._connect()
        except Exception, e:
            logger.warn("%s Couldn't connect, got: %s"%(self.url,str(e)))
            self.pool.removePublisher(self)
            self.close()
        

class RobustPublisher:
    protomap={'http': RestPublisher,
              'stomp': StompPublisher}
    def __init__(self,publisherURLs):
        self.pool=PublishersPool()
        if len( publisherURLs ) < 1 :
            raise PublisherException( 'No endpoints for Publishers provided.' )
        for url in publisherURLs:
            proto=parseURL(url)[0]
            if proto not in self.protomap:
                logger.warn('Protocol Not Supported: %s ' % proto )
                #raise ProtocolNotSupported,proto
            else:
                self.protomap[proto](url,self.pool)

    def publishMessages(self,messages):
        ''' Returns the number of published messages. Otherwise, Exception. '''
        try :
            messagesToPublish=len(messages)
            self.pool.addMessages(messages)
            asyncore.loop()
            messagesPublished = messagesToPublish - len(self.pool.messages) 
            logger.info("Success: %s out of %s messages published" 
                                                       % (messagesPublished, messagesToPublish ) )
            return messagesPublished 
        except Exception, e :
            messagesPublished = messagesToPublish - len(self.pool.messages) 
            
            logger.error("Exception: %s %s " %(e.__class__.__name__, e.args) )
            raise PublisherException( "failed to publish all messages: published %i out of %i" 
                                                       % (messagesPublished, messagesToPublish ) )
    
class ProtocolNotSupported(Exception) :
    pass

class PublisherException(Exception) :
    pass

class PublisherPoolException(Exception) :
    pass

###################################################################################################
## Script testing
##################################################################################################    

def test_RobustPublisher():
    msg_list=[{'destination':'/topic/t1', 
               'body':'This is a message1 body'},
               {'destination':'/topic/t1', 
               'body':'This is a message2 body'},
               {'destination':'/topic/t1', 
               'body':'This is a message3 body'},
               {'destination':'/topic/t1', 
               'body':'This is a message4 body'},
               {'destination':'/topic/t1', 
               'body':'This is a message5 body with \0\n character in it'}]
    
    publishers=['stomp://lxb2089.cern.ch:6163','http://lxn1181.cern.ch:8080/amq/message']

    p=RobustPublisher(publishers) 
    p.publishMessages(msg_list)   

if __name__=='__main__':
    logger.setLevel(logging.DEBUG)
    test_RobustPublisher()
