import socket

class Publisher:
    
    def __init__(self, host, port, user='', passcode=''):
        self.ss = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.ss.connect((host, port))
        connstr = 'CONNECT\nuser: %s\npasscode: %s\n\n\x00\n' % (user, passcode)
        self.ss.send(connstr)
        self.buf = [ ]
        #print self.__read(self.ss)
        
    
    def __read(self, sock):
        try:
            while 1:
                c = sock.recv(1024)
                self.buf.append(c)
                if '\x00' in c:
                    break
        except Exception:
            pass
        s = ''.join(self.buf)
        return s
    
    def send(self, dest, msg, transactionid=None, headers=None):
        if transactionid:
            sheader = 'transaction: %s\n' % transactionid
        else:
            sheader = ''
        if headers:
            for k, v in headers.iteritems():
                sheader+=k + ": " + v + "\n"
        tosend = 'SEND\ndestination: %s\n%s\n%s\x00\n' % (dest, sheader, msg)
        self.ss.send(tosend)
        
    def disconnect(self):
        self.ss.send('DISCONNECT\n\n\x00\n')
