import rgma
import time
import re



CONTINUOUS=rgma.QueryType.CONTINUOUS
LATEST=rgma.QueryType.LATEST
HISTORY=rgma.QueryType.HISTORY



class Query (object):

    def __init__(self,sql,type=LATEST,timeout=300,termination=300):

        terminationInterval=rgma.TimeInterval(termination)
        timeoutInterval=rgma.TimeInterval(timeout)
        self.cons=rgma.Consumer(terminationInterval,
                           rgma.QueryProperties(type,timeoutInterval),
                           sql)
        self.sql = sql
        self.cons.start(timeoutInterval)
        self.buffer=[]


    def __close__(self):
        self.cons.close()


    def __iter__(self):
        return self


    def next(self):
        "Return next value or stop iteration"
        # make sure buffer is not empty unless the query finished
        while (not self.buffer) and self.cons.isExecuting():
            self.buffer=self.cons.pop()
            time.sleep(0.1)

        # get the remaining data 
        if not self.buffer:
            self.buffer.extend(self.cons.pop())

        if self.buffer:
            return self.buffer.pop(0) # next value
        else:
            raise StopIteration       # no more data 


    def count(self):
        return self.cons.count() + len(self.buffer)


    def count_existing(self):

        query = re.sub("SELECT.+ FROM ", "SELECT COUNT(*) FROM ", self.sql )

        cons = rgma.Consumer(rgma.TimeInterval(10),
                           rgma.QueryProperties(rgma.QueryType.LATEST,rgma.TimeInterval(100)),
                           query)
        cons.start(rgma.TimeInterval(100))

        while cons.isExecuting():
            time.sleep(1)

        tuple = cons.pop()
        cons.close()
        if tuple:
            return tuple[0]
        else:
            return None

