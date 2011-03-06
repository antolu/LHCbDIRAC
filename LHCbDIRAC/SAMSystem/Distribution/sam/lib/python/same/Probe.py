import os
import sys
import time
from same import Config
from same.Config import config
from same import Database
from same import Scheduler
from same import Sensor
import re
import tempfile

same_home=os.environ['SAME_HOME']

statusMap={ 0: 'UNKNOWN',
            10: 'OK', 20: 'OK', 30: 'OK',
            40: 'WARNING',
            50: 'CRITICAL', 60: 'CRITICAL',
            100: 'UNKNOWN'}

def w3ctime(t=time.time()):
    return time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime(t))
    
def operListSubmit(sensor,**kw):
    tests=Sensor.getSensorTests(sensor)
    for test in tests:
        d=Sensor.readSensorTestDef(sensor,test)
        print "metricName:",d['testname']
        print "serviceType:",d['testname'].split('-')[0]
        if d.has_key('datatype'):
            print "metricType: performance"
            dt=d['datatype']
        else:
            print "metricType: status"
            dt=None
        print "metricAbbr:",d.get('testabbr',test)
        if dt:
            print "dataType ",dt
        print "EOT"


def operListPrepare(sensor,**kw):
    print """metricName: SAM-prepare-%s
metricType: status
metricAbbr: pre%s
serviceType: SAM
EOT"""%(sensor,sensor)


def operPrepare(sensor,sargs=[],**kw):
    lock=Sensor.lockSensor(sensor,'prepare')
    Sensor.setupSensorEnv(sensor)
    Sensor.writeNodesFile([])
    
    if lock:
        raise Exception, "Sensor locked by %s operation!"%lock

    status,lines=Sensor.executePrepareScript(sensor," -i nodes.map "+''.join(sargs))
    Sensor.unlockSensor(sensor)

    if status:
	status='CRITICAL'
    else:
        status='OK'
    print """metricName: SAM-prepare-%s
metricStatus: %s
voName: %s
hostName: %s
timestamp: %s
EOT"""%(sensor,status,os.environ['SAME_VO'],os.environ['HOSTNAME'],w3ctime())


def translatePair(key,value,serviceURI):
    if key=='voname':
        print "voName: "+value
    elif key=='testname':
        print "metricName: "+value
    elif key=='nodename' and serviceURI:
        print "serviceURI: "+serviceURI
    elif key=='timestamp':
        print "timestamp: "+w3ctime(int(value))
    elif key=='status':
        print "metricStatus: "+statusMap[int(value)]
    elif key=='summarydata':
        print "summaryData: "+value
    elif key=='detaileddata':
        print "detailedData: "+value

    
def translateSensorOutput(serviceURI,filename):
    infile=open(filename)
    r=re.compile('\s*(\S+)\s*:\s*(\S.*\S)\s*')
    line=infile.readline()
    newTuple=True
    while line:
        if line=='EOT\n':
            print 'EOT'
            newTuple=True
        else:
            m=r.match(line)
            if m:
                key,value=m.groups()
                if newTuple:
                    print "gatheredAt: "+os.environ['HOSTNAME']
                    newTuple=False
                translatePair(key.lower(),value,serviceURI)
                if value=='EOT':
                    line=infile.readline()
                    while line and line!='EOT\n':
                        print line[:-1]
                        line=infile.readline()
                    print 'EOT'
                    newTuple=True
        line=infile.readline()


def operSubmit(sensor,serviceURI=None,sargs=[],**kw):
    Sensor.setupSensorEnv(sensor)
    tmpfile=tempfile.mktemp()
    os.environ['SAME_PUBLISHER_OUTPUT']=tmpfile
    
    if serviceURI:
        nodename=re.search('([^:.]+:)?/*([^:/]+)',serviceURI).groups()[1]
    else:
        nodename=os.environ['HOSTNAME']

    attrs=Sensor.getAttrs(sensor)
    print >>sys.stderr, "Node attrs: "+str(attrs)

    try:
        nodes=Database.get(attrs,{'nodename':[nodename]})
    except Exception,e:
        print >>sys.stderr, "Query web service error!"
        print e.args[0]
        unlockSensor(sensor)
        return
    
    if nodes:
        print >>sys.stderr, "\nMatching nodes: "+str(nodes)
    else:
        print >>sys.stderr, "\nNodes not found, using the nodename as sensors argument"
        nodes=[[nodename]]
    print >>sys.stderr,"nodename = "+nodename+""

    print >>sys.stderr,'-'*40
    cmd=same_home+'/sensors/'+sensor+'/check-'+sensor
    for node in nodes:
        Scheduler.run(cmd+' '+' '.join(node))    
    Scheduler.wait()
    print >>sys.stderr,'-'*40
    print >>sys.stderr,"Results written in: "+tmpfile
    translateSensorOutput(serviceURI,tmpfile)
    os.unlink(tmpfile)


operationsMap={'list-submit':operListSubmit,'list-prepare':operListPrepare,
               'prepare':operPrepare,'submit':operSubmit}


def run(params):
    if not Sensor.checkSensor(params['sensor']):
        raise Exception,"Sensor %s does not exist or is broken!"%params['sensor']

    if params['operation'] in operationsMap:
        operationsMap[params['operation']](**params)
    else:
        raise Exception, "Operation %s not implemented!"%params['operation']
    
