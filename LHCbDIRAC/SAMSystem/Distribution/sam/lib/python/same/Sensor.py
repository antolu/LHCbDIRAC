import os
import sys
import re
from same import Config
from same.Config import config
from same import Database
from same import Scheduler

same_home=os.environ['SAME_HOME']

operations=['submit','publish','status','cancel','nodetest','unlock']

class WrongSensor(Exception):
    pass

def setupDir(dir):
    try:
	os.chdir(dir)
    except:
	os.makedirs(dir)
	os.chdir(dir)


def setupWorkDir():
    global same_work
    if os.environ.has_key('SAME_WORK'):
        same_work=os.environ['SAME_WORK']
    else:
        same_work=config.get('DEFAULT','workdir')
        os.environ['SAME_WORK']=same_work
    setupDir(same_work)

def setupSensorDir(sensor):
    sensordir=same_work+'/'+sensor
    setupDir(sensordir)
    

def setupTestsEnv():
    os.environ['SAME_VO']=config.get('submission','vo')
    for status in ['OK','INFO','NOTICE','WARNING','ERROR','CRITICAL','MAINTENANCE']:
	os.environ['SAME_'+status]=config.get('statuscode',status)
    
def setupSensorEnv(sensor):
    setupWorkDir()
    setupSensorDir(sensor)
    setupTestsEnv()
    os.environ['SAME_SENSOR_NAME']=sensor
    os.environ['SAME_SENSOR_HOME']=same_home+'/sensors/'+sensor
    os.environ['SAME_SENSOR_WORK']=same_work+'/'+sensor
    Config.save(same_work+'/'+sensor+'/same.conf')

def createFilter(sensor,user_filter):
    common_filter=config.get('sensors','common_filter').strip('"\'')
    try:
	sensor_filter=config.get('sensors','%s_filter'%sensor).strip('"\'')
    except:
	sensor_filter='serviceabbr=%s'%sensor
    filter_string=' '.join([common_filter,sensor_filter,user_filter])
    filter=dict(map(lambda x: (x[0],x[1].split(',')),map(lambda x: x.split('='),filter_string.split())))
    return filter

def getAttrs(sensor):
    try:
        attrs=config.get('sensors','%s_attrs'%sensor).strip('"\'')
    except:
        attrs=config.get('sensors','common_attrs').strip('"\'')
    return attrs.split();

def writeNodesFile(nodes,filename="nodes.map"):
    out=open(filename,"w")
    lines=map(lambda x: '\t'.join(x)+'\n',nodes)
    out.writelines(lines)
    out.close()

def executePrepareScript(sensor,args=''):
    cmd=same_home+'/sensors/'+sensor+'/prepare-'+sensor+' '+args

    p=os.popen(cmd,"r")
    lines=p.readlines()
    status=p.close()
    return status,lines

def executeCheckScripts(sensor,nodes,args=''):
    cmd=same_home+'/sensors/'+sensor+'/check-'+sensor+' '+args

    for node in nodes:
	Scheduler.run(cmd+' '+' '.join(node))

    Scheduler.wait()

def checkSensor(sensor):
    try:
        os.stat(same_home+'/sensors/'+sensor)
    except:
        return False
    return True

def lockSensor(sensor,operation):
    setupWorkDir()
    setupSensorDir(sensor)
    lockFile=same_work+'/'+sensor+'/lock'
    try:
        os.stat(lockFile)
        return False,open(lockFile).readline().strip()
    except:
        open(lockFile,"w").write(operation+"\n")
        return True, None

def unlockSensor(sensor):
    setupWorkDir()
    setupSensorDir(sensor)
    lockFile=same_work+'/'+sensor+'/lock'
    try:
        os.unlink(lockFile)
    except:
        pass
      
def operationExec(operation,sensor,user_filter,sensor_args):
    if operation == 'nodetest':
        return operationNodeTest(sensor,user_filter,sensor_args)
    elif operation == 'unlock':
        unlockSensor(sensor)
        return

    locked,lockingOperation=lockSensor(sensor,operation)
    if not locked:
        if operation=='status':
            print "Sensor locked by %s operation, but it is ignored for status checking."%lockingOperation
        else:
            print "Sensor locked by %s operation!"%lockingOperation
            return
       
    if operation != 'submit':
        operationParam='--'+operation
    else:
        operationParam=''

    try:
        filter=createFilter(sensor,user_filter)
    except:
        print "Wrong filter format!"
        unlockSensor(sensor)
        return
        
    attrs=getAttrs(sensor)
    sargs=' '.join(sensor_args)
    print "Sensor: "+sensor
    print "Node attrs: "+str(attrs)
    print "Node filter: "+str(filter)
    print "Sensor args: "+sargs

    try:
        nodes=Database.get(attrs,filter)
    except Exception,e:
        print "Query web service error!"
        print e.args[0]
        unlockSensor(sensor)
        return
    
    print "\nMatching nodes: "+str(len(nodes))

    setupSensorEnv(sensor)
    writeNodesFile(nodes)

    print "Executing prepare script"
    print '-'*40
    status,lines=executePrepareScript(sensor,operationParam+" -i nodes.map "+sargs)
    print '-'*40
    if status:
	print "Prepare script failed! Giving up."
        unlockSensor(sensor)
	sys.exit(status)

    if operation != 'submit':
        nodes=map(lambda x: tuple(x.split()),lines)
        
    print "Prepare script finished successfully"	
    print "Executing check script for all nodes"
    print '-'*40
    executeCheckScripts(sensor,nodes,operationParam)
    print '-'*40
    print "Executing check scripts finished"
    unlockSensor(sensor)

def operationNodeTest(sensor,nodeName,sensor_args):
    sargs=' '.join(sensor_args)
    print "Sensor: "+sensor
    print "Node name: "+nodeName
    print "Sensor args: "+sargs

    nodes=[(nodeName,)]

    setupSensorEnv(sensor)
    writeNodesFile(nodes)

    print "Executing prepare script"
    print '-'*40
    status,lines=executePrepareScript(sensor,"-i nodes.map "+sargs)
    print '-'*40
    if status:
	print "Prepare script failed! Giving up."
	sys.exit(status)

    print "Prepare script finished successfully"	
    print "Executing check script for all nodes"
    print '-'*40
    executeCheckScripts(sensor,nodes)
    print '-'*40
    print "Executing check scripts finished"


def getSensorTests(sensor):
    l=[]
    for i in map(lambda x: x.split(),open(same_home+'/sensors/'+sensor+'/test-sequence.lst').readlines()):
        if i:
            l.append(i[0])
    return l

testDefPattern=re.compile('(\w+)\s*:\s*(.*)')

def readSensorTestDef(sensor,test):
    d={}
    for l in open(same_home+'/sensors/'+sensor+'/tests/'+test+'.def').readlines():
        m=testDefPattern.search(l)
        if m:
            g=m.groups()
            d[g[0].lower()]=g[1].strip()
    return d
    
