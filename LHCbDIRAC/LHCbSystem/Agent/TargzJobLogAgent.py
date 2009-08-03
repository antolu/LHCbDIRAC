__author__ = 'Vladimir Romanovsky'
__date__ = 'July 2009'
__version__ = 0.0

'''
Compress Old Jobs 
'''

from DIRAC import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.Agent import Agent

import sys, os
import time
import types
import glob
import re
from datetime import datetime, timedelta
import tarfile

AGENT_NAME = "LHCb/TargzJobLogAgent"

class TargzJobLogAgent(Agent):

  def __init__(self):
    """ Standard constructor
    """
    Agent.__init__(self,AGENT_NAME)

  def initialize( self ):
  
    result = Agent.initialize(self)

    self.logLevel = gConfig.getValue(self.section+'/LogLevel','INFO')
    gLogger.info("LogLevel",self.logLevel)
    gLogger.setLevel(self.logLevel)

    self.pollingTime = gConfig.getValue(self.section+'/PollingTime',3600)
    gLogger.info("PollingTime %d hours" %(int(self.pollingTime)/3600))

    self.logPath = gConfig.getValue(self.section+'/LogPath', '/storage/lhcb/MC/MC09/LOG')
    gLogger.info("LogPath", self.logPath)
    
    return S_OK()

  def execute(self):

    self.log.info( 'Starting Agent loop')
    path = os.path.abspath(self.logPath)

    age = gConfig.getValue(self.section+'/AgeDays', 7)
    gLogger.info("AgeDays", age)

    g1 = gConfig.getValue(self.section+'/ProductionGlob', '????????')
    gLogger.info("ProductionGlob", g1)
    g2 = gConfig.getValue(self.section+'/SubdirGlob', '????')
    gLogger.info("SubdirGlob", g2)
    g3 = gConfig.getValue(self.section+'/JobGlob', '????')
    gLogger.info("JobGlob", g3)
    
    numberOfTared = 0
    numberOfFailed = 0
    
    for jobpath in self._iFindOldJob(path,g1,g2,g3,age):
      pathlist = jobpath.split("/")
      job = pathlist[-1]
      prod = pathlist[-3]
      gLogger.debug( "Found Old Log", "Production %s, Job %s" %(prod,job) )

      name = prod + "_" + job + ".tgz"
      try:
        lines = open(os.path.join(jobpath,'index.html')).read()
        lines = lines.replace('</title>',' compressed</title>')
        lines = lines.replace('</h3>',' compressed</h3>',1)
        lines = re.compile('<a href.*</a><br>.*\n').sub('',lines)
        lines = lines.replace('compressed</h3>','compressed</h3>\n<br><a href="%s">%s</a><br>'%(name,name))
      
        self._tarJobDir(path,prod,job)
        open(os.path.join(jobpath,'index.html'),'w').write(lines)
	numberOfTared += 1
      except Exception,x:
        gLogger.warn( "Exception during taring %s"%x, "Production %s, Job %s" %(prod,job)  )
	numberOfFailed += 1

    gLogger.info( "Number of tared jobs %d"%numberOfTared )
    gLogger.info( "Number of failed jobs %d"%numberOfFailed )

    return S_OK()

  def _iFindOldJob(self,path, g1, g2, g3, agedays):  

    p1 = '^\d{8}$'
    c1 = re.compile(p1)
    p2 = '^\d{4}$'
    c2 = re.compile(p2)
    p3 = '^\d{8}$'
    c3 = re.compile(p3)

    def iFindDir(path,gl,reobject):

      dirs = glob.glob(os.path.join(path,gl))
      for d in dirs:
        name = os.path.basename(d)
        if reobject.match(name) and os.path.isdir(d):
          yield d
   
    for d1 in iFindDir(path, g1, c1):
      for d2 in iFindDir(d1, g2, c2):
        for d3 in iFindDir(d2, os.path.basename(d2)+g3, c3):
          mtime = os.stat(d3)[8]
          modified = datetime.fromtimestamp(mtime)
          if datetime.now()-modified>timedelta(days=agedays):
            prod = os.path.basename(d1)
            job = os.path.basename(d3)
            name = prod + "_" + job + ".tgz"
            if not os.path.exists(os.path.join(d3,name)):
              yield d3

  def _tarJobDir(self,path,prod,job):

    oldpath = os.getcwd()
    try:
      name = prod + "_" + job + ".tgz"
      jobpath = os.path.join(path, prod, job[0:4],job)
      files = os.listdir(jobpath)
      os.chdir(jobpath)

      tarFile = tarfile.open(name,"w:gz")
      for f in files:
        tarFile.add(f)
      tarFile.close()
      for f in files:
        os.remove(f)
    finally:
      os.chdir(oldpath)
