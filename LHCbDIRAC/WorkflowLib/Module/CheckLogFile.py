########################################################################
# $Id: CheckLogFile.py,v 1.12 2008/03/04 15:04:37 joel Exp $
########################################################################
""" Script Base Class """

__RCSID__ = "$Id: CheckLogFile.py,v 1.12 2008/03/04 15:04:37 joel Exp $"

import commands, os

from DIRAC.Core.Utilities.Subprocess                     import shellCall
from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC.DataManagementSystem.Client.ReplicaManager    import ReplicaManager
from WorkflowLib.Utilities.Tools import *
#from DIRAC.DataManagementSystem.Client.PoolXMLCatalog    import PoolXMLCatalog
from DIRAC import                                        S_OK, S_ERROR, gLogger, gConfig


class CheckLogFile(object):

  def __init__(self):
      self.log = gLogger.getSubLogger("CheckLogFile")
      self.result = S_ERROR()
      self.mailadress = 'None'
      self.appName = 'None'
      self.inputData = None
      self.JOB_ID = None
      self.poolXMLCatName = 'pool_xml_catalog.xml'
      self.appVersion = 'None'
      pass

  def execute(self):
      self.log.info( "Execute method to be implemented in the derived classes" )

  def sendErrorMail(self,subj = ''):
    try:
        if self.EMAIL:
            self.mailadress = self.EMAIL
        elif self.appName.upper()+'_EMAIL':
            self.mailadress = self.appName.upper()+'_EMAIL'
    except:
        self.log.warn('No EMAIL adress supplied')

    jid = ''
    if os.environ.has_key('JOBID'):
      jid = str(os.environ['JOBID'])

    dataTypes = ['SIM','DIGI','DST','RAW','ETC','SETC','FETC','RDST','MDF']

    for inputname in self.inputData.split(';'):
      self.LFN_ROOT = ''
      lfnroot = inputname.split('/')
      if len(lfnroot) > 1:
          CONTINUE = 1
          j = 1
          while CONTINUE == 1:
            if not lfnroot[j] in dataTypes:
              self.LFN_ROOT = self.LFN_ROOT+'/'+lfnroot[j]
            else:
              CONTINUE = 0
              break
            j = j + 1
            if j > len(lfnroot):
              CONTINUE = 0
              break

    subject = self.appName+' '+ self.appVersion + \
              " "+subj+' '+self.PRODUCTION_ID+'_'+self.JOB_ID+' JobID='+jid

    self.mode = gConfig.getValue('/LocalSite/Setup','Setup')
    # a convertir
    logpath = makeProductionPath(self.JOB_ID,self.LFN_ROOT,'LOG',self.mode,self.PRODUCTION_ID,log=True)
#    logpath = '/lhcb/test/DIRAC3/'+self.prod_id+'/'+self.job_id

    lfile = open('logmail','w')
    # If there are inpud data to the step, upload it and
    # send the file names in the mail
    if self.inputData:
      lfile.write('\n\nInput Data:\n')
#      lfile.write(self.inputData+'\n')
      for ind in self.inputData.split(';'):
        lfile.write(ind+'\n')


      retVal = gConfig.getSections('/Resources/StorageElements/CERN-Debug')
      if retVal['OK']:
        debugse = 'CERN-Debug'
      else:
        debugse = 'None'

      print debugse
      debugse = 'None'
      #  il faut traiter le cas fichier local ou fichier sur SE cas du reprocsessing
      if debugse != 'None':
        rm = ReplicaManager()
        for ind in self.inputData.split(';'):
          self.log.info( "Sending %s to %s" % ( str( ind ), str( debugse ) ) )
          pfnName = getPFNFromPoolXMLCatalog(self.poolXMLCatName,ind.split(':')[1])
          result = rm.putAndRegister(ind.split(':')[1],pfnName,debugse)
#          result = rm.replicate(ind.split(':')[1],debugse,'CERN-tape',destPath='/lhcb/failover')
          if not result['OK']:
            self.log.error( "Transfer to %s failed\n%s" % ( debugse, result['Message'] ) )
          else:
            self.log.info( "Transfer to %s successful" % debugse )


    if self.appLog:

      logse = gConfig.getOptions('/Resources/StorageElements/LogSE')
      logurl = 'http://lhcb-logs.cern.ch/storage'+logpath
#  a definir
#      ses = gConfig.getValue('http')
#      if ses:
        # HTTP protocol is defined for LogSE
#        logurl = ses[0].getPFNBase()+logpath
#      else:
        # HTTP protocol is not defined for LogSE
#        try:
#          logurl = cfgSvc.get(job_mode,'LogPathHTTP')+logpath
#        except:
          # Default url
#          logurl = 'http://lxb2003.cern.ch/storage'+logpath

      lfile.write('\n\nLog Files directory for the job:\n')
      url = logurl+'/'+ self.JOB_ID+'/'
      lfile.write(url+'\n')
      lfile.write('\n\nLog File for the problematic step:\n')
      url = logurl+'/'+ self.JOB_ID+'/'+ self.appLog + '.gz'
      lfile.write(url+'\n')
      lfile.write('\n\nJob StdOut:\n')
      url = logurl+'/'+ self.JOB_ID+'/std.out'
      lfile.write(url+'\n')
      lfile.write('\n\nJob StdErr:\n')
      url = logurl+'/'+ self.JOB_ID+'/std.err'
      lfile.write(url+'\n')

    lfile.close()

    self.log.info( "Sending mail to %s : %s" % ( self.mailadress, subject ) )
    if os.path.exists('corecomm.log'):
      comm = 'cat corecomm.log logmail | mail -s "'+subject+'" '+ self.mailadress
    else:
      comm = 'cat logmail | mail -s "'+subject+'" '+ self.mailadress

    self.result = shellCall(0,comm)
    #status = 0
    if self.result['Value'][0]:
      self.log.error( "Sending mail failed %s" % str( self.result['Value'][1] ) )

  def analyseCore(self):

    """ Check, if there is a core file produced by the application,
analyse the core file with gdb and send mail to the application
manager """

    files = os.listdir('.')
    for c in files:
      res = re.search('^core',c)
      if res is not None:

        # Send the mail to a software manager
        subject = 'Core found'

        # Generate core analysis by gdb
        cfile = open('corecomm','w')
        cfile.write('#!/bin/sh\n')
        cfile.write('touch corecomm.log\n')
        cfile.write('gdb '+self.module.step.applicationName+'.exe core* >> corecomm.log << EOF\n')
        cfile.write('where\n')
        cfile.write('quit\n')
        cfile.write('EOF\n')
        cfile.write('\n')
        cfile.write('exit 0\n')
        cfile.close()
        os.chmod('corecomm',0777)
        status,out=commands.getstatusoutput('/bin/sh -c ./corecomm')
        gLog.info( "%s %s" % ( str( status ), str( out ) ) )
        status,out=commands.getstatusoutput('rm -f core*')
        gLog.info( "%s %s" % ( str( status ), str( out ) ) )
        return S_ERROR()

    return S_OK()
