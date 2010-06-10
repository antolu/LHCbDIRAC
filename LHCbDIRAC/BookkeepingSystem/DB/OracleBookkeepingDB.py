########################################################################
# $Id$
########################################################################
"""

"""

__RCSID__ = "$Id$"

from LHCbDIRAC.BookkeepingSystem.DB.IBookkeepingDB                   import IBookkeepingDB
from types                                                           import *
from DIRAC                                                           import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config                         import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder                     import getDatabaseSection
from DIRAC.Core.Utilities.OracleDB                                   import OracleDB
import datetime
import types
global ALLOWED_ALL 
ALLOWED_ALL = 2
class OracleBookkeepingDB(IBookkeepingDB):
  
  #############################################################################
  def __init__(self):
    """
    """
    super(OracleBookkeepingDB, self).__init__()
    self.cs_path = getDatabaseSection('Bookkeeping/BookkeepingDB')
    
    self.dbHost = ''
    result = gConfig.getOption( self.cs_path+'/LHCbDIRACBookkeepingTNS')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: Host')
      return
    self.dbHost = result['Value']
    
    self.dbUser = ''
    result = gConfig.getOption( self.cs_path+'/LHCbDIRACBookkeepingUser')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: User')
      return
    self.dbUser = result['Value']
    
    self.dbPass = ''
    result = gConfig.getOption( self.cs_path+'/LHCbDIRACBookkeepingPassword')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: User')
      return
    self.dbPass = result['Value']


    self.dbServer = ''
    result = gConfig.getOption( self.cs_path+'/LHCbDIRACBookkeepingServer')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: User')
      return
    self.dbServer = result['Value']

    self.dbW_ = OracleDB(self.dbServer, self.dbPass, self.dbHost)
    self.dbR_ = OracleDB(self.dbUser, self.dbPass, self.dbHost)
    
  #############################################################################
  def getAvailableConfigurations(self):
    """
    """
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getAvailableConfigurations',[])

  #############################################################################
  def getAvailableFileTypes(self):
    command = ' select distinct filetypes.name from filetypes'
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def insertFileTypes(self, ftype, desc):
    command = 'select max(filetypeid) from filetypes'
    res = self.dbR_._query(command)
    if res['OK']:
      value = res['Value']
      if len(value) > 0:
        id = int(value[0][0]) + 1
        command = ' insert into filetypes (description, filetypeid, name, version) values(\''+desc+'\','+str(id)+',\''+ftype+'\' , \'ROOT_All\')'
        res = self.dbW_._query(command)
        return res
      else:
        return S_ERROR('Can not create a new filetypeID')
    else:
      return S_ERROR(res['Message'])
        
        
  #############################################################################
  def getAvailableConfigNames(self):
    command = ' select distinct Configname from bookkeepingview'
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getAvailableProductions(self):
    command = ' select distinct production from bookkeepingview where production > 0'
    res = self.dbR_._query(command)
    return res
  
  def getAvailableRuns(self):
    command = ' select distinct production from bookkeepingview where production < 0'
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getConfigVersions(self, configname):
    command = ' select distinct configversion from bookkeepingview where configname=\''+configname+'\''
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getSimulationConditions(self, configName, configVersion, realdata):
    condition = ' and bookkeepingview.configname=\''+configName+'\' and \
                bookkeepingview.configversion=\''+configVersion+'\''
    if realdata==0:
      command = 'select distinct SIMID, SIMDESCRIPTION,BEAMCOND,BEAMENERGY,GENERATOR,MAGNETICFIELD,DETECTORCOND,LUMINOSITY \
                    from simulationConditions,bookkeepingView where            \
                    bookkeepingview.DAQPeriodId=simulationConditions.simid' + condition
      res = self.dbR_._query(command)
      return res
    else:
      command = 'select data_taking_conditions.DAQPERIODID,data_taking_conditions.DESCRIPTION,data_taking_conditions.BEAMCOND, \
                data_taking_conditions.BEAMENERGY,data_taking_conditions.MAGNETICFIELD,data_taking_conditions.VELO,data_taking_conditions.IT, \
               data_taking_conditions.TT,data_taking_conditions.OT,data_taking_conditions.RICH1,data_taking_conditions.RICH2,data_taking_conditions.SPD_PRS, \
             data_taking_conditions.ECAL, data_taking_conditions.HCAL, data_taking_conditions.MUON, data_taking_conditions.L0, data_taking_conditions.HLT, data_taking_conditions.VeloPosition\
          from data_taking_conditions,bookkeepingView where \
               bookkeepingview.DAQPeriodId=data_taking_conditions.DAQPERIODID'+ condition
      res = self.dbR_._query(command)
      return res
    return S_ERROR("Data not found!")
               
  #############################################################################
  def getAvailableEventTypes(self):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getAvailableEventTypes', [])
  
  #############################################################################
  def getEventTypes(self, configName, configVersion):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getEventTypes', [configName, configVersion])
  
   #nnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnnneeeeeeeeeeeeeeeeeeewwwwwwwwwwwwwwwwwwwwwww
  #############################################################################
  def getProPassWithSimCondNew(self, configName, configVersion, simcondid):
    
    condition = ' and bview.configname=\''+configName+'\' and \
                bview.configversion=\''+configVersion+'\''
    
    if simcondid !='ALL':
      condition += ' and bview.DAQPeriodId='+str(simcondid)
    
    command = ' select distinct prod.passid, prod.TOTALPROCPASS,   \
     a0.applicationname s0, a0.applicationversion v0, a0.optionfiles op0, a0.dddb d0, a0.conddb c0, a0.EXTRAPACKAGES e0, \
     a1.applicationname s1, a1.applicationversion v1, a1.optionfiles op1, a1.dddb d1, a1.conddb c1, a1.EXTRAPACKAGES e1, \
     a2.applicationname s2, a2.applicationversion v2, a2.optionfiles op2, a2.dddb d2, a2.conddb c2, a2.EXTRAPACKAGES e2, \
     a3.applicationname s3, a3.applicationversion v3, a3.optionfiles op3, a3.dddb d3, a3.conddb c3, a3.EXTRAPACKAGES e3, \
     a4.applicationname s4, a4.applicationversion v4, a4.optionfiles op4, a4.dddb d4, a4.conddb c4, a4.EXTRAPACKAGES e4, \
     a5.applicationname s5, a5.applicationversion v5, a5.optionfiles op5, a5.dddb d5, a5.conddb c5, a5.EXTRAPACKAGES e5, \
     a6.applicationname s6, a6.applicationversion v6, a6.optionfiles op6, a6.dddb d6, a6.conddb c6, a6.EXTRAPACKAGES e6  \
   from bookkeepingview bview, productions prod,pass_index pi , \
        applications a0, applications a1, applications a2, applications a3, applications a4, applications a5, applications a6 \
   where  \
      bview.production=prod.production and \
      prod.passid=pi.passid and \
      pi.step0=a0.applicationid(+) and \
      pi.step1=a1.applicationid(+) and \
      pi.step2=a2.applicationid(+) and \
      pi.step3=a3.applicationid(+) and \
      pi.step4=a4.applicationid(+) and \
      pi.step5=a5.applicationid(+) and \
      pi.step6=a6.applicationid(+) ' + condition
       
    res = self.dbR_._query(command)
    if not res['OK']:
      return S_ERROR(res['Message'])
    value = res['Value']
    retvalue = []
    description = ''
    for one in value:
      tmp = list(one)
      groups = tmp[1].split('<')
      description = ''
      for group in groups:
        result = self.getDescription(group)['Value'][0][0]
        description += result +' + '
      tmp[1]=description[:-3] 
      retvalue += [tuple(tmp)]
    return S_OK(retvalue)
  
  #############################################################################
  def getProPassWithSimCond(self, configName, configVersion, simcondid):
    
    condition = ' and bookkeepingview.configname=\''+configName+'\' and \
                bookkeepingview.configversion=\''+configVersion+'\''
    
    if simcondid !='ALL':
      condition += ' and bookkeepingview.DAQPeriodId='+str(simcondid)
                
    command = 'select distinct productions.TOTALPROCPASS,  \
               pass_index.step0,pass_index.step1,pass_index.step2,pass_index.step3,pass_index.step4,pass_index.step5,pass_index.step6 from bookkeepingview,productions,pass_index,applications where \
               bookkeepingview.production=productions.production and \
               productions.passid=pass_index.passid'+condition
    res = self.dbR_._query(command)
    value = res['Value']
    retvalue = []
    description = ''
    for one in value:
      tmp = list(one)
      groups = tmp[0].split('<')
      description = ''
      for group in groups:
        result = self.getDescription(group)['Value'][0][0]
        description += result +' + '
      tmp[0]=description[:-3] 
      for appid in range(1,7):
        appname = ''
        if tmp[appid]!= None:
          retVal = self._getApplication(tmp[appid])
          if retVal['OK']:
            value = retVal['Value']
            appname = value[0][1]+' '+value[0][2]
            gLogger.info('Application name:',appname)
            infos = '/'+str(value[0][3])+'/'+str(value[0][4])+'/'+str(value[0][5])+'/'+str(value[0][6])
            tmp[appid]=appname+infos
          else:
            return S_ERROR(retVal['Message'])
      
      retvalue += [tuple(tmp)]
    return S_OK(retvalue)

  
  #############################################################################
  def getMoreProductionInformations(self, prodid):
    command = 'select bookkeepingview.configname, bookkeepingview.configversion, bookkeepingview.ProgramName, bookkeepingview.programversion from bookkeepingview where bookkeepingview.production='+str(prodid)
    res = self.dbR_._query(command)
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      record = res['Value']
      cname = record[0][0]
      cversion = record[0][1]
      pname = record[0][2]
      pversion = record[0][3]
    
    command = 'select totalprocpass, simcondid from productions where production='+str(prodid)
    res = self.dbR_._query(command)
    simid = 0
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      record = res['Value']
      p = record[0][0]
      simid = record[0][1]
      description = ''
      groups = p.split('<')
      procdescription = ''
      for group in groups:
        result = self.getDescription(group)['Value'][0][0]
        procdescription += result +' + '
      procdescription = procdescription[:-3]
    simdesc = ''
    daqdesc = ''
    command = 'select simdescription from simulationconditions where simid='+str(simid)
    res = self.dbR_._query(command)
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      value = res['Value']
      if len(value) != 0:
        simdesc = value[0][0]
      else:
        command = 'select description from data_taking_conditions where daqperiodid='+str(simid)
        res = self.dbR_._query(command)
        if not res['OK']:
          return S_ERROR(res['Message'])
        else:
          value = res['Value']
          if len(value) != 0:
            daqdesc = value[0][0]
          else:
            return S_ERROR('Simulation condition or data taking condition not exist!')
    if simdesc != '':
      return S_OK({'ConfigName':cname,'ConfigVersion':cversion,'ProgramName':pname,'ProgramVersion':pversion,'Processing pass':procdescription,'Simulation conditions':simdesc})
    else:
      return S_OK({'ConfigName':cname,'ConfigVersion':cversion,'ProgramName':pname,'ProgramVersion':pversion,'Processing pass':procdescription,'Data taking conditions':daqdesc})
  
  #############################################################################
  def getPassindex_old(self, passid):
    command='select distinct pi.description, g.groupdescription,   \
     a0.applicationname s0, a0.applicationversion v0, a0.optionfiles op0, a0.dddb d0, a0.conddb c0, a0.EXTRAPACKAGES e0, \
     a1.applicationname s1, a1.applicationversion v1, a1.optionfiles op1, a1.dddb d1, a1.conddb c1, a1.EXTRAPACKAGES e1, \
     a2.applicationname s2, a2.applicationversion v2, a2.optionfiles op2, a2.dddb d2, a2.conddb c2, a2.EXTRAPACKAGES e2, \
     a3.applicationname s3, a3.applicationversion v3, a3.optionfiles op3, a3.dddb d3, a3.conddb c3, a3.EXTRAPACKAGES e3, \
     a4.applicationname s4, a4.applicationversion v4, a4.optionfiles op4, a4.dddb d4, a4.conddb c4, a4.EXTRAPACKAGES e4, \
     a5.applicationname s5, a5.applicationversion v5, a5.optionfiles op5, a5.dddb d5, a5.conddb c5, a5.EXTRAPACKAGES e5, \
     a6.applicationname s6, a6.applicationversion v6, a6.optionfiles op6, a6.dddb d6, a6.conddb c6, a6.EXTRAPACKAGES e6 \
   from pass_index pi , pass_group g, \
        applications a0, applications a1, applications a2, applications a3, applications a4, applications a5, applications a6 \
   where      \
      g.groupid=pi.groupid         and \
      pi.step0=a0.applicationid(+) and \
      pi.step1=a1.applicationid(+) and \
      pi.step2=a2.applicationid(+) and \
      pi.step3=a3.applicationid(+) and \
      pi.step4=a4.applicationid(+) and \
      pi.step5=a5.applicationid(+) and \
      pi.step6=a6.applicationid(+) and \
      pi.passid='+str(passid)
    res = self.dbR_._query(command)
    return res
  
  def getPassIndex(self, passdesc):
    command='select distinct pi.description, g.groupdescription,   \
     a0.applicationname s0, a0.applicationversion v0, a0.optionfiles op0, a0.dddb d0, a0.conddb c0, a0.EXTRAPACKAGES e0, \
     a1.applicationname s1, a1.applicationversion v1, a1.optionfiles op1, a1.dddb d1, a1.conddb c1, a1.EXTRAPACKAGES e1, \
     a2.applicationname s2, a2.applicationversion v2, a2.optionfiles op2, a2.dddb d2, a2.conddb c2, a2.EXTRAPACKAGES e2, \
     a3.applicationname s3, a3.applicationversion v3, a3.optionfiles op3, a3.dddb d3, a3.conddb c3, a3.EXTRAPACKAGES e3, \
     a4.applicationname s4, a4.applicationversion v4, a4.optionfiles op4, a4.dddb d4, a4.conddb c4, a4.EXTRAPACKAGES e4, \
     a5.applicationname s5, a5.applicationversion v5, a5.optionfiles op5, a5.dddb d5, a5.conddb c5, a5.EXTRAPACKAGES e5, \
     a6.applicationname s6, a6.applicationversion v6, a6.optionfiles op6, a6.dddb d6, a6.conddb c6, a6.EXTRAPACKAGES e6 \
   from pass_index pi , pass_group g, \
        applications a0, applications a1, applications a2, applications a3, applications a4, applications a5, applications a6 \
   where      \
      g.groupid=pi.groupid         and \
      pi.step0=a0.applicationid(+) and \
      pi.step1=a1.applicationid(+) and \
      pi.step2=a2.applicationid(+) and \
      pi.step3=a3.applicationid(+) and \
      pi.step4=a4.applicationid(+) and \
      pi.step5=a5.applicationid(+) and \
      pi.step6=a6.applicationid(+) and \
      g.groupdescription='+str(passdesc)
    res = self.dbR_._query(command)
    return res
  
  def getProcessingPassDescfromProduction(self, prod):
    result = {}
    processing = {}
    parametersNames = ['Steps','PassDescription', 'Groupdescription','Application Name','Application Version','Option files','DDDb', 'CondDb','Extra Packages']
    records = []
    condition = ''
    condition += ' and prod.production='+str(prod)
    command =' select distinct pi.description, g.groupdescription,   \
   a0.applicationname s0, a0.applicationversion v0, a0.optionfiles op0, a0.dddb d0, a0.conddb c0, a0.EXTRAPACKAGES e0, \
   a1.applicationname s1, a1.applicationversion v1, a1.optionfiles op1, a1.dddb d1, a1.conddb c1, a1.EXTRAPACKAGES e1, \
   a2.applicationname s2, a2.applicationversion v2, a2.optionfiles op2, a2.dddb d2, a2.conddb c2, a2.EXTRAPACKAGES e2, \
   a3.applicationname s3, a3.applicationversion v3, a3.optionfiles op3, a3.dddb d3, a3.conddb c3, a3.EXTRAPACKAGES e3, \
   a4.applicationname s4, a4.applicationversion v4, a4.optionfiles op4, a4.dddb d4, a4.conddb c4, a4.EXTRAPACKAGES e4, \
   a5.applicationname s5, a5.applicationversion v5, a5.optionfiles op5, a5.dddb d5, a5.conddb c5, a5.EXTRAPACKAGES e5, \
   a6.applicationname s6, a6.applicationversion v6, a6.optionfiles op6, a6.dddb d6, a6.conddb c6, a6.EXTRAPACKAGES e6 \
 from pass_index pi , pass_group g, productions prod, \
      applications a0, applications a1, applications a2, applications a3, applications a4, applications a5, applications a6  \
 where   \
    g.groupid=pi.groupid         and \
    pi.step0=a0.applicationid(+) and \
    pi.step1=a1.applicationid(+) and \
    pi.step2=a2.applicationid(+) and \
    pi.step3=a3.applicationid(+) and \
    pi.step4=a4.applicationid(+) and \
    pi.step5=a5.applicationid(+) and \
    pi.step6=a6.applicationid(+) and \
    prod.passid=pi.passid' + condition
    
    res = self.dbR_._query(command)
    if not res['OK']:
      return S_ERROR(res['Message'])
    value = res['Value']
    nb = 0
    
    for record in value:
      nb += 1
      s0 = ['Step0', record[0],record[1],str(record[2]),record[3],record[4],str(record[5]),str(record[6]),record[7]]
      s1 = ['Step1', record[0],record[1], str(record[8]),record[9],record[10],str(record[11]),str(record[12]),record[13]]
      s2 = ['Step2', record[0],record[1], str(record[14]),record[15],record[16],str(record[17]),str(record[18]),record[19]]
      s3 = ['Step3', record[0],record[1], str(record[20]),record[21],record[22],str(record[23]),str(record[24]),record[25]]
      s4 = ['Step4', record[0],record[1], str(record[26]),record[27],record[28],str(record[29]),str(record[30]),record[31]]
      records = [s0,s1,s2,s3,s4]
      processing[record[0]] = records
    result = {'Parameters':parametersNames,'Records':processing, 'TotalRecords':nb}
    return S_OK(result)
  
  def getProcessingPassDesc_new(self, totalproc, simid = 'ALL'):
    result = {}
    processing = {}
    parametersNames = ['Steps','PassDescription', 'Groupdescription','Application Name','Application Version','Option files','DDDb', 'CondDb','Extra Packages']
    records = []
    proc = totalproc.split('+')
    for group in proc:
      condition = ''
      if simid != 'ALL':
        condition += ' and prod.simcondid='+str(simid)
      condition += ' and g.groupdescription=\''+str(group.strip())+'\''
      command =' select distinct pi.description, g.groupdescription,   \
     a0.applicationname s0, a0.applicationversion v0, a0.optionfiles op0, a0.dddb d0, a0.conddb c0, a0.EXTRAPACKAGES e0, \
     a1.applicationname s1, a1.applicationversion v1, a1.optionfiles op1, a1.dddb d1, a1.conddb c1, a1.EXTRAPACKAGES e1, \
     a2.applicationname s2, a2.applicationversion v2, a2.optionfiles op2, a2.dddb d2, a2.conddb c2, a2.EXTRAPACKAGES e2, \
     a3.applicationname s3, a3.applicationversion v3, a3.optionfiles op3, a3.dddb d3, a3.conddb c3, a3.EXTRAPACKAGES e3, \
     a4.applicationname s4, a4.applicationversion v4, a4.optionfiles op4, a4.dddb d4, a4.conddb c4, a4.EXTRAPACKAGES e4, \
     a5.applicationname s5, a5.applicationversion v5, a5.optionfiles op5, a5.dddb d5, a5.conddb c5, a5.EXTRAPACKAGES e5, \
     a6.applicationname s6, a6.applicationversion v6, a6.optionfiles op6, a6.dddb d6, a6.conddb c6, a6.EXTRAPACKAGES e6 \
   from pass_index pi , pass_group g, productions prod, \
        applications a0, applications a1, applications a2, applications a3, applications a4, applications a5, applications a6  \
   where   \
      g.groupid=pi.groupid         and \
      pi.step0=a0.applicationid(+) and \
      pi.step1=a1.applicationid(+) and \
      pi.step2=a2.applicationid(+) and \
      pi.step3=a3.applicationid(+) and \
      pi.step4=a4.applicationid(+) and \
      pi.step5=a5.applicationid(+) and \
      pi.step6=a6.applicationid(+) and \
      prod.passid=pi.passid' + condition
      
      res = self.dbR_._query(command)
      if not res['OK']:
        return S_ERROR(res['Message'])
      value = res['Value']
      nb = 0
      
      for record in value:
        nb += 1
        s0 = ['Step0', record[0],record[1],str(record[2]),record[3],record[4],str(record[5]),str(record[6]),record[7]]
        s1 = ['Step1', record[0],record[1], str(record[8]),record[9],record[10],str(record[11]),str(record[12]),record[13]]
        s2 = ['Step2', record[0],record[1], str(record[14]),record[15],record[16],str(record[17]),str(record[18]),record[19]]
        s3 = ['Step3', record[0],record[1], str(record[20]),record[21],record[22],str(record[23]),str(record[24]),record[25]]
        s4 = ['Step4', record[0],record[1], str(record[26]),record[27],record[28],str(record[29]),str(record[30]),record[31]]
        records = [s0,s1,s2,s3,s4]
        processing[record[0]] = records
      result = {'Parameters':parametersNames,'Records':processing, 'TotalRecords':nb}
    return S_OK(result)
  
  #############################################################################
  def getProcessingPassDesc(self, totalproc, passid, simid = 'ALL'):
    result = {}
    proc = totalproc.split('+')
    retVal = self.getPassindex_old(passid)
    processing = {}
    if not retVal['OK']:
      return S_ERROR(retVal)
    value = retVal['Value']
    parametersNames = ['Steps','PassDescription', 'Groupdescription','Application Name','Application Version','Option files','DDDb', 'CondDb','Extra Packages']
    records = []
    for record in value:
      s0 = ['Step0', record[0],record[1],str(record[2]),record[3],record[4],str(record[5]),str(record[6]),record[7]]
      s1 = ['Step1', record[0],record[1], str(record[8]),record[9],record[10],str(record[11]),str(record[12]),record[13]]
      s2 = ['Step2', record[0],record[1], str(record[14]),record[15],record[16],str(record[17]),str(record[18]),record[19]]
      s3 = ['Step3', record[0],record[1], str(record[20]),record[21],record[22],str(record[23]),str(record[24]),record[25]]
      s4 = ['Step4', record[0],record[1], str(record[26]),record[27],record[28],str(record[29]),str(record[30]),record[31]]
      records += [s0,s1,s2,s3,s4]
      processing[record[0]] = records
    if len(proc) == 1:
      result = {'Parameters':parametersNames,'Records':processing, 'TotalRecords':1}
    else:
      for group in proc:
        condition = ''
        if simid != 'ALL':
          condition += ' and prod.simcondid='+str(simid)
        condition += ' and g.groupdescription=\''+str(group.strip())+'\''
        command =' select distinct pi.description, g.groupdescription,   \
       a0.applicationname s0, a0.applicationversion v0, a0.optionfiles op0, a0.dddb d0, a0.conddb c0, a0.EXTRAPACKAGES e0, \
       a1.applicationname s1, a1.applicationversion v1, a1.optionfiles op1, a1.dddb d1, a1.conddb c1, a1.EXTRAPACKAGES e1, \
       a2.applicationname s2, a2.applicationversion v2, a2.optionfiles op2, a2.dddb d2, a2.conddb c2, a2.EXTRAPACKAGES e2, \
       a3.applicationname s3, a3.applicationversion v3, a3.optionfiles op3, a3.dddb d3, a3.conddb c3, a3.EXTRAPACKAGES e3, \
       a4.applicationname s4, a4.applicationversion v4, a4.optionfiles op4, a4.dddb d4, a4.conddb c4, a4.EXTRAPACKAGES e4, \
       a5.applicationname s5, a5.applicationversion v5, a5.optionfiles op5, a5.dddb d5, a5.conddb c5, a5.EXTRAPACKAGES e5, \
       a6.applicationname s6, a6.applicationversion v6, a6.optionfiles op6, a6.dddb d6, a6.conddb c6, a6.EXTRAPACKAGES e6 \
     from pass_index pi , pass_group g, productions prod, \
          applications a0, applications a1, applications a2, applications a3, applications a4, applications a5, applications a6  \
     where   \
        g.groupid=pi.groupid         and \
        pi.step0=a0.applicationid(+) and \
        pi.step1=a1.applicationid(+) and \
        pi.step2=a2.applicationid(+) and \
        pi.step3=a3.applicationid(+) and \
        pi.step4=a4.applicationid(+) and \
        pi.step5=a5.applicationid(+) and \
        pi.step6=a6.applicationid(+) and \
        prod.passid=pi.passid' + condition
        
        res = self.dbR_._query(command)
        if not res['OK']:
          return S_ERROR(res['Message'])
        value = res['Value']
        nb = 0
        
        for record in value:
          nb += 1
          s0 = ['Step0', record[0],record[1],str(record[2]),record[3],record[4],str(record[5]),str(record[6]),record[7]]
          s1 = ['Step1', record[0],record[1], str(record[8]),record[9],record[10],str(record[11]),str(record[12]),record[13]]
          s2 = ['Step2', record[0],record[1], str(record[14]),record[15],record[16],str(record[17]),str(record[18]),record[19]]
          s3 = ['Step3', record[0],record[1], str(record[20]),record[21],record[22],str(record[23]),str(record[24]),record[25]]
          s4 = ['Step4', record[0],record[1], str(record[26]),record[27],record[28],str(record[29]),str(record[30]),record[31]]
          records = [s0,s1,s2,s3,s4]
          processing[record[0]] = records
        result = {'Parameters':parametersNames,'Records':processing, 'TotalRecords':nb}
    return S_OK(result)
  
  #############################################################################
  def _getApplication(self, applid):
    command = 'select * from applications where applicationid='+str(applid)
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getDescription(self, id):
    command = 'select groupdescription from pass_group where groupid='+str(id)
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getGroupId(self, description):
    command = 'select groupid from pass_group where groupdescription=\''+str(description)+'\''
    res = self.dbR_._query(command)
    return res
    
  #############################################################################
  def getEventTypeWithSimcond(self,configName, configVersion, simcondid, procPass):
    condition = ''
    if configName != 'ALL':
      condition += ' and bookkeepingview.configname=\''+configName+'\''
    if configVersion != 'ALL':
      condition += 'and bookkeepingview.configversion=\''+configVersion+'\''
    
    if simcondid != 'ALL':
      condition += ' and bookkeepingview.DAQPeriodId='+str(simcondid)
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
    
    command ='select distinct eventTypes.EventTypeId, eventTypes.Description from eventtypes,bookkeepingview,productions where \
         bookkeepingview.production=productions.production and \
         eventTypes.EventTypeId=bookkeepingview.eventtypeid'+condition
  
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getProductionsWithSimcond(self, configName, configVersion, simcondid, procPass, evtId):
    all = 0
    condition = ' and bookkeepingview.configname=\''+configName+'\' and \
                    bookkeepingview.configversion=\''+configVersion+'\''
    
    if simcondid != 'ALL':
      condition += ' and bookkeepingview.DAQPeriodId='+str(simcondid)
    else:
      all += 1
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
    else:
      all += 1
    
    if evtId != 'ALL':
      condition += ' and bookkeepingview.EventTypeId='+str(evtId)
    else:
      all += 1
    
    if all > ALLOWED_ALL:
      return S_ERROR('To many ALL selected')
    
    command = 'select distinct bookkeepingview.production from bookkeepingview,productions where \
         bookkeepingview.production=productions.production'+condition
  
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getFileTypesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod):

    condition = ''    
    if configName != 'ALL':
      condition += ' and bookkeepingview.configname=\''+configName+'\''
    
    if configVersion != 'ALL':
     condition += ' and bookkeepingview.configversion=\''+configVersion+'\''
    
    all = 0
    if simcondid != 'ALL':
      condition += ' and bookkeepingview.DAQPeriodId='+str(simcondid)
    else:
      all += 1
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
    else:
      all += 1
          
    if evtId != 'ALL':
      condition += ' and bookkeepingview.EventTypeId='+str(evtId)
    else:
        all += 1
    
    if prod != 'ALL':
      condition += ' and bookkeepingview.production='+str(prod)
    else:
      all += 1
    
    if all > ALLOWED_ALL:
      return S_ERROR("To many all selected")
    
    command = 'select distinct filetypes.name from filetypes,bookkeepingview,productions where \
               bookkeepingview.filetypeId=fileTypes.filetypeid and bookkeepingview.production=productions.production'+condition
               
    res = self.dbR_._query(command)
    return res
  
  def getProgramNameWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype):
    condition = ' and bookkeepingview.configname=\''+configName+'\' and \
                    bookkeepingview.configversion=\''+configVersion+'\''
    
    all = 0
    if simcondid != 'ALL':
      condition += ' and bookkeepingview.DAQPeriodId='+str(simcondid)
    else:
      all += 1
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
    else:
      all += 1
          
    if evtId != 'ALL':
      condition += ' and bookkeepingview.EventTypeId='+str(evtId)
    else:
      all += 1
    
    if prod != 'ALL':
      condition += ' and bookkeepingview.production='+str(prod)
    else:
      all += 1
    
    if ftype!= 'ALL':
      fType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fType)
      if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
      else:
        ftypeId = res['Value'][0][0]
        condition += ' and bookkeepingview.filetypeid='+str(ftypeId)
    else:
      all += 1
    
    if all > ALLOWED_ALL:
      return S_ERROR("To many ALL selected")
    command = 'select distinct bookkeepingview.ProgramName, bookkeepingView.ProgramVersion, 0 from filetypes,bookkeepingview,productions where \
               bookkeepingview.filetypeId=fileTypes.filetypeid and bookkeepingview.production=productions.production'+condition
               
    res = self.dbR_._query(command)
    return res
    
  #############################################################################
  def getFilesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion):
    condition = ''    
    if configName != 'ALL':
      condition += ' and configurations.ConfigName=\''+configName+'\''
    
    if configVersion != 'ALL':
     condition += ' and configurations.ConfigVersion=\''+configVersion+'\''
    
    all = 0;
    tables = ' jobs, files,configurations'
    if simcondid != 'ALL':
      condition += ' and jobs.production=productions.production'
      condition += ' and productions.simcondid='+str(simcondid)
      tables += ' ,productions'
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
      condition += ' and productions.PRODUCTION=jobs.production'
      if 'productions' not in tables:
         tables += ', productions'
    else:
      all += 1
      
    
    if evtId != 'ALL':
      condition += ' and files.EventTypeId='+str(evtId)
    else:
      all += 1
    
    if prod != 'ALL':
      condition += ' and jobs.Production='+str(prod)
    else:
      all += 1
    
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
      else:
        ftypeId = res['Value'][0][0]
        condition += ' and files.FileTypeId='+str(ftypeId)
    else:
      all += 1
    
    if progName != 'ALL' and progVersion != 'ALL':
      condition += ' and jobs.ProgramName=\''+progName+'\''
      condition += ' and jobs.ProgramVersion=\''+progVersion+'\''
    else:
      all +=1
         
    if ftype == 'ALL':
      command =' select files.FileName, files.EventStat, files.FileSize, files.CreationDate, \'Unkown\',\'Unkown\',\
         jobs.JobStart, jobs.JobEnd, jobs.WorkerNode, filetypes.Name, jobs.runnumber, jobs.fillnumber, files.fullstat, dataquality.dataqualityflag, jobs.eventinputstat from '+ tables+' ,filetypes, dataquality \
         where files.JobId=jobs.JobId and \
         jobs.configurationid=configurations.configurationid and \
         files.gotReplica=\'Yes\' and \
         files.filetypeid=filetypes.filetypeid and \
         files.qualityid= dataquality.qualityid' + condition 
      all +=1
    else:
      command =' select files.FileName, files.EventStat, files.FileSize, files.CreationDate,\'Unkown\',\'Unkown\',\
         jobs.JobStart, jobs.JobEnd, jobs.WorkerNode, \''+str(ftype)+'\' , jobs.runnumber, jobs.fillnumber, files.fullstat, dataquality.dataqualityflag, jobs.eventinputstat from '+ tables +' ,dataquality\
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\' and \
         jobs.configurationid=configurations.configurationid and \
         files.qualityid= dataquality.qualityid' + condition 
    if all > ALLOWED_ALL:
      return S_ERROR(" TO many ALL selected")
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getFilesWithSimcondAndDataQuality(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion, quality):
    condition = ''    
    if configName != 'ALL':
      condition += ' and configurations.ConfigName=\''+configName+'\''
    
    if configVersion != 'ALL':
     condition += ' and configurations.ConfigVersion=\''+configVersion+'\''
    
    all = 0;
    tables = ' jobs, files,configurations'
    if simcondid != 'ALL':
      condition += ' and jobs.production=productions.production'
      condition += ' and productions.simcondid='+str(simcondid)
      tables += ' ,productions'
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
      condition += ' and productions.PRODUCTION=jobs.production'
      if 'productions' not in tables:
         tables += ', productions'
    else:
      all += 1
      
    
    if evtId != 'ALL':
      condition += ' and files.EventTypeId='+str(evtId)
    else:
      all += 1
    
    if prod != 'ALL':
      condition += ' and jobs.Production='+str(prod)
    else:
      all += 1
    
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
      else:
        ftypeId = res['Value'][0][0]
        condition += ' and files.FileTypeId='+str(ftypeId)
    else:
      all += 1
    
    if progName != 'ALL' and progVersion != 'ALL':
      condition += ' and jobs.ProgramName=\''+progName+'\''
      condition += ' and jobs.ProgramVersion=\''+progVersion+'\''
    else:
      all +=1
    
    if len(quality) > 0:
      conds = ' ('
      for i in quality:
        quality = None
        command = 'select QualityId from dataquality where dataqualityflag=\''+str(i)+'\''
        res = self.dbR_._query(command)
        if not res['OK']:
          gLogger.error('Data quality problem:',res['Message'])
        elif len(res['Value']) == 0:
            return S_ERROR('Dataquality is missing!')
        else:
          quality = res['Value'][0][0]
        conds += ' files.qualityid='+str(quality)+' or'
      condition += 'and'+conds[:-3] + ')'
            
    if ftype == 'ALL':
      command =' select files.FileName, files.EventStat, files.FileSize, files.CreationDate, \'Unkown\',\'Unkown\',\
         jobs.JobStart, jobs.JobEnd, jobs.WorkerNode, filetypes.Name, jobs.runnumber, jobs.fillnumber, files.fullstat, dataquality.dataqualityflag, jobs.eventinputstat from '+ tables+' ,filetypes, dataquality \
         where files.JobId=jobs.JobId and \
         jobs.configurationid=configurations.configurationid and \
         files.gotReplica=\'Yes\' and \
         files.filetypeid=filetypes.filetypeid and \
         files.qualityid= dataquality.qualityid' + condition 
      all +=1
    else:
      command =' select files.FileName, files.EventStat, files.FileSize, files.CreationDate,\'Unkown\',\'Unkown\',\
         jobs.JobStart, jobs.JobEnd, jobs.WorkerNode, \''+str(ftype)+'\' , jobs.runnumber, jobs.fillnumber, files.fullstat, dataquality.dataqualityflag, jobs.eventinputstat from '+ tables +' ,dataquality\
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\' and \
         jobs.configurationid=configurations.configurationid and \
         files.qualityid= dataquality.qualityid' + condition 
    if all > ALLOWED_ALL:
      return S_ERROR(" TO many ALL selected")
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getLimitedFilesWithSimcond(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion, startitem, maxitems):
    
    condition = ''    
    if configName != 'ALL':
      condition += ' and configurations.ConfigName=\''+configName+'\''
    
    if configVersion != 'ALL':
     condition += ' and configurations.ConfigVersion=\''+configVersion+'\''
     
    tables = ' jobs, files,configurations'
    if simcondid != 'ALL':
      condition += ' and jobs.production=productions.production'
      condition += ' and productions.simcondid='+str(simcondid)
      tables += ' ,productions'
        
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        if desc != '':
          result = self.getGroupId(desc.strip())
          if not result['OK']:
            return S_ERROR(result['Message'])
          elif len(result['Value']) == 0:
            return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
          val = result['Value'][0][0]
          totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
      condition += ' and productions.PRODUCTION=jobs.production'
      if 'productions' not in tables:
         tables += ', productions'
      
    all = 0
    if evtId != 'ALL':
      condition += ' and files.EventTypeId='+str(evtId)
    else:
      all += 1
    
    if prod != 'ALL':
      condition += ' and jobs.Production='+str(prod)
    else:
      all += 1
    
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
      else:
        ftypeId = res['Value'][0][0]
        condition += ' and files.FileTypeId='+str(ftypeId)
    else:
      all += 1
    
    if progName != 'ALL' and progVersion != 'ALL':
      condition += ' and jobs.ProgramName=\''+progName+'\''
      condition += ' and jobs.ProgramVersion=\''+progVersion+'\''
    else:
      all += 1
         
    if ftype == 'ALL':
      command = 'select rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode, ftype, runnb,fillnb,fullst,quality, jeventinput \
      FROM \
       ( select rownum rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode,ftype,runnb,fillnb,fullst,quality,jeventinput \
          from( \
           select fileName fname, files.EventStat eventstat, files.FileSize fsize, files.CreationDate creation, \
            \'not used\' gen, \'not used\' geom, \
            jobs.JobStart jstart, jobs.JobEnd jend, jobs.WorkerNode wnode, filetypes.name ftype, \
        jobs.runnumber runnb, jobs.fillnumber fillnb, files.fullstat fullst, dataquality.dataqualityflag quality, jobs.eventinputstat jeventinput\
        from'+tables+',filetypes, dataquality \
         where files.JobId=jobs.JobId and \
         jobs.configurationid=configurations.configurationid and \
         files.gotReplica=\'Yes\' and \
         files.qualityid=dataquality.qualityid \
         files.filetypeid=filetypes.filetypeid' + condition + ' ) where rownum <= '+str(maxitems)+ ' ) where rnum > '+ str(startitem)
      all += 1
    else:
      command = 'select rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode, \''+str(ftype)+'\' , runnb,fillnb,fullst,quality, jeventinput from \
       ( select rownum rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode, runnb,fillnb,fullst,quality, jeventinput \
          from( \
           select fileName fname, files.EventStat eventstat, files.FileSize fsize, files.CreationDate creation, \
            \'not used\' gen, \'not used\' geom, \
            jobs.JobStart jstart, jobs.JobEnd jend, jobs.WorkerNode wnode, jobs.runnumber runnb, jobs.fillnumber fillnb, files.fullstat fullst, dataquality.dataqualityflag quality, jobs.eventinputstat jeventinput  \
        from'+ tables+', dataquality\
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\' and \
         files.qualityid=dataquality.qualityid and \
         jobs.configurationid=configurations.configurationid' + condition + ' ) where rownum <= ' + str(maxitems)+ ' ) where rnum > '+str(startitem)
      
    if all > ALLOWED_ALL:
      return S_ERROR("To many ALL selected")
    
    res = self.dbR_._query(command)
    return res
 
  #############################################################################
  def getLimitedFilesWithSimcondAndDataQuality(self, configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion, startitem, maxitems, quality):
    
    condition = ''    
    if configName != 'ALL':
      condition += ' and configurations.ConfigName=\''+configName+'\''
    
    if configVersion != 'ALL':
     condition += ' and configurations.ConfigVersion=\''+configVersion+'\''
     
    tables = ' jobs, files,configurations'
    if simcondid != 'ALL':
      condition += ' and jobs.production=productions.production'
      condition += ' and productions.simcondid='+str(simcondid)
      tables += ' ,productions'
        
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        if desc != '':
          result = self.getGroupId(desc.strip())
          if not result['OK']:
            return S_ERROR(result['Message'])
          elif len(result['Value']) == 0:
            return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
          val = result['Value'][0][0]
          totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
      condition += ' and productions.PRODUCTION=jobs.production'
      if 'productions' not in tables:
         tables += ', productions'
      
    all = 0
    if evtId != 'ALL':
      condition += ' and files.EventTypeId='+str(evtId)
    else:
      all += 1
    
    if prod != 'ALL':
      condition += ' and jobs.Production='+str(prod)
    else:
      all += 1
    
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
      else:
        ftypeId = res['Value'][0][0]
        condition += ' and files.FileTypeId='+str(ftypeId)
    else:
      all += 1
    
    if progName != 'ALL' and progVersion != 'ALL':
      condition += ' and jobs.ProgramName=\''+progName+'\''
      condition += ' and jobs.ProgramVersion=\''+progVersion+'\''
    else:
      all += 1
         
    if len(quality) > 0:
      conds = ' ('
      for i in quality:
        if quality[i] != False:
          qualityFlag = None
          command = 'select QualityId from dataquality where dataqualityflag=\''+str(i)+'\''
          res = self.dbR_._query(command)
          if not res['OK']:
            gLogger.error('Data quality problem:',res['Message'])
          elif len(res['Value']) == 0:
              return S_ERROR('Dataquality is missing!')
          else:
            qualityFlag = res['Value'][0][0]
          conds += ' files.qualityid='+str(qualityFlag)+' or'
      condition += 'and'+conds[:-3] + ')'
        
    if ftype == 'ALL':
      command = 'select rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode, ftype, runnb,fillnb,fullst,quality, jeventinput \
      FROM \
       ( select rownum rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode,ftype,runnb,fillnb,fullst,quality,jeventinput \
          from( \
           select fileName fname, files.EventStat eventstat, files.FileSize fsize, files.CreationDate creation, \
            \'not used\' gen, \'not used\' geom, \
            jobs.JobStart jstart, jobs.JobEnd jend, jobs.WorkerNode wnode, filetypes.name ftype, \
        jobs.runnumber runnb, jobs.fillnumber fillnb, files.fullstat fullst, dataquality.dataqualityflag quality, jobs.eventinputstat jeventinput\
        from'+tables+',filetypes, dataquality \
         where files.JobId=jobs.JobId and \
         jobs.configurationid=configurations.configurationid and \
         files.gotReplica=\'Yes\' and \
         files.qualityid=dataquality.qualityid \
         files.filetypeid=filetypes.filetypeid' + condition + ' ) where rownum <= '+str(maxitems)+ ' ) where rnum > '+ str(startitem)
      all += 1
    else:
      command = 'select rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode, \''+str(ftype)+'\' , runnb,fillnb,fullst,quality, jeventinput from \
       ( select rownum rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode, runnb,fillnb,fullst,quality, jeventinput \
          from( \
           select fileName fname, files.EventStat eventstat, files.FileSize fsize, files.CreationDate creation, \
            \'not used\' gen, \'not used\' geom, \
            jobs.JobStart jstart, jobs.JobEnd jend, jobs.WorkerNode wnode, jobs.runnumber runnb, jobs.fillnumber fillnb, files.fullstat fullst, dataquality.dataqualityflag quality, jobs.eventinputstat jeventinput  \
        from'+ tables+', dataquality\
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\' and \
         files.qualityid=dataquality.qualityid and \
         jobs.configurationid=configurations.configurationid' + condition + ' ) where rownum <= ' + str(maxitems)+ ' ) where rnum > '+str(startitem)
      
    if all > ALLOWED_ALL:
      return S_ERROR("To many ALL selected")
    
    res = self.dbR_._query(command)
    return res
    
  def getLimitedNbOfFiles(self,configName, configVersion, simcondid, procPass, evtId, prod, ftype, progName, progVersion):
    
    condition = ''    
    if configName != 'ALL':
      condition += ' and configurations.ConfigName=\''+configName+'\''
    
    if configVersion != 'ALL':
     condition += ' and configurations.ConfigVersion=\''+configVersion+'\''
    
    tables = ' jobs, files,configurations'
    if simcondid != 'ALL':
      condition += ' and jobs.production=productions.production'
      condition += ' and productions.simcondid='+str(simcondid)
      tables += ', productions' 
    

    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
      condition += ' and productions.PRODUCTION=jobs.production'
      if 'productions' not in tables:
         tables += ', productions'   
      
    all = 0
    if evtId != 'ALL':
      condition += ' and files.EventTypeId='+str(evtId)
    else:
      all += 1
    
    if prod != 'ALL':
      condition += ' and jobs.Production='+str(prod)
    else:
      all += 1
    
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
      else:
        ftypeId = res['Value'][0][0]
        condition += ' and files.FileTypeId='+str(ftypeId)
    else:
      all += 1
    
    if progName != 'ALL' and progVersion != 'ALL':
      condition += ' and jobs.ProgramName=\''+progName+'\''
      condition += ' and jobs.ProgramVersion=\''+progVersion+'\''
    else:
      all += 1
    if ftype == 'ALL':
      command =' select count(*), SUM(files.EventStat), SUM(files.FILESIZE) from '+ tables +', filetypes \
         where files.JobId=jobs.JobId and \
         jobs.configurationid=configurations.configurationid and \
         files.gotReplica=\'Yes\' and \
         files.filetypeid=filetypes.filetypeid' + condition 
      all += 1
    else:
      command =' select count(*), SUM(files.EventStat), SUM(files.FILESIZE) from ' + tables +' \
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\' and \
         jobs.configurationid=configurations.configurationid' + condition 
    
    if all > ALLOWED_ALL:
      return S_ERROR("To many ALL selected")
    
    res = self.dbR_._query(command)
    return res
    
  #############################################################################
  def getSimCondWithEventType(self, configName, configVersion, eventType, realdata = 0):
    condition = ' and bookkeepingview.configname=\''+configName+'\' and \
                    bookkeepingview.configversion=\''+configVersion+'\''
    
    if eventType != 'ALL':
      condition += ' and bookkeepingview.EventTypeId='+str(eventType)
    
    if realdata != 0:
      command = 'select data_taking_conditions.DAQPERIODID,data_taking_conditions.DESCRIPTION,data_taking_conditions.BEAMCOND, \
                data_taking_conditions.BEAMENERGY,data_taking_conditions.MAGNETICFIELD,data_taking_conditions.VELO,data_taking_conditions.IT, \
               data_taking_conditions.TT,data_taking_conditions.OT,data_taking_conditions.RICH1,data_taking_conditions.RICH2,data_taking_conditions.SPD_PRS, \
             data_taking_conditions.ECAL \
          from data_taking_conditions,bookkeepingView where \
               bookkeepingview.DAQPeriodId=data_taking_conditions.DAQPERIODID'+ condition
    else:
      command = 'select distinct SIMID, SIMDESCRIPTION,BEAMCOND,BEAMENERGY,GENERATOR,MAGNETICFIELD,DETECTORCOND,LUMINOSITY \
               from simulationConditions,bookkeepingView where bookkeepingview.DAQPeriodId=simulationConditions.simid' + condition
    
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getFilesWithGivenDataSets(self, simdesc, datataking, procPass, ftype, evt, configName='ALL', configVersion='ALL', production='ALL', flag = 'ALL', startDate = None, endDate = None, nbofEvents=False, startRunID=None, endRunID=None, runnumbers = []):
    
    configid = None
    condition = ''
    
    if configName != 'ALL' and configVersion != 'ALL':
      command = ' select configurationid from configurations where configurations.ConfigName=\''+configName+'\' and \
                    configurations.ConfigVersion=\''+configVersion+'\''
      res = self.dbR_._query(command)
      if not res['OK']:
        return S_ERROR(res['Message'])
      elif len(res['Value']) == 0:
        return S_ERROR('Config name and version dosnt exist!')
      else:
        configid = res['Value'][0][0]
        if configid != 0:
          condition = ' and jobs.configurationid='+str(configid)
        else:
          return S_ERROR('Wrong configuration name and version!')
                    
    if production != 'ALL':
      if type(production) == types.ListType:
        condition += ' and '
        cond = ' ( '
        for i in production:
          cond += 'jobs.production='+str(i)+ ' or '
        cond = cond[:-3] + ')'
        condition += cond
      else:
       condition += ' and jobs.production='+str(production)
    
    if len(runnumbers) > 0:
      if type(runnumbers) == types.ListType:
        condition += ' and '
        cond = ' ( '
        for i in runnumbers:
          cond += 'jobs.runnumber='+str(i)+ ' or '
        cond = cond[:-3] + ')'
        condition += cond
            
    tables = ' files,jobs '
    pcondition = ''
    jcondition = ''
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Processing pass is missing'+str(procPass))
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
    
      pcondition +=' and productions.totalprocpass=\''+totalproc+'\''
      jcondition = ' and jobs.production=productions.production '
      tables += ',productions'
    
    if ftype != 'ALL':
      if type(ftype) == types.ListType:
        condition += ' and '
        cond = ' ( '
        for i in ftype:
          fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(i)+'\''
          res = self.dbR_._query(fileType)
          if not res['OK']:
            gLogger.error('File Type not found:',res['Message'])
          elif len(res['Value'])==0:
            return S_ERROR('File type not found!'+str(i))
          else:
            ftypeId = res['Value'][0][0]
            cond  += ' files.FileTypeId='+str(ftypeId) + ' or '
        cond = cond[:-3] + ')'
        condition += cond  
      elif type(ftype) == types.StringType:
        fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
        res = self.dbR_._query(fileType)
        if not res['OK']:
          gLogger.error('File Type not found:',res['Message'])
        elif len(res['Value'])==0:
          return S_ERROR('File type not found!'+str(ftype))
        else:
          ftypeId = res['Value'][0][0]
          condition += ' and files.FileTypeId='+str(ftypeId)

    if evt != 0:
      if type(evt) in (types.ListType,types.TupleType):
        condition += ' and '
        cond = ' ( '
        for i in evt:
          cond +=  ' files.eventtypeid='+str(i) + ' or '
        cond = cond[:-3] + ')'
        condition += cond
      elif type(evt) in (types.StringTypes + (types.IntType,types.LongType)):
        condition +=  ' and files.eventtypeid='+str(evt)
              
    if startDate != None:
      condition += ' and files.inserttimestamp >= TO_TIMESTAMP (\''+str(startDate)+'\',\'YYYY-MM-DD HH24:MI:SS\')'
    
    if endDate != None:
      condition += ' and files.inserttimestamp <= TO_TIMESTAMP (\''+str(endDate)+'\',\'YYYY-MM-DD HH24:MI:SS\')'
    elif startDate != None and endDate == None:
      d = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S') 
      condition += ' and files.inserttimestamp <= TO_TIMESTAMP (\''+str(d)+'\',\'YYYY-MM-DD HH24:MI:SS\')'
      
    if flag != 'ALL':
      if type(flag) in (types.ListType,types.TupleType):
        conds = ' ('
        for i in flag:
          quality = None
          command = 'select QualityId from dataquality where dataqualityflag=\''+str(i)+'\''
          res = self.dbR_._query(command)
          if not res['OK']:
            gLogger.error('Data quality problem:',res['Message'])
          elif len(res['Value']) == 0:
              return S_ERROR('Dataquality is missing!')
          else:
            quality = res['Value'][0][0]
          conds += ' files.qualityid='+str(quality)+' or'
        condition += 'and'+conds[:-3] + ')'
      else:
        quality = None
        command = 'select QualityId from dataquality where dataqualityflag=\''+str(flag)+'\''
        res = self.dbR_._query(command)
        if not res['OK']:
          gLogger.error('Data quality problem:',res['Message'])
        elif len(res['Value']) == 0:
            return S_ERROR('Dataquality is missing!')
        else:
          quality = res['Value'][0][0]
        
        condition += ' and files.qualityid='+str(quality)
      
    if startRunID != None:
      condition += ' and jobs.runnumber>='+str(startRunID)
    if endRunID != None:
      condition += ' and jobs.runnumber<='+str(endRunID)
 
    simcondition = ''
    daqcondition = ''
    if simdesc == 'ALL' and datataking =='ALL':
      command = ' select filename from '+tables+' where files.jobid= jobs.jobid and files.gotreplica=\'Yes\'' +condition + jcondition + pcondition
      res = self.dbR_._query(command)
      return res
    elif simdesc != 'ALL':
      simcondition = ' select production from  productions, simulationconditions where  \
                   simulationconditions.simdescription=\''+simdesc+'\' and \
                   productions.simcondid= simulationconditions.simid '+ pcondition
    elif datataking != 'ALL':
      daqcondition = ' select production from  productions, data_Taking_conditions where  \
                   data_Taking_conditions.description=\''+datataking+'\' and \
                   productions.simcondid= data_Taking_conditions.Daqperiodid '+ pcondition
    
    if nbofEvents:
      command = ' select sum(files.eventstat) from files,jobs where files.jobid= jobs.jobid and files.gotreplica=\'Yes\''+condition+' \
                   and jobs.production in (' + simcondition + daqcondition+')'
    else:
      command = ' select filename from files,jobs where files.jobid= jobs.jobid and files.gotreplica=\'Yes\''+condition+' \
                   and jobs.production in (' + simcondition + daqcondition+')'
                   
    res = self.dbR_._query(command)
    
    return res
  
  #############################################################################
  def getProPassWithEventType(self, configName, configVersion, eventType, simcond):
    condition = ' and bookkeepingview.configname=\''+configName+'\' and \
                    bookkeepingview.configversion=\''+configVersion+'\''
    
    if eventType != 'ALL':
      condition += ' and bookkeepingview.EventTypeId='+str(eventType)
    
    if  simcond != 'ALL':
      condition += ' and bookkeepingview.DAQPeriodId='+str(simcond)
  
    command = 'select distinct productions.TOTALPROCPASS, pass_index.step0, pass_index.step1, pass_index.step2, pass_index.step3, pass_index.step4,pass_index.step5, pass_index.step6 \
       from bookkeepingview,productions,pass_index where \
            bookkeepingview.production=productions.production and \
            productions.passid=pass_index.passid'+ condition
    res = self.dbR_._query(command)
    value = res['Value']
    retvalue = []
    description = ''
    for one in value:
      tmp = list(one)
      groups = tmp[0].split('<')
      description = ''
      for group in groups:
        result = self.getDescription(group)['Value'][0][0]
        description += result +' + '
      tmp[0]=description[:-3] 
      retvalue += [tuple(tmp)]
    return S_OK(retvalue)

  #############################################################################
  def getProPassWithEventTypeNew(self, configName, configVersion, eventType, simcond):
    
    condition = ' and bview.configname=\''+configName+'\' and \
                bview.configversion=\''+configVersion+'\''
    
    if simcond !='ALL':
      condition += ' and bview.DAQPeriodId='+str(simcond)
    
    if eventType != 'ALL':
      condition += ' and bview.EventTypeId='+str(eventType)
    
    command = ' select distinct prod.passid, prod.TOTALPROCPASS,   \
     a0.applicationname s0, a0.applicationversion v0, a0.optionfiles op0, a0.dddb d0, a0.conddb c0, a0.EXTRAPACKAGES e0, \
     a1.applicationname s1, a1.applicationversion v1, a1.optionfiles op1, a1.dddb d1, a1.conddb c1, a1.EXTRAPACKAGES e1, \
     a2.applicationname s2, a2.applicationversion v2, a2.optionfiles op2, a2.dddb d2, a2.conddb c2, a2.EXTRAPACKAGES e2, \
     a3.applicationname s3, a3.applicationversion v3, a3.optionfiles op3, a3.dddb d3, a3.conddb c3, a3.EXTRAPACKAGES e3, \
     a4.applicationname s4, a4.applicationversion v4, a4.optionfiles op4, a4.dddb d4, a4.conddb c4, a4.EXTRAPACKAGES e4, \
     a5.applicationname s5, a5.applicationversion v5, a5.optionfiles op5, a5.dddb d5, a5.conddb c5, a5.EXTRAPACKAGES e5, \
     a6.applicationname s6, a6.applicationversion v6, a6.optionfiles op6, a6.dddb d6, a6.conddb c6, a6.EXTRAPACKAGES e6  \
   from bookkeepingview bview, productions prod,pass_index pi , \
        applications a0, applications a1, applications a2, applications a3, applications a4, applications a5, applications a6 \
   where  \
      bview.production=prod.production and \
      prod.passid=pi.passid and \
      pi.step0=a0.applicationid(+) and \
      pi.step1=a1.applicationid(+) and \
      pi.step2=a2.applicationid(+) and \
      pi.step3=a3.applicationid(+) and \
      pi.step4=a4.applicationid(+) and \
      pi.step5=a5.applicationid(+) and \
      pi.step6=a6.applicationid(+) ' + condition
      
    res = self.dbR_._query(command)
    value = res['Value']
    retvalue = []
    description = ''
    for one in value:
      tmp = list(one)
      groups = tmp[1].split('<')
      description = ''
      for group in groups:
        result = self.getDescription(group)['Value'][0][0]
        description += result +' + '
      tmp[1]=description[:-3] 
      retvalue += [tuple(tmp)]
    return S_OK(retvalue)
  
  #############################################################################
  def getJobInfo(self, lfn):
    command = 'select  jobs.DIRACJOBID, jobs.DIRACVERSION, jobs.EVENTINPUTSTAT, jobs.EXECTIME, jobs.FIRSTEVENTNUMBER, \'not used\', \
                 \'not used\', \'not used\', \'not used\', jobs.LOCATION,  jobs.NAME, jobs.NUMBEROFEVENTS, \
                 jobs.STATISTICSREQUESTED, jobs.WNCPUPOWER, jobs.CPUTIME, jobs.WNCACHE, jobs.WNMEMORY, jobs.WNMODEL, jobs.WORKERNODE, jobs.WNCPUHS06, jobs.jobid from jobs,files where files.jobid=jobs.jobid and files.filename=\''+str(lfn)+'\''
    res = self.dbR_._query(command)
    return res
    
    
  #############################################################################
  def getProductionFilesWithAGivenDate(self, prod, ftype, startDate = None, endDate = None):
    command = ''
    value = {}
    condition = ''
    if startDate != None:
      condition += ' and files.inserttimestamp >= TO_TIMESTAMP (\''+str(startDate)+'\',\'YYYY-MM-DD HH24:MI:SS\')'
    
    if endDate != None:
      condition += ' and files.inserttimestamp <= TO_TIMESTAMP (\''+str(endDate)+'\',\'YYYY-MM-DD HH24:MI:SS\')'
    elif startDate != None and endDate == None:
      d = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S') 
      condition += ' and files.inserttimestamp <= TO_TIMESTAMP (\''+str(d)+'\',\'YYYY-MM-DD HH24:MI:SS\')'
      
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error(res['Message'])
        return S_ERROR('Oracle error'+res['Message'])
      else:
        if len(res['Value']) == 0:
          return S_ERROR('File Type not found:'+str(ftype)) 
        
        ftypeId = res['Value'][0][0]
        command = 'select files.filename, files.gotreplica, files.filesize,files.guid, \''+ftype+'\' from jobs,files where jobs.jobid=files.jobid and files.filetypeid='+str(ftypeId)+condition+' and jobs.production='+str(prod)
    else:
      command = 'select files.filename, files.gotreplica, files.filesize,files.guid, filetypes.name from jobs,files,filetypes where jobs.jobid=files.jobid and files.filetypeid=filetypes.filetypeid'+condition+' and jobs.production='+str(prod)
   
    res = self.dbR_._query(command)
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        value[record[0]] = {'GotReplica':record[1],'FilesSize':record[2],'GUID':record[3], 'FileType':record[4]} 
    else:
      return S_ERROR(res['Message'])
    return S_OK(value)
  
  #############################################################################
  def getRunNumber(self, lfn):
    command = 'select jobs.runnumber from jobs,files where files.jobid=jobs.jobid and files.filename=\''+str(lfn)+'\''
    res = self.dbR_._query(command)
    return res
         
  #############################################################################
  def getProductionFiles(self, prod, ftype, gotreplica='ALL'):
    command = ''
    value = {}
    condition = ''
    if gotreplica != 'ALL':
      condition += 'and files.gotreplica=\''+str(gotreplica)+'\''
      
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error(res['Message'])
        return S_ERROR('Oracle error'+res['Message'])
      else:
        if len(res['Value']) == 0:
          return S_ERROR('File Type not found:'+str(ftype)) 
        
        ftypeId = res['Value'][0][0]
        command = 'select files.filename, files.gotreplica, files.filesize,files.guid, \''+ftype+'\' from jobs,files where jobs.jobid=files.jobid and files.filetypeid='+str(ftypeId)+' and jobs.production='+str(prod)+condition
    else:
      command = 'select files.filename, files.gotreplica, files.filesize,files.guid, filetypes.name from jobs,files,filetypes where jobs.jobid=files.jobid and files.filetypeid=filetypes.filetypeid and jobs.production='+str(prod)+condition
   
    res = self.dbR_._query(command)
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        value[record[0]] = {'GotReplica':record[1],'FileSize':record[2],'GUID':record[3], 'FileType':record[4]} 
    else:
      return S_ERROR(res['Message'])
    return S_OK(value)
  
  #############################################################################
  def getAvailableRunNumbers(self):
    command = 'select distinct runnumber from bookkeepingview'
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getProPassWithRunNumber(self, runnumber):
    
    condition = ' and bview.runnumber='+str(runnumber)
    
    command = ' select distinct prod.passid, prod.TOTALPROCPASS,   \
     a0.applicationname s0, a0.applicationversion v0, a0.optionfiles op0, a0.dddb d0, a0.conddb c0, a0.EXTRAPACKAGES e0, \
     a1.applicationname s1, a1.applicationversion v1, a1.optionfiles op1, a1.dddb d1, a1.conddb c1, a1.EXTRAPACKAGES e1, \
     a2.applicationname s2, a2.applicationversion v2, a2.optionfiles op2, a2.dddb d2, a2.conddb c2, a2.EXTRAPACKAGES e2, \
     a3.applicationname s3, a3.applicationversion v3, a3.optionfiles op3, a3.dddb d3, a3.conddb c3, a3.EXTRAPACKAGES e3, \
     a4.applicationname s4, a4.applicationversion v4, a4.optionfiles op4, a4.dddb d4, a4.conddb c4, a4.EXTRAPACKAGES e4, \
     a5.applicationname s5, a5.applicationversion v5, a5.optionfiles op5, a5.dddb d5, a5.conddb c5, a5.EXTRAPACKAGES e5, \
     a6.applicationname s6, a6.applicationversion v6, a6.optionfiles op6, a6.dddb d6, a6.conddb c6, a6.EXTRAPACKAGES e6  \
   from bookkeepingview bview, productions prod,pass_index pi , \
        applications a0, applications a1, applications a2, applications a3, applications a4, applications a5, applications a6 \
   where  \
      bview.production=prod.production and \
      prod.passid=pi.passid and \
      pi.step0=a0.applicationid(+) and \
      pi.step1=a1.applicationid(+) and \
      pi.step2=a2.applicationid(+) and \
      pi.step3=a3.applicationid(+) and \
      pi.step4=a4.applicationid(+) and \
      pi.step5=a5.applicationid(+) and \
      pi.step6=a6.applicationid(+) ' + condition
       
    res = self.dbR_._query(command)
    if not res['OK']:
      return S_ERROR(res['Message'])
    value = res['Value']
    retvalue = []
    description = ''
    for one in value:
      tmp = list(one)
      groups = tmp[1].split('<')
      description = ''
      for group in groups:
        result = self.getDescription(group)['Value'][0][0]
        description += result +' + '
      tmp[1]=description[:-3] 
      retvalue += [tuple(tmp)]
    return S_OK(retvalue)
  
  
  #############################################################################
  def getEventTypeWithAgivenRuns(self,runnumber, processing):
    condition = ' and bookkeepingview.runnumber='+str(runnumber)
    if processing != 'ALL':
      descriptions = processing.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
    
    command ='select distinct eventTypes.EventTypeId, eventTypes.Description from eventtypes,bookkeepingview,productions where \
         bookkeepingview.production=productions.production and \
         eventTypes.EventTypeId=bookkeepingview.eventtypeid'+condition
  
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getFileTypesWithAgivenRun(self, runnumber, procPass, evtId):

    condition = ' and bookkeepingview.runnumber='+str(runnumber)    

    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
          
    if evtId != 'ALL':
      condition += ' and bookkeepingview.EventTypeId='+str(evtId)
    
      
    command = 'select distinct filetypes.name from filetypes,bookkeepingview,productions where \
               bookkeepingview.filetypeId=fileTypes.filetypeid and bookkeepingview.production=productions.production'+condition
               
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getLimitedNbOfRunFiles(self,  procPass, evtId, runnumber, ftype):
    
    condition = ' and jobs.runnumber='+str(runnumber)    
    tables = ' jobs, files, productions'
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
      condition += ' and productions.PRODUCTION=jobs.production'
    
    if evtId != 'ALL':
      condition += ' and files.EventTypeId='+str(evtId)
    
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
      else:
        ftypeId = res['Value'][0][0]
        condition += ' and files.FileTypeId='+str(ftypeId)  
    
    if ftype == 'ALL':
      command =' select count(*), SUM(files.EventStat), SUM(files.FILESIZE) from '+ tables +', filetypes \
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\' and \
         files.filetypeid=filetypes.filetypeid' + condition 
    else:
      command =' select count(*), SUM(files.EventStat), SUM(files.FILESIZE) from ' + tables +' \
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\'' + condition 
    
    res = self.dbR_._query(command)
    return res

  #############################################################################
  def getLimitedFilesWithAgivenRun(self, procPass, evtId, runnumber, ftype, startitem, maxitems):
    
    condition = ' and jobs.runnumber='+str(runnumber)    
     
    tables = ' jobs, files, productions'
            
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
      condition += ' and productions.PRODUCTION=jobs.production'
      
    
    if evtId != 'ALL':
      condition += ' and files.EventTypeId='+str(evtId)
    
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
      else:
        ftypeId = res['Value'][0][0]
        condition += ' and files.FileTypeId='+str(ftypeId)
    
         
    if ftype == 'ALL':
      command = 'select rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode, ftype, fevt, runnb,fillnb,fullst,quality, jevent \
      FROM \
       ( select rownum rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode,ftype,fevt, runnb,fillnb,fullst,quality, jevent \
          from( \
           select fileName fname, files.EventStat eventstat, files.FileSize fsize, files.CreationDate creation, \
            \'not used\' gen, \'not used\' geom, \
            jobs.JobStart jstart, jobs.JobEnd jend, jobs.WorkerNode wnode, filetypes.name ftype, files.eventtypeid fevt,\
        jobs.runnumber runnb, jobs.fillnumber fillnb, files.fullstat fullst, dataquality.dataqualityflag quality, jobs.eventinputstat jevent\
        from'+tables+',filetypes, dataquality \
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\' and \
         files.qualityid=dataquality.qualityid  and \
         files.filetypeid=filetypes.filetypeid' + condition + ' ) where rownum <= '+str(maxitems)+ ' ) where rnum > '+ str(startitem)
    else:
      command = 'select rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode, \''+str(ftype)+'\' , fevt, runnb,fillnb,fullst,quality,jevent from \
       ( select rownum rnum, fname,eventstat, fsize,creation,gen,geom,jstart,jend,wnode, \''+str(ftype)+'\' ,fevt, runnb,fillnb,fullst,quality,jevent \
          from( \
           select fileName fname, files.EventStat eventstat, files.FileSize fsize, files.CreationDate creation, \
            \'not used\' gen, \'not used\' geom, \
            jobs.JobStart jstart, jobs.JobEnd jend, jobs.WorkerNode wnode, \''+str(ftype)+'\', files.eventtypeid fevt, jobs.runnumber runnb, jobs.fillnumber fillnb, files.fullstat fullst, dataquality.dataqualityflag quality, jobs.eventinputstat jevent \
        from'+ tables+', dataquality \
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\' and \
         files.qualityid=dataquality.qualityid ' + condition + ' ) where rownum <= ' + str(maxitems)+ ' ) where rnum > '+str(startitem)
      
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getRunFilesWithAgivenRunWithDataQuality(self, procPass, evtId, runnumber, ftype, quality=[]):
    condition = ' and jobs.runnumber='+str(runnumber)    
    
    tables = ' jobs, files,productions'
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
      condition += ' and productions.PRODUCTION=jobs.production'
      
    
    if evtId != 'ALL':
      condition += ' and files.EventTypeId='+str(evtId)
    
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
      else:
        ftypeId = res['Value'][0][0]
        condition += ' and files.FileTypeId='+str(ftypeId)
    
    if len(quality) > 0:
      conds = ' ('
      for i in quality:
        if quality[i] != False:
          qualityFlag = None
          command = 'select QualityId from dataquality where dataqualityflag=\''+str(i)+'\''
          res = self.dbR_._query(command)
          if not res['OK']:
            gLogger.error('Data quality problem:',res['Message'])
          elif len(res['Value']) == 0:
              return S_ERROR('Dataquality is missing!')
          else:
            qualityFlag = res['Value'][0][0]
          conds += ' files.qualityid='+str(qualityFlag)+' or'
      condition += 'and'+conds[:-3] + ')'
             
    if ftype == 'ALL':
      command =' select files.FileName, files.EventStat, files.FileSize, files.CreationDate, \'Unkown\',\'Unkown\',\
         jobs.JobStart, jobs.JobEnd, jobs.WorkerNode, filetypes.Name, jobs.runnumber, jobs.fillnumber, files.fullstat, dataquality.dataqualityflag, jobs.eventinputstat from '+ tables+' ,filetypes, dataquality \
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\' and \
         files.filetypeid=filetypes.filetypeid and \
         files.qualityid= dataquality.qualityid' + condition 
      all +=1
    else:
      command =' select files.FileName, files.EventStat, files.FileSize, files.CreationDate,\'Unkown\',\'Unkown\',\
         jobs.JobStart, jobs.JobEnd, jobs.WorkerNode, \''+str(ftype)+'\' , jobs.runnumber, jobs.fillnumber, files.fullstat, dataquality.dataqualityflag, jobs.eventinputstat from '+ tables +' ,dataquality\
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\' and \
         files.qualityid= dataquality.qualityid' + condition 
    
    
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getRunFilesWithAgivenRun(self, procPass, evtId, runnumber, ftype):
    condition = ' and jobs.runnumber='+str(runnumber)    
    
    tables = ' jobs, files,productions'
    
    if procPass != 'ALL':
      descriptions = procPass.split('+')
      totalproc = ''
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
      condition += ' and productions.TOTALPROCPASS=\''+totalproc+'\''
      condition += ' and productions.PRODUCTION=jobs.production'
      
    
    if evtId != 'ALL':
      condition += ' and files.EventTypeId='+str(evtId)
    
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error('File Type not found:',res['Message'])
      else:
        ftypeId = res['Value'][0][0]
        condition += ' and files.FileTypeId='+str(ftypeId)
    
         
    if ftype == 'ALL':
      command =' select files.FileName, files.EventStat, files.FileSize, files.CreationDate, \'Unkown\',\'Unkown\',\
         jobs.JobStart, jobs.JobEnd, jobs.WorkerNode, filetypes.Name, jobs.runnumber, jobs.fillnumber, files.fullstat, dataquality.dataqualityflag, jobs.eventinputstat from '+ tables+' ,filetypes, dataquality \
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\' and \
         files.filetypeid=filetypes.filetypeid and \
         files.qualityid= dataquality.qualityid' + condition 
      all +=1
    else:
      command =' select files.FileName, files.EventStat, files.FileSize, files.CreationDate,\'Unkown\',\'Unkown\',\
         jobs.JobStart, jobs.JobEnd, jobs.WorkerNode, \''+str(ftype)+'\' , jobs.runnumber, jobs.fillnumber, files.fullstat, dataquality.dataqualityflag, jobs.eventinputstat from '+ tables +' ,dataquality\
         where files.JobId=jobs.JobId and \
         files.gotReplica=\'Yes\' and \
         files.qualityid= dataquality.qualityid' + condition 
    
    
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getRunFiles(self, runid):
    value = {}
    command = 'select files.filename, files.gotreplica, files.filesize,files.guid from jobs,files where jobs.jobid=files.jobid and files.filetypeid=23 and jobs.runnumber='+str(runid)
    res = self.dbR_._query(command)
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        value[record[0]] = {'GotReplica':record[1],'FileSize':record[2],'GUID':record[3]} 
    else:
      return S_ERROR(res['Message'])
    return S_OK(value)
  
  #############################################################################
  def insert_aplications(self, appName, appVersion, option, dddb, condb, extrapack):
    
    retVal = self.check_applications(appName, appVersion, option, dddb, condb, extrapack)
    if retVal['OK']:
      id = retVal['Value']
      if id == 0:
        command = ' SELECT applications_index_seq.nextval FROM dual'
        retVal = self.dbR_._query(command)
        if retVal['OK']:
          appid = retVal['Value'][0][0]
          values = str(appid) + ','
          values += '\''+str(appName)+'\','
          values += '\''+str(appVersion)+'\','
          values += '\''+str(option)+'\','
          values += '\''+str(dddb)+'\','
          values += '\''+str(condb)+'\','
          values += '\''+str(extrapack)+'\''
          command = ' insert into applications (ApplicationID, ApplicationName, ApplicationVersion, OptionFiles, DDDb, condDb, Extrapackages) values ('+values+')'
          retVal = self.dbW_._query(command)
          if not retVal['OK']:
            return S_ERROR(retVal['Message'])
        else:
          return S_ERROR(retVal['Message'])
        return S_OK(appid)
      else:
        return S_OK(id)
    else:
      return S_ERROR(retVal['Message'])
  
  #############################################################################
  def check_applications(self,appName, appVersion, option, dddb, condb, extrapack):
    command = ''
    condition = ''
    if dddb != '':
      condition += ' and dddb=\''+str(dddb)+'\' '
    
    if option != '':
      condition += ' and optionfiles=\''+str(option)+'\' '
    
    if condb != '':
      condition += ' and conddb=\''+str(condb)+'\' '
    
    if extrapack != '':
      condition += ' and ExtraPackages=\''+str(extrapack)+'\' '
    
    command = 'select applicationid from applications where applicationname=\''+str(appName)+'\' and applicationversion=\''+str(appVersion)+'\''+condition
    retVal = self.dbR_._query(command)
    if retVal['OK']:
      id = retVal['Value']
      if len(id) == 0:
        return S_OK(0)
      else:
        return S_OK(id[0][0])
    else:
      return S_ERROR(retVal['Message'])
  
  #############################################################################
  def insert_pass_index_migration(self, passid, descr, groupid, step0,step1, step2,step3,step4,step5,step6):
    values = str(passid)+','
    values += '\''+descr+'\','
    values += str(groupid) +','
    values += str(step0) +','
    values += str(step1) +','
    values += str(step2) +','
    values += str(step3) +','
    values += str(step4) +','
    values += str(step5) +','
    values += str(step6)
    command = 'insert into pass_index (PASSID, DESCRIPTION, GROUPID, STEP0, STEP1, STEP2, STEP3, STEP4, STEP5, STEP6) values('+values+')'
    res = self.dbW_._query(command)
    return res


  #############################################################################
  def updateFileMetaData(self, filename, filesAttr):
    utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    command = 'update files Set inserttimestamp=TO_TIMESTAMP(\''+str(utctime)+'\',\'YYYY-MM-DD HH24:MI:SS\') ,'
    for attribute in filesAttr.keys():
      if type(filesAttr[attribute]) == types.StringType:
        command += str(attribute)+'=\''+str(filesAttr[attribute])+'\' ,'
      else:
        command += str(attribute)+'='+str(filesAttr[attribute])+' ,'
    
    command = command[:-1]
    command += ' where fileName=\''+filename+'\''
    res = self.dbW_._query(command)
    return res
    

  #############################################################################
  def renameFile(self, oldLFN, newLFN):
    utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    command = ' update files Set inserttimestamp=TO_TIMESTAMP(\''+str(utctime)+'\',\'YYYY-MM-DD HH24:MI:SS\'), fileName = \''+newLFN+'\' where filename=\''+oldLFN+'\''
    res = self.dbW_._query(command)
    return res
  
  #############################################################################
  def getInputAndOutputJobFiles(self, jobids):
    list = {}
    for jobid in jobids:
      tmp = {}
      res = self.getInputFiles(jobid)

      if not res['OK']:
        return S_ERROR(res['Message'])
      input = res['Value']
      inputs = []
      for lfn in input:
        inputs += [lfn]
        
      res = self.getOutputFiles(jobid)
      if not res['OK']:
        return S_ERROR(res['Message'])
      output = res['Value'] 
      outputs = []
      for lfn in output:
        if lfn not in inputs:
          outputs += [lfn]
      tmp = {'InputFiles':inputs,'OutputFiles':outputs}
      list[jobid]=tmp
    return S_OK(list)  
      
  #############################################################################
  def getInputFiles(self, jobid):
    command = ' select files.filename from inputfiles,files where files.fileid=inputfiles.fileid and inputfiles.jobid='+str(jobid)
    res = self.dbR_._query(command)
    return res
      
  #############################################################################
  def getOutputFiles(self, jobid):  
    command = ' select files.filename from files where files.jobid ='+str(jobid) 
    res = self.dbR_._query(command)
    return res
  #############################################################################
  def getJobsIds(self, filelist):
    list = {}
    for file in filelist:
      res = self.getJobInfo(file)
      if not res['OK']:
        return S_ERROR(res['Message'])  
      dbResult = res['Value']
      for record in dbResult:
        jobid = str(record[19])
        value = {'DiracJobID':record[0], 'DiracVersion':record[1], 'EventInputStat':record[2], 'ExecTime':record[3], 'FirstEventNumber':record[4], \
                  'Location':record[5], 'Name':record[6], 'NumberofEvents':record[7], \
                  'StatistivsRequested':record[8], 'WNCPUPOWER':record[9], 'CPUTIME':record[10], 'WNCACHE':record[11], 'WNMEMORY':record[12], 'WNMODEL':record[13], 'WORKERNODE':record[14]}  
      list[jobid]=value
    return S_OK(list)
  
  
  
  #############################################################################
  def getSpecificFiles(self,configName, configVersion, programName, programVersion, fileType, eventTypeId, production):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getSpecificFiles', [configName, configVersion, programName, programVersion, fileType, eventTypeId, production])
  
  #############################################################################
  def getPass_index(self):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getPass_index', [])
  
  #############################################################################  
  def insert_pass_index(self, groupdesc, step0, step1, step2, step3, step4, step5, step6):
    return self.dbW_.executeStoredFunctions('BKK_ORACLE.insert_pass_index', LongType, [groupdesc, step0, step1, step2, step3, step4, step5, step6])
  
  #############################################################################
  def insertTag(self, name, tag):
    return self.dbW_.executeStoredProcedure('BKK_ORACLE.insertTag', [name, tag], False)
  
  #############################################################################
  def existsTag(self, name, value):
    command = 'select count(*) from tags where name=\''+str(name)+'\' and tag=\''+str(value)+'\''
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    elif retVal['Value'][0][0] > 0:
      return S_OK(True)
    return S_OK(False)
  
  #############################################################################  
  def checkAddProduction(self, steps, groupdesc, simcond, inputProdTotalProcessingPass, production):
    keys = steps.keys()
    keys.sort()
    s = [None, None, None, None, None, None, None]
    i = 0
    result = ''
    for step in keys:
      appName = steps[step]['ApplicationName']
      appVersion = steps[step]['ApplicationVersion'] 
      optfiles = steps[step]['OptionFiles']
      if optfiles == None:
        optfiles = ''
        result += 'Option files are missing! \n'
      dddb = steps[step]['DDDb']
      if dddb == None: 
        dddb = ''
        result += 'DDDb tag is missing! \n'
      condb = steps[step]['CondDb']
      if condb == None:
        condb = ''
        result += 'ConDDb tag is missing! \n'
      
      extrapack = steps[step]['ExtraPackages']
      if extrapack != None:
        extrapack = ''
        result += 'ExtraPackages is missing! \n'
      
      
      res = self.check_applications(appName, appVersion, optfiles, dddb, condb, extrapack)
      if not res['OK']:
        return S_ERROR(res['Message'])
      else:
        value = res['Value']
        if value == 0:
          result += 'Application Name:'+str(appName)+' Application Version:'+str(appVersion)+' Optionfiles:'+str(optfiles)+ 'DDDb:'+str(dddb) +'CondDb:'+str(condb)+'ExtraPackages:'+str(extrapack)+' are missing in the BKKDB! \n'
        else:
          result += 'Application Name:'+str(appName)+' Application Version:'+str(appVersion)+' Optionfiles:'+str(optfiles)+ 'DDDb:'+str(dddb) +'CondDb:'+str(condb)+'ExtraPackages:'+str(extrapack)+' are in the BKKDB! \n'
          s[i] = res['Value']
          i += 1
    

    command = ' select groupId from PASS_GROUP where GROUPDESCRIPTION=\''+str(groupdesc)+'\''
    retVal = self.dbR_._query(command)
    groupid = None
    
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      if len(retVal['Value']) == 0:
        result += 'Group description is not exist in the BKKDB! \n'
      else: 
        groupid = retVal['Value'][0][0]
      
    
      retVal = self.check_pass_index(groupid, s[0], s[1], s[2], s[3], s[4], s[5],s[6])
      if not retVal['OK']:
        return S_ERROR(retVal['Message'])
      elif retVal['Value']==None:
        result += 'Pass index not exists! \n'
        result += '     pass index:'+str(steps)+'\n'
      else:
        result += 'Pass index exists!\n'
        result += '     pass index:'+str(steps)+'\n'
    
    res = self.getSimulationCondIdByDesc(simcond['SimDescription'])
    if not res['OK']:
      result += res['Message'] +'\n'
    elif res['Value'] == 0: 
      result += ' Simulation conditions not exists!\n'
    else:
      result += ' Simulation conditions exists!\n'
    
    return S_OK(result)

    
  
  #############################################################################  
  def insert_procressing_pass(self, steps, groupdesc, simcond, inputProdTotalProcessingPass, production):
    keys = steps.keys()
    keys.sort()
    s = [None, None, None, None, None, None, None]
    i = 0
    for step in keys:
      appName = steps[step]['ApplicationName']
      appVersion = steps[step]['ApplicationVersion'] 
      optfiles = steps[step]['OptionFiles']
      if optfiles == None:
        optfiles = ''
      dddb = steps[step]['DDDb']
      if dddb == None: 
        dddb = ''
      condb = steps[step]['CondDb']
      if condb == None:
        condb = ''
      extrapack = steps[step]['ExtraPackages']
      if extrapack == None:
        extrapack = ''
      res = self.insert_aplications(appName, appVersion, optfiles, dddb, condb, extrapack)
      if not res['OK']:
        return S_ERROR(res['Message'])
      else:
        s[i] = res['Value']
        i += 1
    res = self.insert_pass_index_new(groupdesc, s[0], s[1], s[2], s[3], s[4], s[5],s[6])
    passid = None
    if not res['OK']:
      print res['Message']
    else:
      passid = res['Value']
    
    simdesc = ''
    retVal = self.__getSimDesc(simcond)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      simdesc = retVal['Value']
    
    gLogger.info('Insert productions:')
    gLogger.info('Passid: '+str(passid))
    gLogger.info('Simulation description: '+str(simdesc))
    gLogger.info('Production: '+str(production))
    gLogger.info('Input Production total processing pass: '+str(inputProdTotalProcessingPass))
    
    retVal = self.insertProcessing(production, passid, inputProdTotalProcessingPass, simdesc)
    if retVal['OK']:
      return S_OK('The processing pass added successfully!')
    else:
      return S_ERROR('Error discovered!'+ str(retVal['Message']))
  
  #############################################################################  
  def insert_procressing_passRealData(self, steps, groupdesc, daqdesc, inputProdTotalProcessingPass, production):
    keys = steps.keys()
    keys.sort()
    s = [None, None, None, None, None, None, None]
    i = 0
    for step in keys:
      appName = steps[step]['ApplicationName']
      appVersion = steps[step]['ApplicationVersion'] 
      optfiles = steps[step]['OptionFiles']
      if optfiles == None:
        optfiles = ''
      dddb = steps[step]['DDDb']
      if dddb == None: 
        dddb = ''
      condb = steps[step]['CondDb']
      if condb == None:
        condb = ''
      extrapack = steps[step]['ExtraPackages']
      if extrapack == None:
        extrapack = ''
        
      res = self.insert_aplications(appName, appVersion, optfiles, dddb, condb,extrapack)
      if not res['OK']:
        return S_ERROR(res['Message'])
      else:
        s[i] = res['Value']
        i += 1
    res = self.insert_pass_index_new(groupdesc, s[0], s[1], s[2], s[3], s[4], s[5],s[6])
    passid = None
    if not res['OK']:
      print res['Message']
    else:
      passid = res['Value']
    
    gLogger.info('Insert productions:')
    gLogger.info('Passid: '+str(passid))
    gLogger.info('Data taking description: '+str(daqdesc))
    gLogger.info('Production: '+str(production))
    gLogger.info('Input Production total processing pass: '+str(inputProdTotalProcessingPass))
    
    retVal = self.insertProcessingRealData(production, passid, inputProdTotalProcessingPass, daqdesc)
    if retVal['OK']:
      return S_OK('The processing pass added successfully!')
    else:
      return S_ERROR('Error discovered!'+ str(retVal['Message']))
  
  #############################################################################  
  def __getSimDesc(self, simcond):
    res = self.getSimulationCondIdByDesc(simcond['SimDescription'])
    if not res['OK']:
      gLogger.error("Simulation conditions problem", res["Message"])
      return S_ERROR("Simulation conditions problem" + str(res["Message"]))
    elif res['Value'] == 0: 
      retVal = self.insertSimConditions(simcond['SimDescription'], simcond['BeamCond'], simcond['BeamEnergy'], simcond['Generator'], simcond['MagneticField'], simcond['DetectorCond'], simcond['Luminosity'])
      if not retVal['OK']:
         return S_ERROR(retVal['Message'])
    simdesc = simcond['SimDescription'] 
    return S_OK(simdesc)
       
    
  
  #############################################################################  
  def __equalsPrograms(self, existing, new):
    ok = True
    if self.__countSteps(existing) != len(new):
      ok = False
    else:
      i = 3
      keys = new.keys()
      keys.sort()
      for key in keys:
        newProgramName = new[key].split('-')[0]
        newProgramVersion = new[key].split('-')[1]
        tags = self.__getTags(existing[i])
        name = self.__getProgramName(existing[i])
        i += 1
        ok = (newProgramName == name and newProgramVersion == tags[0])
        if not ok:
          break;
    return ok
  
  #############################################################################  
  def __countSteps(self, value):
    stepNb = 0
    for i in range(3, len(value)):
      if value[i] != None:
        stepNb += 1
      i += 1
    return stepNb
  #############################################################################  
  
  def __getTags(self, value):
    tmp = value.split('&')
    tags = []
    for i in range(len(tmp)):
      tags  += tmp[i].split('-')[1].split('/')
    return tags
  
  #############################################################################  
  def __getProgramName(self, value):
    return value.split('-')[0]
  
  #############################################################################  
  def insertProcessing(self, production, passid, inputprod, simdesc):
    totalproc = ''
    simid = None
    if inputprod <> '':
      descriptions = inputprod.split('+')
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions or Simulation Condition missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
    else:
      inputprod = None
    res = self.getSimulationCondIdByDesc(simdesc)
    
    if not res['OK']:
      gLogger.error("Simulation conditions problem", res["Message"])
      return S_ERROR("Simulation conditions problem" + str(res["Message"]))
    elif res['Value'] != 0: 
      simid = res['Value']
    else:
      return S_ERROR('Simulation condition is not found in BKK!')
    return self.dbW_.executeStoredProcedure('BKK_ORACLE.insertProcessing', [production, passid, totalproc, simid], False)
  
  #############################################################################  
  def insertProcessingRealData(self, production, passid, inputprod, datataking):
    totalproc = ''
    daqid = None
    if inputprod <> '':
      descriptions = inputprod.split('+')
      for desc in descriptions:
        result = self.getGroupId(desc.strip())
        if not result['OK']:
          return S_ERROR(result['Message'])
        elif len(result['Value']) == 0:
          return S_ERROR('Data Taking Conditions is missing in the DB!')
        val = result['Value'][0][0]
        totalproc += str(val)+"<"
      totalproc = totalproc[:-1]
    else:
      inputprod = None
    daq = {'Description':datataking}
    res = self.getDataTakingCondId(daq)
    if not res['OK']:
      gLogger.error("Data takin conditions problem", res["Message"])
      return S_ERROR("Data takin conditions problem" + str(res["Message"]))
    elif len(res['Value']) != 0: 
      daqid = res['Value'][0][0]
    else:
      return S_ERROR('Data takin conditions is not found in BKK!')
    return self.dbW_.executeStoredProcedure('BKK_ORACLE.insertProcessing', [production, passid, totalproc, daqid], False)
  
  
  #############################################################################  
  def listProcessingPass(self, prod = None):
    condition =''
    if prod != None:
      condition = ' where production='+str(prod)
    
    
    command = 'select distinct productions.TOTALPROCPASS,  \
               production from productions'+condition+ ' ORDER BY production'
    
    
    res = self.dbR_._query(command)
    value = res['Value']
    retvalue = []
    description = ''
    for one in value:
      tmp = list(one)
      groups = tmp[0].split('<')
      description = ''
      for group in groups:
        result = self.getDescription(group)['Value'][0][0]
        description += result +' + '
      tmp[0]=description[:-3] 
      retvalue += [tuple(tmp)]
    return S_OK(retvalue)
  
  #############################################################################  
  def setQuality(self, lfns, flag):
    result = {}
    command = ' select qualityid from dataquality where dataqualityflag=\''+str(flag)+'\''
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    elif len(retVal['Value']) == 0:
      return S_ERROR('Data quality flag is missing in the DB')
    qid = retVal['Value'][0][0]
    
    failed = []
    succ = []
    for lfn in lfns:
      retVal = self.__updateQualityFlag(lfn, qid)
      if not retVal['OK']:
        failed += [lfn] 
        gLogger.error(retVal['Message'])
      else:
        succ += [lfn]
    result['Successful'] = succ
    result['Failed'] = failed
    return S_OK(result)
  
  #############################################################################  
  def getAvailableDataQuality(self):
    command = ' select dataqualityflag from dataquality'
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    flags = retVal['Value']
    result = []
    for i in flags:
      result += [i[0]]
    return S_OK(result)
  
  #############################################################################  
  def setQualityRun(self, runNb, flag):
    command = 'select distinct jobs.fillnumber, configurations.configname, configurations.configversion, data_taking_conditions.description, pass_group.groupdescription from \
          jobs, configurations,data_taking_conditions,productions, pass_group, pass_index \
         where jobs.configurationid=configurations.configurationid and data_taking_conditions.daqperiodid=productions.simcondid and \
         productions.passid=pass_index.passid and pass_index.groupid=pass_group.groupid and \
         jobs.production=productions.production and jobs.runnumber='+str(runNb)
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    value = retVal['Value']
    if len(value) == 0:
      return S_ERROR('This '+str(runNb)+' run is missing in the BKK DB!')
    command = ' select qualityid from dataquality where dataqualityflag=\''+str(flag)+'\''
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    elif len(retVal['Value']) == 0:
      return S_ERROR('Data quality flag is missing in the DB')
    qid = retVal['Value'][0][0]
    
    utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    command = ' update files set inserttimestamp=TO_TIMESTAMP(\''+str(utctime)+'\',\'YYYY-MM-DD HH24:MI:SS\'), qualityId='+str(qid)+' where fileid in ( select files.fileid from jobs, files where jobs.jobid=files.jobid and \
      jobs.runnumber='+str(runNb)+')'
    retVal = self.dbW_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    
    command = 'select files.filename from jobs, files where jobs.jobid=files.jobid and \
      jobs.runnumber='+str(runNb)
    
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    
    succ = []
    records = retVal['Value']
    for record in records:
      succ += [record[0]]
    result = {}
    result['Successful'] = succ
    result['Failed'] = []
    return S_OK(result)
  
  def setQualityProduction(self, prod, flag):
    command = 'select distinct jobs.production  from jobs where jobs.production='+str(prod)
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    value = retVal['Value']
    if len(value) == 0:
      return S_ERROR('This '+str(prod)+' production is missing in the BKK DB!')
    command = ' select qualityid from dataquality where dataqualityflag=\''+str(flag)+'\''
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    elif len(retVal['Value']) == 0:
      return S_ERROR('Data quality flag is missing in the DB')
    qid = retVal['Value'][0][0]
    
    utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    command = ' update files set inserttimestamp=TO_TIMESTAMP(\''+str(utctime)+'\',\'YYYY-MM-DD HH24:MI:SS\'), qualityId='+str(qid)+' where fileid in ( select files.fileid from jobs, files where jobs.jobid=files.jobid and \
      jobs.production='+str(prod)+')'
    retVal = self.dbW_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    
    command = 'select files.filename from jobs, files where jobs.jobid=files.jobid and \
      jobs.production='+str(prod)
    
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    
    succ = []
    records = retVal['Value']
    for record in records:
      succ += [record[0]]
    result = {}
    result['Successful'] = succ
    result['Failed'] = []
    return S_OK(result)
  
  #############################################################################  
  def __updateQualityFlag(self, lfn, qid):
    utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    command = 'update files set inserttimestamp=TO_TIMESTAMP(\''+str(utctime)+'\',\'YYYY-MM-DD HH24:MI:SS\'), qualityId='+str(qid)+' where filename=\''+str(lfn)+'\''
    retVal = self.dbW_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      return S_OK('Quality flag has been updated!')
    
  #############################################################################  
  def getProductionsWithPocessingPass(self, processingPass):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getProductions', [processingPass])
  
  #############################################################################  
  def getFilesByProduction(self, production, eventtype, filetype):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getFilesByProduction', [production, eventtype, filetype])
  
  #############################################################################
  def getProductions(self, configName, configVersion, eventTypeId):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getProductions', [configName, configVersion, eventTypeId])
  
  #############################################################################
  def getNumberOfEvents(self, configName, configversion, eventTypeId, production):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getNumberOfEvents', [configName, configversion, eventTypeId, production])
  
  #############################################################################
  def getEventTyesWithProduction(self, production):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getEventTyesWithProduction', [production])
  
  #############################################################################  
  def getFileTypesWithProduction(self, production, eventType):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getFileTypesWithProduction', [production, eventType])
  
  #############################################################################  
  def getSpecificFilesWithoutProd(self, configName, configVersion, pname, pversion, filetype, eventType):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getSpecificFilesWithoutProd',[configName,configVersion, pname, pversion, filetype, eventType])
  
  #############################################################################  
  def getFileTypes(self, configName, configVersion, eventType, prod):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getFileTypes',[configName, configVersion, eventType, prod]) 
  
  #############################################################################  
  def getProgramNameAndVersion(self, configName, configVersion, eventType, prod, fileType):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getProgramNameAndVersion', [configName, configVersion, eventType, prod, fileType])
  
  #############################################################################  
  def getAvailableEventTypes(self):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getAvailableEventTypes', [])
  
  #############################################################################  
  def getConfigNameAndVersion(self, eventTypeId):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getConfigNameAndVersion', [eventTypeId])
  
  #############################################################################  
  def getAvailableProcessingPass(self, configName, configVersion, eventTypeId):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getAvailableProcessingPass', [configName, configVersion, eventTypeId])

  #############################################################################
  def getFileTypesWithEventType(self, configName, configVersion, eventTypeId, production):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getFileTypesWithEventType', [configName, configVersion, eventTypeId, production])
  
  #############################################################################
  def getFileTypesWithEventTypeALL(self, configName, configVersion, eventTypeId):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getFileTypesWithEventType', [configName, configVersion, eventTypeId])
  
  #############################################################################
  def getFilesByEventType(self, configName, configVersion, fileType, eventTypeId, production):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getFilesByEventType', [configName, configVersion, fileType, eventTypeId, production])
  
  #############################################################################
  def getFilesByEventTypeALL(self, configName, configVersion, fileType, eventTypeId):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getFilesByEventType', [configName, configVersion, fileType, eventTypeId])
  
  #############################################################################
  def getProductionsWithEventTypes(self, eventType, configName,  configVersion, processingPass):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getProductionsWithEventTypes', [eventType, configName,  configVersion, processingPass])
  
  #############################################################################
  def getSimulationCondID(self, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    return self.dbR_.executeStoredFunctions('BKK_ORACLE.getSimulationCondID', LongType, [BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity])
  
  #############################################################################
  def getSimCondIDWhenFileName(self, fileName):
    return self.dbR_.executeStoredFunctions('BKK_ORACLE.getSimCondIDWhenFileName', LongType, [fileName])

  #############################################################################  
  def getLFNsByProduction(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getLFNsByProduction',[prodid])
  
  #############################################################################  
  def getAncestors(self, lfn, depth):
    logicalFileNames={}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    jobsId = []
    job_id = -1
    if depth < 1:
      return S_OK({'Failed:':lfn})
    odepth = depth 
    gLogger.debug('original',lfn)
    for fileName in lfn:
      depth = odepth
      jobsId = []
      gLogger.debug('filename',fileName)
      jobsId = []
      result = self.dbR_.executeStoredFunctions('BKK_MONITORING.getJobId',LongType,[fileName])
      if not result["OK"]:
        gLogger.error('Ancestor',result['Message'])
      else:
        job_id = int(result['Value'])
      if job_id != 0:
        jobsId = [job_id]
        files = []
        while (depth-1) and jobsId:
          for job_id in jobsId:
            command = 'select files.fileName,files.jobid, files.gotreplica from inputfiles,files where inputfiles.fileid=files.fileid and inputfiles.jobid='+str(job_id)
            jobsId=[]
            res = self.dbR_._query(command)
            if not res['OK']:
              gLogger.error('Ancestor',result["Message"])
            else:
              dbResult = res['Value']
              for record in dbResult:
                jobsId +=[record[1]]
                if record[2] != 'No':
                  files += [record[0]]
          depth-=1 
        
        ancestorList[fileName]=files    
      else:
        logicalFileNames['Failed']+=[fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK(logicalFileNames)
  
  #############################################################################  
  def getAllAncestors(self, lfn, depth):
    logicalFileNames={}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    jobsId = []
    job_id = -1
    if depth < 1:
      depth = 1
    odepth = depth 
    gLogger.debug('original',lfn)
    for fileName in lfn:
      depth = odepth
      jobsId = []
      gLogger.debug('filename',fileName)
      jobsId = []
      result = self.dbR_.executeStoredFunctions('BKK_MONITORING.getJobId',LongType,[fileName])
      if not result["OK"]:
        gLogger.error('Ancestor',result['Message'])
      else:
        job_id = int(result['Value'])
      if job_id != 0:
        jobsId = [job_id]
        files = []
        while (depth-1) and jobsId:
           for job_id in jobsId:
             command = 'select files.fileName,files.jobid, files.gotreplica from inputfiles,files where inputfiles.fileid=files.fileid and inputfiles.jobid='+str(job_id)
             jobsId=[]
             res = self.dbR_._query(command)
             if not res['OK']:
               gLogger.error('Ancestor',result["Message"])
             else:
               dbResult = res['Value']
               for record in dbResult:
                 jobsId +=[record[1]]
                 files += [record[0]]
           depth-=1 
        
        ancestorList[fileName]=files    
      else:
        logicalFileNames['Failed']+=[fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK(logicalFileNames)
  
  #############################################################################  
  def getAllAncestorsWithFileMetaData(self, lfn, depth):
    logicalFileNames={}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    jobsId = []
    job_id = -1
    if depth < 1:
      depth = 1
    odepth = depth 
    gLogger.debug('original',lfn)
    for fileName in lfn:
      depth = odepth
      jobsId = []
      gLogger.debug('filename',fileName)
      jobsId = []
      result = self.dbR_.executeStoredFunctions('BKK_MONITORING.getJobId',LongType,[fileName])
      if not result["OK"]:
        gLogger.error('Ancestor',result['Message'])
      else:
        job_id = int(result['Value'])
      if job_id != 0:
        jobsId = [job_id]
        files = []
        while (depth-1) and jobsId:
          for job_id in jobsId:
            command = 'select files.fileName,files.jobid, files.gotreplica, files.eventstat, files.eventtypeid from inputfiles,files where inputfiles.fileid=files.fileid and inputfiles.jobid='+str(job_id)
            jobsId=[]
            res = self.dbR_._query(command)
            if not res['OK']:
              gLogger.error('Ancestor',result["Message"])
            else:
              dbResult = res['Value']
              for record in dbResult:
                jobsId +=[record[1]]
                files += [{'FileName':record[0],'GotReplica':record[2],'EventStat':record[3],'EventType':record[4]}]
          depth-=1 
        
        ancestorList[fileName]=files    
      else:
        logicalFileNames['Failed']+=[fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK(logicalFileNames)
  
  #############################################################################  
  def getAllDescendents(self, lfn, depth = 0, production=0, checkreplica=False):
    logicalFileNames = {}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    logicalFileNames['NotProcessed'] = []
    file_id = -1
    fileids = []
    odepth = -1
    if depth < 1:
      depth = 1
      odepth = depth
    else:
      odepth = depth + 1
    
    condition = ''
    if production!=0:
      condition = ' and jobs.production='+str(production)
    
    gLogger.debug('original',lfn)
    for fileName in lfn:
      depth = odepth
      gLogger.debug('filename',fileName)
      fileids = []
      command = 'select files.fileid from files where filename=\''+str(fileName)+'\''
      res = self.dbR_._query(command)
      if not res["OK"]:
        gLogger.error('Ancestor',res['Message'])
      elif len(res['Value']) == 0:
        logicalFileNames['Failed']+=[fileName]
      else:
        file_id = int(res['Value'][0][0])
      if file_id != 0:
        fileids += [file_id]
        files = []
        while (depth-1) and fileids:
          for file_id in fileids:
            command = 'select jobid from inputfiles where fileid='+str(file_id)
            res = self.dbR_._query(command)
            if not res["OK"]:
              gLogger.error('Ancestor',res['Message'])
            elif len(res['Value']) != 0:
              job_ids = res['Value']
              fileids.remove(file_id)
              
              for i in job_ids:
                job_id = i[0]
                command = 'select files.fileName,files.fileid,files.gotreplica from files,jobs where files.jobid=jobs.jobid and files.jobid='+str(job_id)+condition
                res = self.dbR_._query(command)
                if not res["OK"]:
                  gLogger.error('Ancestor',res['Message'])
                elif len(res['Value']) == 0:
                  logicalFileNames['NotProcessed']+=[fileName]
                else:
                  dbResult = res['Value']
                  for record in dbResult:
                    fileids +=[record[1]]
                    if checkreplica:
                      if record[2] != 'No':
                        files += [record[0]]
                    else:
                      files += [record[0]]
          depth-=1 
        
        ancestorList[fileName]=files    
      else:
        logicalFileNames['Failed']+=[fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK(logicalFileNames)
  
  #############################################################################  
  def getDescendents(self, lfn, depth = 0):
    logicalFileNames = {}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    logicalFileNames['NotProcessed'] = []
    file_id = -1
    fileids = []
    odepth = -1
    if depth < 1:
      depth = 1
      odepth = depth
    else:
      odepth = depth + 1
    
    gLogger.debug('original',lfn)
    for fileName in lfn:
      depth = odepth
      gLogger.debug('filename',fileName)
      fileids = []
      command = 'select files.fileid from files where filename=\''+str(fileName)+'\''
      res = self.dbR_._query(command)
      if not res["OK"]:
        gLogger.error('Ancestor',res['Message'])
      elif len(res['Value']) == 0:
        logicalFileNames['Failed']+=[fileName]
      else:
        file_id = int(res['Value'][0][0])
      if file_id != 0:
        fileids += [file_id]
        files = []
        while (depth-1) and fileids:
          for file_id in fileids:
            command = 'select jobid from inputfiles where fileid='+str(file_id)
            res = self.dbR_._query(command)
            if not res["OK"]:
              gLogger.error('Ancestor',res['Message'])
            elif len(res['Value']) != 0:
              job_ids = res['Value']
              fileids.remove(file_id)
              
              for i in job_ids:
                job_id = i[0]
                command = 'select files.fileName,files.fileid,files.gotreplica from files where files.jobid='+str(job_id)
                res = self.dbR_._query(command)
                if not res["OK"]:
                  gLogger.error('Ancestor',res['Message'])
                elif len(res['Value']) == 0:
                  logicalFileNames['NotProcessed']+=[fileName]
                else:
                  dbResult = res['Value']
                  for record in dbResult:
                    if record[2] != 'No':
                      files += [record[0]]
                      fileids +=[record[1]]
          depth-=1 
        
        ancestorList[fileName]=files    
      else:
        logicalFileNames['Failed']+=[fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK(logicalFileNames)
    
  """
  data insertation into the database
  """
  #############################################################################
  def checkfile(self, fileName): #file

    result = self.dbR_.executeStoredProcedure('BKK_ORACLE.checkfile',[fileName])
    if result['OK']: 
      res = result['Value']
      if len(res)!=0:
        return S_OK(res)
      else:
        gLogger.warn("File not found! ",str(fileName))
        return S_ERROR("File not found!"+str(fileName))
    else:
      return S_ERROR(result['Message'])
    return result
  
  #############################################################################
  def checkFileTypeAndVersion(self, type, version): #fileTypeAndFileTypeVersion(self, type, version):
    result = self.dbR_.executeStoredProcedure('BKK_ORACLE.checkFileTypeAndVersion',[type, version])
    if result['OK']:
      res = result['Value']
      if len(res)!=0:
        return S_OK(res)
      else:
        gLogger.error("File type not found! ",str(type))
        return S_ERROR("File type not found!"+str(type))
    else:
      return S_ERROR(result['Message'])
    
    return result

    
  
  #############################################################################
  def checkEventType(self, eventTypeId):  #eventType(self, eventTypeId):
    
    result = self.dbR_.executeStoredProcedure('BKK_ORACLE.checkEventType',[eventTypeId])
    if result['OK']:
      res = result['Value']
      if len(res)!=0:
        return S_OK(res)
      else:
        gLogger.error("Event type not found! ",str(eventTypeId))
        return S_ERROR("Event type not found!"+str(eventTypeId))
    else:
      return S_ERROR(result['Message'])
    return result
  
  #############################################################################
  def insertJob(self, job):
    
    gLogger.info("Insert job into database!")
    attrList = {'ConfigName':None, \
                 'ConfigVersion':None, \
                 'DiracJobId':None, \
                 'DiracVersion':None, \
                 'EventInputStat':None, \
                 'ExecTime':None, \
                 'FirstEventNumber':None, \
                 'JobEnd':None, \
                 'JobStart':None, \
                 'Location':None, \
                 'Name':None, \
                 'NumberOfEvents':None, \
                 'Production':None, \
                 'ProgramName':None, \
                 'ProgramVersion':None, \
                 'StatisticsRequested':None, \
                 'WNCPUPOWER':None, \
                 'CPUTIME':None, \
                 'WNCACHE':None, \
                 'WNMEMORY':None, \
                 'WNMODEL':None, \
                 'WorkerNode':None, \
                 'RunNumber':None, \
                 'FillNumber':None, \
                 'WNCPUHS06': 0}
    
    for param in job:
      if not attrList.__contains__(param):
        gLogger.error("insert job error: "," the job table not contains "+param+" this attributte!!")
        return S_ERROR(" The job table not contains "+param+" this attributte!!")
  
      if param=='JobStart' or param=='JobEnd': # We have to convert data format
        dateAndTime = job[param].split(' ')
        date = dateAndTime[0].split('-')
        time = dateAndTime[1].split(':')
        if len(time) > 2:
          timestamp = datetime.datetime(int(date[0]), int(date[1]), int(date[2]), int(time[0]), int(time[1]), int(time[2]), 0)
        else:
          timestamp = datetime.datetime(int(date[0]), int(date[1]), int(date[2]), int(time[0]), int(time[1]), 0, 0)
        attrList[param]=timestamp
      else:
        attrList[param] = job[param]
        
      
      
    result = self.dbW_.executeStoredFunctions('BKK_ORACLE.insertJobsRow',LongType,[ attrList['ConfigName'], attrList['ConfigVersion'], \
                  attrList['DiracJobId'], \
                  attrList['DiracVersion'], \
                  attrList['EventInputStat'], \
                  attrList['ExecTime'], \
                  attrList['FirstEventNumber'], \
                  attrList['JobEnd'], \
                  attrList['JobStart'], \
                  attrList['Location'], \
                  attrList['Name'], \
                  attrList['NumberOfEvents'], \
                  attrList['Production'], \
                  attrList['ProgramName'], \
                  attrList['ProgramVersion'], \
                  attrList['StatisticsRequested'], \
                  attrList['WNCPUPOWER'], \
                  attrList['CPUTIME'], \
                  attrList['WNCACHE'], \
                  attrList['WNMEMORY'], \
                  attrList['WNMODEL'], \
                  attrList['WorkerNode'], \
                  attrList['RunNumber'], \
                  attrList['FillNumber'], \
                  attrList['WNCPUHS06'] ])           
    return result
  
  #############################################################################
  def insertInputFile(self, jobID, FileId):
    result = self.dbW_.executeStoredProcedure('BKK_ORACLE.insertInputFilesRow',[FileId, jobID], False)
    return result
  #############################################################################
  def insertOutputFile(self, file):
  
      attrList = {  'Adler32':None, \
                    'CreationDate':None, \
                    'EventStat':None, \
                    'EventTypeId':None, \
                    'FileName':None,  \
                    'FileTypeId':None, \
                    'GotReplica':None, \
                    'Guid':None,  \
                    'JobId':None, \
                    'MD5Sum':None, \
                    'FileSize':0, \
                    'FullStat':None }
      
      for param in file:
        if not attrList.__contains__(param):
          gLogger.error("insert file error: "," the files table not contains "+param+" this attributte!!")
          return S_ERROR(" The files table not contains "+param+" this attributte!!")
        
        if param=='CreationDate': # We have to convert data format
          dateAndTime = file[param].split(' ')
          date = dateAndTime[0].split('-')
          time = dateAndTime[1].split(':')
          timestamp = datetime.datetime(int(date[0]), int(date[1]), int(date[2]), int(time[0]), int(time[1]), 0, 0)
          attrList[param]=timestamp
        else:
          attrList[param] = file[param]
      utctime = datetime.datetime.utcnow()
      result = self.dbW_.executeStoredFunctions('BKK_ORACLE.insertFilesRow',LongType, [  attrList['Adler32'], \
                    attrList['CreationDate'], \
                    attrList['EventStat'], \
                    attrList['EventTypeId'], \
                    attrList['FileName'],  \
                    attrList['FileTypeId'], \
                    attrList['GotReplica'], \
                    attrList['Guid'],  \
                    attrList['JobId'], \
                    attrList['MD5Sum'], \
                    attrList['FileSize'], \
                    attrList['FullStat'], utctime ] ) 
      return result
      
  #############################################################################
  def updateReplicaRow(self, fileID, replica): #, name, location):
    utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    command = 'update files set inserttimestamp = TO_TIMESTAMP(\''+str(utctime)+'\',\'YYYY-MM-DD HH24:MI:SS\'), GOTREPLICA=\''+str(replica)+'\' where fileid='+str(fileID)
    res = self.dbW_._query(command)
    return res
  
  #############################################################################
  def deleteJob(self, jobID):
    result = self.dbW_.executeStoredProcedure('BKK_ORACLE.deleteJob',[jobID], False)
    return result
  
  #############################################################################
  def deleteInputFiles(self, jobid):
    result = self.dbW_.executeStoredProcedure('BKK_ORACLE.deleteInputFiles',[jobid], False)
    return result
  
  #############################################################################
  def deleteFile(self, fileid):
    result = self.dbW_.executeStoredProcedure('BKK_ORACLE.deletefile',[fileid], False)
    return result
  
  #############################################################################
  def deleteFiles(self, lfns):
    return S_ERROR('Not Implemented !!')
    '''
    result = {}
    for file in lfns:
      res = self.checkfile(file)
      if res['OK']:
        fileID = long(res['Value'][0][0])
        jobID =  long(res['Value'][0][1])
        res = self.deleteFile(fileID)
        if res['OK']:
          res = self.deleteInputFiles(jobID)
          res = self.deleteJob(jobID)
          if res['OK']:        
            result['Succesfull']=file
          else:
            result['Failed']=file
      else:
        result['Failed']=file
      
    return S_OK(result)    
  '''
  
  #############################################################################
  def insertQuality(self, fileID, group, flag ):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertQualityParam(self, fileID, qualityID, name, value):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def modifyReplica(self, fileID , name, value):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity):
    return self.dbW_.executeStoredFunctions('BKK_ORACLE.insertSimConditions', LongType, [simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity])
  
  #############################################################################
  def getSimulationCondIdByDesc(self, desc):
    return self.dbR_.executeStoredFunctions('BKK_ORACLE.getSimulationCondIdByDesc', LongType, [desc])

  #############################################################################
  def getDataTakingCondId(self, condition):
    command = 'select DaqPeriodId from data_taking_conditions where ' 
    for param in condition:
      if type(condition[param]) == types.StringType and len(condition[param].strip()) == 0: 
        command += str(param)+' is NULL and '
      elif condition[param] != None:
        command +=  str(param)+'=\''+condition[param]+'\' and '
      else:
        command += str(param)+' is NULL and '
    
    command = command[:-4]
    res = self.dbR_._query(command)
    if res['OK']:
        if len(res['Value'])==0:
          command = 'select DaqPeriodId from data_taking_conditions where ' 
          for param in condition:
            if param != 'Description':
              if type(condition[param]) == types.StringType and len(condition[param].strip()) == 0: 
                command += str(param)+' is NULL and '
              elif condition[param] != None:
                command +=  str(param)+'=\''+condition[param]+'\' and '
              else:
                command += str(param)+' is NULL and '
            
          command = command[:-4]
          retVal = self.dbR_._query(command)
          if retVal['OK']:
            if len(retVal['Value'])!=0:
              return S_ERROR('Only the Description is different, the other attributes are the same and they are exists in the DB!')
    return res
  
  #############################################################################
  def getSimConditions(self):
    return self.dbR_.executeStoredProcedure('BKK_ORACLE.getSimConditions',[])
  
  #############################################################################
  def insertDataTakingCond(self, conditions): 
    datataking = {  'Description':None,\
                    'BeamCond':None, \
                    'BeamEnergy':None, \
                    'MagneticField':None, \
                    'VELO':None, \
                    'IT':None,  \
                    'TT':None, \
                    'OT':None, \
                    'RICH1':None,  \
                    'RICH2':None, \
                    'SPD_PRS':None,\
                    'ECAL':None, \
                    'HCAL':None, \
                    'MUON':None, \
                    'L0':None, \
                    'HLT':None,
                    'VeloPosition':None}
        
    for param in conditions:
      if not datataking.__contains__(param):
        gLogger.error("insert datataking error: "," the files table not contains "+param+" this attributte!!")
        return S_ERROR(" The datatakingconditions table not contains "+param+" this attributte!!")
      datataking[param] = conditions[param]
        
    res = self.dbW_.executeStoredFunctions('BKK_ORACLE.insertDataTakingCond', LongType, [datataking['Description'], datataking['BeamCond'], datataking['BeamEnergy'], \
                                                                                  datataking['MagneticField'], datataking['VELO'], \
                                                                                  datataking['IT'], datataking['TT'], datataking['OT'], \
                                                                                  datataking['RICH1'], datataking['RICH2'], \
                                                                                  datataking['SPD_PRS'], datataking['ECAL'], \
                                                                                  datataking['HCAL'], datataking['MUON'], datataking['L0'], datataking['HLT'], datataking['VeloPosition'] ])
    return res
  
  #############################################################################
  def getAvailableFileTypes(self):
    command = ' select distinct Name from filetypes'
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def removeReplica(self, fileName):
    result = self.checkfile(fileName) 
    if result['OK']:
      fileID = long(result['Value'][0][0])
      res = self.updateReplicaRow(fileID, 'No')
      if res['OK']:
        return S_OK("Replica has ben removed!!!")
      else:
        return S_ERROR(res['Message'])      
    else:
      return S_ERROR('The file '+fileName+'not exist in the BKK database!!!')
  
  #############################################################################
  def getFileMetadata(self, lfns):
    result = {}
    for file in lfns:
      res = self.dbR_.executeStoredProcedure('BKK_ORACLE.getFileMetaData',[file])
      if not res['OK']:
        result[file]= res['Message']
      else:
        records = res['Value']  
        for record in records:
          row = {'ADLER32':record[1],'CreationDate':record[2],'EventStat':record[3],'FullStat':record[10],'EventTypeId':record[4],'FileType':record[5],'GotReplica':record[6],'GUID':record[7],'MD5SUM':record[8],'FileSize':record[9],'DQFlag':record[11],'JobId':record[12],'RunNumber':record[13]}
          result[file]= row
    return S_OK(result)
  
  #############################################################################
  def getFilesInformations(self,lfns):
    result = {}
    res = self.getFileMetadata(lfns)
    if not res['OK']:
      result = res
    else:
      records = res['Value']  
      for file in records:
        value = records[file]
        command = 'select jobs.runnumber,jobs.fillnumber, configurations.configname,configurations.configversion from jobs,configurations where configurations.configurationid= jobs.configurationid and jobs.jobid='+str(value['JobId'])
        res = self.dbR_._query(command)
        if not res['OK']:
          result[file]= res['Message']
        else:
            info = res['Value']
            if len(info)!=0:
              row = {'RunNumber':info[0][0],'FillNumber':info[0][1],'ConfigName':info[0][2],'ConfigVersion':info[0][3]}
              value.pop('ADLER32')
              value.pop('GUID')
              value.pop('MD5SUM')
              value.pop('JobId')
              row.update(value)
              result[file]= row
            else:
              result[file]= {}
    return S_OK(result)
  
  #############################################################################
  def getProductionsWithPrgAndEvt(self, programName='ALL', programversion='ALL', evt='ALL'):
    condition = ''
    if programName != 'ALL':
      condition = ' and jobs.PROGRAMNAME=\''+str(programName)+'\'' 
    
    if programversion!='ALL':
      condition += ' and jobs.PROGRAMVERSION=\''+str(programversion)+'\''
    
    if evt != 'ALL':
      condition += ' and eventtypes.EVENTTYPEID='+str(evt)
    
    command = 'select distinct eventtypes.EVENTTYPEID, eventtypes.DESCRIPTION, jobs.PRODUCTION from jobs,eventtypes, files where jobs.jobid=files.jobid and files.eventtypeid=eventtypes.eventtypeid '+condition
    res = self.dbR_._query(command)
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      return S_OK(res['Value'])
      
  #############################################################################
  def getFileMetaDataForUsers(self, lfns):
    totalrecords = len(lfns)
    parametersNames = ['Name', 'FileSize','FileType','CreationDate','EventTypeId','EventStat','GotReplica']
    records = []
    for file in lfns:
      res = self.dbR_.executeStoredProcedure('BKK_ORACLE.getFileMetaData',[file])
      if not res['OK']:
        records = [str(res['Message'])]
      else:
        values = res['Value']  
        for record in values:
          row = [file, record[9],record[5], record[2], record[4], record[3], record[6]]
          records += [row]
    return S_OK({'TotalRecords':totalrecords,'ParameterNames':parametersNames,'Records':records}) 
  
  #############################################################################
  def __getProductionStatisticsForUsers(self, prod):
    command = 'select count(*), SUM(files.EventStat), SUM(files.FILESIZE) from files ,jobs where jobs.jobid=files.jobid and jobs.production='+str(prod)
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getProductionFilesForUsers(self, prod, ftypeDict, SortDict, StartItem, Maxitems):
    command = ''
    parametersNames = ['Name', 'FileSize','FileType','CreationDate','EventTypeId','EventStat','GotReplica']
    records = []
    
    totalrecords = 0
    nbOfEvents = 0
    filesSize = 0
    ftype = ftypeDict['type']
    if len(SortDict) > 0:
      res = self.__getProductionStatisticsForUsers(prod)
      if not res['OK']:
        gLogger.error(res['Message'])
      else:
        totalrecords = res['Value'][0][0]
        nbOfEvents = res['Value'][0][1]
        filesSize = res['Value'][0][2]
        
    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fileType)
      if not res['OK']:
        gLogger.error(res['Message'])
        return S_ERROR('Oracle error'+res['Message'])
      else:
        if len(res['Value']) == 0:
          return S_ERROR('File Type not found:'+str(ftype)) 
        
        ftypeId = res['Value'][0][0]
        
        command = 'select rnum, filename, filesize, \''+str(ftype)+'\' , creationdate, eventtypeId, eventstat,gotreplica from \
                ( select rownum rnum, filename, filesize, \''+str(ftype)+'\' , creationdate, eventtypeId, eventstat, gotreplica \
                from ( select files.filename, files.filesize, \''+str(ftype)+'\' , files.creationdate, files.eventtypeId, files.eventstat,files.gotreplica \
                           from jobs,files where \
                           jobs.jobid=files.jobid and \
                           files.filetypeid='+str(ftypeId)+' and \
                           jobs.production='+str(prod)+' Order by files.filename) where rownum <='+str(Maxitems)+ ') where rnum >'+str(StartItem) 
    else:
      
      command = 'select rnum, filename, filesize, name, creationdate, eventtypeId, eventstat,gotreplica from \
                ( select rownum rnum, filename, filesize, name, creationdate, eventtypeId, eventstat, gotreplica \
                from ( select files.filename, files.filesize, filetypes.name, files.creationdate, files.eventtypeId, files.eventstat,files.gotreplica \
                           from jobs,files,filetypes where \
                           jobs.jobid=files.jobid and \
                           files.filetypeid=filetypes.filetypeid and \
                           jobs.production='+str(prod)+' Order by files.filename) where rownum <='+str(Maxitems)+ ') where rnum >'+str(StartItem) 
   
    res = self.dbR_._query(command)
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        row = [record[1],record[2],record[3],record[4],record[5],record[6],record[7]]
        records += [row]
    else:
      return S_ERROR(res['Message'])
    return S_OK({'TotalRecords':totalrecords,'ParameterNames':parametersNames,'Records':records,'Extras': {'GlobalStatistics':{'Number of Events':nbOfEvents, 'Files Size':filesSize }}}) 
  
  #############################################################################
  def exists(self, lfns):
    result ={}
    for file in lfns:
      res = self.dbR_.executeStoredFunctions('BKK_ORACLE.fileExists', LongType, [file])
      if not res['OK']:
        return S_ERROR(res['Message'])
      if res['Value'] ==0:
        result[file] = False
      else:
        result[file] = True
    return S_OK(result)
   
  #############################################################################
  def addReplica(self, fileName):
    result = self.checkfile(fileName) 
    if result['OK']:
      fileID = long(result['Value'][0][0])
      res = self.updateReplicaRow(fileID, 'Yes')
      if res['OK']:
        return S_OK("Replica has ben added!!!")
      else:
        return S_ERROR(res['Message'])      
    else:
      return S_ERROR('The file '+fileName+'not exist in the BKK database!!!')

  #############################################################################
  def getPassIndexID(self, programName, programVersion):
    returnValue = None
    res = self.insert_aplications(programName, programVersion, '', '', '','')
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      step0 = res['Value']
      retVal = self.insert_pass_index_new('Real Data', step0, None, None, None, None, None, None)
      if not retVal['OK']:
        returnValue = S_ERROR(retVal['Message'])
      else:
        returnValue = S_OK(retVal['Value'])    
      return returnValue 
  
  #############################################################################
  def insert_pass_index_new(self, groupdesc, step0, step1, step2, step3, step4, step5,step6):
    command = ' select groupId from PASS_GROUP where GROUPDESCRIPTION=\''+str(groupdesc)+'\''
    retVal = self.dbR_._query(command)
    groupid = None
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      if len(retVal['Value']) == 0:
        command = ' SELECT GROUPID_SEQ.nextval from dual'
        retVal = self.dbR_._query(command)
        if not retVal['OK']:
          return S_ERROR(retVal['Message'])
        else:
          groupid = retVal['Value'][0][0]
          command = ' insert /* +APPEND */ into PASS_GROUP (GROUPID, GROUPDESCRIPTION) values ('+str(groupid)+',\''+str(groupdesc)+'\')'
          retVal = self.dbW_._query(command)
          if not retVal['OK']:
            return S_ERROR(retVal['Message'])
      else:  
        groupid = retVal['Value'][0][0]
      
      retVal = self.check_pass_index(groupid, step0, step1, step2, step3, step4, step5,step6)
      if not retVal['OK']:
        return S_ERROR(retVal['Message'])
      elif retVal['Value']==None:
        command = 'select PASS_INDEX_SEQ.nextval from dual'
        retVal = self.dbR_._query(command)
        if not retVal['OK']:
          return S_ERROR(retVal['Message'])
        else:
          passid = retVal['Value'][0][0]
          descr = 'Pass'+str(passid)
          values = str(passid)+','
          values += '\''+descr+'\','
          values += str(groupid) +','
          if step0 != None:
            values += str(step0) +','
          else:
            values += 'NULL,'
          
          if step1 != None:
            values += str(step1) +','
          else:
            values += 'NULL,'
          
          if step2 != None:
            values += str(step2) +','
          else:
            values += 'NULL,'
            
          if step3 != None:
            values += str(step3) +','
          else:
            values += 'NULL,'
            
          if step4 != None:
            values += str(step4) +','
          else:
            values += 'NULL,'
            
          if step5 != None:
            values += str(step5) +','
          else:
            values += 'NULL,'
            
          if step6 != None:
            values += str(step6)
          else:
            values += 'NULL'
            
          command = 'insert /* +APPEND */ into pass_index (PASSID, DESCRIPTION, GROUPID, STEP0, STEP1, STEP2, STEP3, STEP4, STEP5, STEP6) values('+values+')'
          retVal = self.dbW_._query(command)
          if not retVal['OK']:
            return S_ERROR(retVal['Message'])
          else:
            return S_OK(passid)
      else:
          return S_OK(retVal['Value'])
          
    return S_ERROR()  
  #############################################################################
  def getRunInformations(self, runnb):
    command = 'select distinct jobs.fillnumber, configurations.configname, configurations.configversion, data_taking_conditions.description, pass_group.groupdescription, jobs.jobstart, jobs.jobend from \
          jobs, configurations,data_taking_conditions,productions, pass_group, pass_index \
         where jobs.configurationid=configurations.configurationid and data_taking_conditions.daqperiodid=productions.simcondid and \
         productions.passid=pass_index.passid and pass_index.groupid=pass_group.groupid and \
         jobs.production=productions.production and jobs.production<0 and jobs.runnumber='+str(runnb)
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    value = retVal['Value']
    if len(value) == 0:
      return S_ERROR('This run is missing in the BKK DB!')
    result = {'Configuration Name':value[0][1],'Configuration Version':value[0][2],'FillNumber':value[0][0]}
    result['DataTakingDescription']=value[0][3]
    result['ProcessingPass']=value[0][4]
    result['RunStart'] = value[0][5]
    result['RunEnd'] = value[0][6]
    
    command = ' select count(*), SUM(files.EventStat), SUM(files.FILESIZE), sum(files.fullstat), files.eventtypeid from files,jobs \
         where files.JobId=jobs.JobId and  \
         files.gotReplica=\'Yes\' and \
         jobs.production<0 and \
         jobs.runnumber='+str(runnb)+' Group by files.eventtypeid'
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    value = retVal['Value']
    if len(value) == 0:
      return S_ERROR('Replica flag is not set!')
    nbfile = [] 
    nbevent = [] 
    fsize = [] 
    fstat = []
    stream = []
    for i in value:
      nbfile += [i[0]]
      nbevent += [i[1]]
      fsize += [i[2]]
      fstat += [i[3]]
      stream += [i[4]]
          
    result['Number of file'] = nbfile
    result['Number of events'] = nbevent
    result['File size'] = fsize
    result['FullStat'] = fstat
    result['Stream'] = stream

    return S_OK(result)
  
  #############################################################################
  def checkProductionStatus(self, productionid = None, lfns = []):
    result = {}
    missing = []
    replicas = []
    noreplicas = []
    if productionid != None:
      command = 'select files.filename, files.gotreplica from files,jobs where \
                 files.jobid=jobs.jobid and \
                 jobs.production='+str(productionid)
      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        return S_ERROR(retVal['Message'])
      files = retVal['Value']
      for file in files:
        if file[1] == 'Yes':
          replicas +=  [file[0]]
        else:
          noreplicas += [file[0]]
      result['replica'] = replicas
      result['noreplica'] = noreplicas
    elif len(lfns) != 0:
      for file in lfns:
        command = ' select files.filename, files.gotreplica from files where filename=\''+str(file)+'\''
        retVal = self.dbR_._query(command)
        if not retVal['OK']:
          return S_ERROR(retVal['Message'])
        value = retVal['Value']
        if len(value) == 0:
          missing += [file]
        else:
          for i in value:
            if i[1] == 'Yes':
              replicas +=  [i[0]]
            else:
              noreplicas += [i[0]]
      result['replica'] = replicas
      result['noreplica'] = noreplicas
      result['missing'] = missing
    
    return S_OK(result)
  
  #############################################################################
  def check_pass_index(self, groupid, step0, step1, step2, step3, step4, step5,step6):
    conditions = ''
    if step0 != None:
      conditions += ' and step0='+str(step0)
    if step1 != None:
      conditions += ' and step1='+str(step1) 
    if step2 != None:
      conditions += ' and step2='+str(step2)
    if step3 != None:
      conditions += ' and step3='+str(step3)
    if step4 != None:
      conditions += ' and step4='+str(step4)
    if step5 != None:
      conditions += ' and step5='+str(step5)
    if step6 != None:
      conditions += ' and step6='+str(step6)
    command = ' select passid from pass_index where groupid='+str(groupid)+conditions
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      if len(retVal['Value']) == 0:
        return S_OK(None)
      else:
        return S_OK(retVal['Value'][0][0])
    return S_ERROR()
  
  #############################################################################
  def getLogfile(self, lfn):
    command = 'select files.jobid from files where files.filename=\''+str(lfn)+'\''
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    if len(retVal['Value']) == 0:
      return S_ERROR('Job not in the DB')
    jobid = retVal['Value'][0][0]
    command = 'select filename from files where (files.filetypeid=17 or files.filetypeid=9) and files.jobid='+str(jobid)
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    else:
      if len(retVal['Value']) == 0:
        return S_ERROR('Log file is not exist!')
      return S_OK(retVal['Value'][0][0])
    return S_ERROR('getLogfile error!')
  
  #############################################################################
  def insertProcessing_pass(self, prod, passid, simcond):
    return self.dbW_.executeStoredProcedure('BKK_ORACLE.insertProcessing_PASS', [prod, passid, simcond], False)
  
  #############################################################################
  def getProcessingPassGroups(self):
     return self.dbR_.executeStoredProcedure('BKK_ORACLE.getProcessingPassGroups',[])
  
  #############################################################################
  def insert_pass_group(self, gropupdesc):
    return self.dbW_.executeStoredFunctions('BKK_ORACLE.insert_pass_group', LongType, [gropupdesc])
  
  #############################################################################
  def insertEventTypes(self, evid, desc, primary):
    return self.dbW_.executeStoredProcedure('BKK_ORACLE.insertEventTypes',[desc, evid, primary], False)
  
  #############################################################################
  def updateEventType(self, evid, desc, primary):
    return self.dbW_.executeStoredProcedure('BKK_ORACLE.updateEventTypes',[desc, evid, primary], False)
  
    
  #############################################################################
  def checkProcessingPassAndSimCond(self, production):
    command = ' select count(*) from productions where production='+ str(production)
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getProductionSummary(self, cName, cVersion, simdesc='ALL', pgroup='ALL', production='ALL', ftype='ALL', evttype='ALL'):
    conditions = ''
    
    if cName != 'ALL':
      conditions += ' and bookkeepingview.configname=\''+cName+'\'' 
        
    if cVersion != 'ALL':
      conditions += ' and bookkeepingview.configversion=\''+cVersion+'\''
        
    if ftype != 'ALL':
      fType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\''+str(ftype)+'\''
      res = self.dbR_._query(fType)
      if not res['OK']:
        gLogger.error(res['Message'])
        return S_ERROR(res['Message'])
      else:
        value = res['Value']
        if len(value) == 0:
          return S_ERROR('File type is not found!')
        ftypeId = res['Value'][0][0]
        conditions += ' and files.filetypeid='+str(ftypeId)
    else:
      conditions += ' and bookkeepingview.filetypeid= files.filetypeid  \
                      and bookkeepingview.filetypeid= filetypes.filetypeid '
    if evttype != 'ALL':
      conditions += ' and bookkeepingview.eventtypeid='+str(evttype)
    else:
      conditions += ' and bookkeepingview.eventtypeid=files.eventtypeid '
    
    if production != 'ALL':
      conditions += ' and jobs.production='+str(production)
      conditions += ' and jobs.production= bookkeepingview.production '
    else:
       conditions += ' and jobs.production= bookkeepingview.production '
  
    if simdesc != 'ALL':
      conditions += 'and productions.simcondid = simulationconditions.simid '
      conditions += 'and simulationconditions.simdescription = \''+str(simdesc)+'\''
    else:
      conditions += 'and productions.simcondid = simulationconditions.simid '    
    
    command = ' select bookkeepingview.configname, bookkeepingview.configversion, simulationconditions.simdescription, \
       pass_group.groupdescription, bookkeepingview.eventtypeid, \
       bookkeepingview.description, bookkeepingview.production, filetypes.name, sum(files.eventstat) \
  from jobs, bookkeepingview,files, filetypes, productions, simulationconditions, pass_index, pass_group \
        where jobs.jobid= files.jobid and \
        files.gotreplica=\'Yes\' and \
        bookkeepingview.programname= jobs.programname and \
        bookkeepingview.programversion= jobs.programversion and \
        bookkeepingview.production = productions.production and \
        productions.passid= pass_index.passid and \
        pass_index.groupid=pass_group.groupid' + conditions 
    
    command += ' Group by  bookkeepingview.configname, bookkeepingview.configversion, \
            simulationconditions.simdescription, pass_group.groupdescription,\
            bookkeepingview.eventtypeid, bookkeepingview.description,\
            bookkeepingview.production, filetypes.name'
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR(retVal['Message'])
    
    parameters = ['ConfigurationName', 'ConfigurationVersion', 'SimulationDescription', 'Processing pass group', 'EventTypeId', 'EventType description','Production', 'FileType','Number of events']
    dbResult = retVal['Value']
    records = []
    nbRecords = 0
    for record in dbResult:
      row = [record[0],record[1],record[2],record[3],record[4],record[5],record[6], record[7],record[8]]
      records += [row]
      nbRecords += 1
    
    return S_OK({'TotalRecords':nbRecords,'ParameterNames':parameters,'Records':records,'Extras': {}})
            
  #############################################################################
  def getProductionSimulationCond(self, prod):
    command ='select simulationconditions.simdescription  FROM simulationconditions, productions where \
         productions.simcondid= simulationconditions.simid and productions.production='+str(prod)
     
    res = self.dbR_._query(command)
    simcond = ''
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      value = res['Value']
      if len(value) == 0:
        command = 'select DESCRIPTION from data_taking_conditions, productions where productions.simcondid=data_taking_conditions.daqperiodid and productions.production='+str(prod) 
        res = self.dbR_._query(command)
        if not res['OK']:
          return S_ERROR(res['Message'])
        else:
          value = res['Value']
          if len(value) != 0:
            daqdesc = value[0][0]
            return S_OK(daqdesc)
          else:
            return S_ERROR('The data taking or simulation condition is missing!')
      else:
        simcond = value[0][0]
    return S_OK(simcond)
  
  #############################################################################
  def getProductionProcessing(self, prod):
    command = 'select pass_group.groupdescription from pass_index, pass_group, productions where \
               productions.passid= pass_index.passid and pass_index.groupid= pass_group.groupid and productions.production='+str(prod)
    res = self.dbR_._query(command)
    procpass = ''
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      value = res['Value']
      procpass = value[0][0]
    return S_OK(procpass)
  
  #############################################################################
  def getFileHistory(self, lfn):
    command = 'select  files.fileid, files.filename,files.adler32,files.creationdate,files.eventstat,files.eventtypeid,files.gotreplica, \
files.guid,files.jobid,files.md5sum, files.filesize,files.fullstat, dataquality.dataqualityflag, files.inserttimestamp from files, dataquality \
where files.fileid in ( select inputfiles.fileid from files,inputfiles where files.jobid= inputfiles.jobid and files.filename=\''+str(lfn)+'\')\
and files.qualityid= dataquality.qualityid'
 
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getProductionInformationsFromView(self, prodid):
    command = 'select * from productioninformations where production='+str(prodid)
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  #
  #          MONITORING
  #############################################################################
  def getJobsNb(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getJobsNb', [prodid])
  
  #############################################################################
  def getNumberOfEvents(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getNumberOfEvents', [prodid])
  
  #############################################################################
  def getSizeOfFiles(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getSizeOfFiles', [prodid])
  
  #############################################################################
  def getNbOfFiles(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getNbOfFiles', [prodid])
  
  #############################################################################
  def getProductionInformation(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getProductionInformation', [prodid])
  
  #############################################################################
  def getSteps(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getSteps', [prodid])
  
  #############################################################################
  def getNbOfJobsBySites(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getJobsbySites', [prodid])
  
  #############################################################################
  def getConfigsAndEvtType(self, prodid):
    return self.dbR_.executeStoredProcedure('BKK_MONITORING.getConfigsAndEvtType',[prodid])
  
  #############################################################################
  def getAvailableTags(self):
    command = 'select name, tag from tags order by inserttimestamp desc'
    retVal = self.dbR_._query(command)
    if retVal['OK']:
      parameters = ['TagName','TagValue']
      dbResult = retVal['Value']
      records = []
      nbRecords = 0
      for record in dbResult:
        row = [record[0],record[1]]
        records += [row]
        nbRecords += 1
      return S_OK({'TotalRecords':nbRecords,'ParameterNames':parameters,'Records':records,'Extras': {}})        
    else:
      return retVal
  
  #############################################################################
  def getProcessedEvents(self, prodid):
    command = 'select sum(jobs.numberofevents) from jobs where jobs.production='+str(prodid)
    res = self.dbR_._query(command)
    return res
  
  #############################################################################
  def getRunsWithAGivenDates(self, dict):
    condition = ''
    if dict.has_key('AllowOutsideRuns') and dict['AllowOutsideRuns']:
      if not dict.has_key('StartDate') and not dict.has_key('EndDate'):
        return S_ERROR('The Start and End date must be given!')
      else:
        if dict.has_key('StartDate'):
          condition += ' and jobs.jobstart >= TO_TIMESTAMP (\''+str(dict['StartDate'])+'\',\'YYYY-MM-DD HH24:MI:SS\')'
      
        if dict.has_key('EndDate'):
          condition += ' and jobs.jobstart <= TO_TIMESTAMP (\''+str(dict['EndDate'])+'\',\'YYYY-MM-DD HH24:MI:SS\')'
    else:
      if dict.has_key('StartDate'):
        condition += ' and jobs.jobstart >= TO_TIMESTAMP (\''+str(dict['StartDate'])+'\',\'YYYY-MM-DD HH24:MI:SS\')'
      
      if dict.has_key('EndDate'):
        condition += ' and jobs.jobend <= TO_TIMESTAMP (\''+str(dict['EndDate'])+'\',\'YYYY-MM-DD HH24:MI:SS\')'
      elif dict.has_key('StartDate') and not dict.has_key('EndDate'):
        d = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S') 
        condition += ' and jobs.jobend <= TO_TIMESTAMP (\''+str(d)+'\',\'YYYY-MM-DD HH24:MI:SS\')'
    
    command = ' select jobs.runnumber from jobs where jobs.production < 0'+ condition
    retVal = self.dbR_._query(command)
    runIds = []
    if retVal['OK']:
      records = retVal['Value'] 
      for record in records:
        if record[0] != None:
          runIds += [record[0]]
    else:
      return S_ERROR(retVal['Message'])
    processedRuns = []
    notProcessedRuns = []
    for i in runIds:
      command = 'select files.filename from files,jobs where jobs.jobid=files.jobid and files.gotreplica=\'Yes\' and jobs.production<0 and jobs.runnumber='+str(i)
      retVal = self.dbR_._query(command)
      if retVal['OK']:
        files = retVal['Value']
        ok = True
        for file in files:
          name = file[0]
          retVal = self.getDescendents([name],1)
          files = retVal['Value']
          successful = files['Successful']
          failed = files['Failed']
          if len(successful[successful.keys()[0]]) == 0: 
            ok = False
        if ok:
          processedRuns += [i]
        else:
          notProcessedRuns += [i]  
    
    return S_OK({'Runs':runIds,'ProcessedRuns':processedRuns,'NotProcessedRuns':notProcessedRuns})
  
  #############################################################################
  def setProductionVisible(self, prodid, Value):
    if Value:
      command = 'update productions set visible=\'1\' where production='+str(prodid) 
    else:
      command = 'update productions set visible=\'0\' where production='+str(prodid)
    retVal = self.dbW_._query(command)
    return retVal