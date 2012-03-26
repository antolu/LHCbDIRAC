########################################################################
# $Id$
########################################################################
"""

"""

__RCSID__ = "$Id$"

from LHCbDIRAC.BookkeepingSystem.DB.IBookkeepingDB                import IBookkeepingDB
from types                                                           import *
from DIRAC                                                           import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config                         import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder                     import getDatabaseSection
#from DIRAC.Core.Utilities.OracleDB                                   import OracleDB
from LHCbDIRAC.BookkeepingSystem.DB.OracleDB                      import OracleDB
import datetime
import types, re
global ALLOWED_ALL
ALLOWED_ALL = 2

global default
default = 'ALL'

class OracleBookkeepingDB(IBookkeepingDB):

  #############################################################################
  def __init__(self):
    """
    """
    super(OracleBookkeepingDB, self).__init__()
    self.cs_path = getDatabaseSection('Bookkeeping/BookkeepingDB')

    self.dbHost = ''
    result = gConfig.getOption(self.cs_path + '/LHCbDIRACBookkeepingTNS')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: Host')
      return
    self.dbHost = result['Value']

    self.dbUser = ''
    result = gConfig.getOption(self.cs_path + '/LHCbDIRACBookkeepingUser')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: User')
      return
    self.dbUser = result['Value']

    self.dbPass = ''
    result = gConfig.getOption(self.cs_path + '/LHCbDIRACBookkeepingPassword')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: User')
      return
    self.dbPass = result['Value']


    self.dbServer = ''
    result = gConfig.getOption(self.cs_path + '/LHCbDIRACBookkeepingServer')
    if not result['OK']:
      gLogger.error('Failed to get the configuration parameters: User')
      return
    self.dbServer = result['Value']

    self.dbW_ = OracleDB(self.dbServer, self.dbPass, self.dbHost)
    self.dbR_ = OracleDB(self.dbUser, self.dbPass, self.dbHost)

  #############################################################################
  def __isSpecialFileType(self, flist):
    result = False
    if ('RAW' in flist) or ('MDF' in flist):
      result = True
    else:
      for i in flist:
        if re.search('HIST', i):
          result = True
          break
    return result

  #############################################################################
  def getAvailableSteps(self, dict={}):
    """
    I never implemented this kind of method in my life. I have to start some time!
    """
    start = 0
    max = 10
    paging = False
    retVal = None

    condition = ''
    tables = 'steps s, steps r, runtimeprojects rr '
    resut = S_ERROR()
    if len(dict) > 0:
      infiletypes = dict.get('InputFileTypes', default)
      outfiletypes = dict.get('OutputFileTypes', default)
      matching = dict.get('Equal', 'YES')

      if (type(matching) == types.BooleanType):
        if matching:
          matching = "YES"
        else:
          matching = 'NO'
      elif matching.upper() not in ['YES', 'NO']:
        return S_ERROR ('Wrong Equal value!')

      if infiletypes != default or outfiletypes != default:
        sort = dict.get('Sort', default)
        if sort != default:
          condition += 'Order by '
          order = sort.get('Order', 'Asc')
          if order.upper() not in ['ASC', 'DESC']:
            return S_ERROR("wrong sorting order!")
          items = sort.get('Items', default)
          if type(items) == types.ListType:
            ord = ''
            for item in items:
              ord += 's.%s,' % (item)
            condition += ' %s %s' % (ord[:-1], order)
          elif type(items) == types.StringType:
            condition += ' s.%s %s' % (items, order)
          else:
            result = S_ERROR('SortItems is not properly defined!')

        if type(infiletypes) == types.StringType:
          infiletypes = []
        if type(outfiletypes) == types.StringType:
          outfiletypes = []
        infiletypes.sort()
        outfiletypes.sort()
        values = 'lists( '
        for i in infiletypes:
          values += "'%s'," %(i)
        inp = values[:-1]+')'

        values = 'lists( '
        for i in outfiletypes:
          values += "'%s'," %(i)
        out = values[:-1]+')'

        command = "select * from table(BOOKKEEPINGORACLEDB.getStepsForFiletypes(%s, %s, '%s')) s %s" % (inp,out, matching.upper(), condition)
        retVal = self.dbR_._query(command)
      else:
        startDate = dict.get('StartDate', default)
        if startDate != default:
          condition += " and s.inserttimestamps >= TO_TIMESTAMP (' %s ' ,'YYYY-MM-DD HH24:MI:SS')" % (startDate)

        stepId = dict.get('StepId', default)
        if stepId != default:
          condition += ' and s.stepid= %s' % (str(stepId))

        stepName = dict.get('StepName', default)
        if stepName != default:
          if type(stepName) == types.StringType:
            condition += " and s.stepname='%s'" % (stepName)
          elif type(stepName) == types.ListType:
            values = ' and ('
            for i in stepName:
              values += " s.stepname='%s' or " % (i)
            condition += values[:-3] + ')'

        appName = dict.get('ApplicationName', default)
        if appName != default:
          if type(appName) == types.StringType:
            condition += " and s.applicationName='%s'" % (appName)
          elif type(appName) == types.ListType:
            values = ' and ('
            for i in appName:
              values += " s.applicationName='%s' or " % (i)
            condition += values[:-3] + ')'

        appVersion = dict.get('ApplicationVersion', default)
        if appVersion != default:
          if type(appVersion) == types.StringType:
            condition += " and s.applicationversion='%s'" % (appVersion)
          elif type(appVersion) == types.ListType:
            values = ' and ('
            for i in appVersion:
              values += " s.applicationversion='%s' or " % (i)
            condition += values[:-3] + ')'

        optFile = dict.get('OptionFiles', default)
        if optFile != default:
          if type(optFile) == types.StringType:
            condition += " and s.optionfiles='%s'" % (optFile)
          elif type(optFile) == types.ListType:
            values = ' and ('
            for i in optFile:
              values += " s.optionfiles='%s' or " % (i)
            condition += values[:-3] + ')'

        dddb = dict.get('DDDB', default)
        if dddb != default:
          if type(dddb) == types.StringType:
            condition += " and s.dddb='%s'" % (dddb)
          elif type(dddb) == types.ListType:
            values = ' and ('
            for i in dddb:
              values += " s.dddb='%s' or " % (i)
            condition += values[:-3] + ')'

        conddb = dict.get('CONDDB', default)
        if conddb != default:
          if type(conddb) == types.StringType:
            condition += " and s.conddb='%s'" % (conddb)
          elif type(conddb) == types.ListType:
            values = ' and ('
            for i in conddb:
              values += " s.conddb='%s' or " % (i)
            condition += values[:-3] + ')'

        extraP = dict.get('ExtraPackages', default)
        if extraP != default:
          if type(extraP) == types.StringType:
            condition += " and s.extrapackages='%s'" % (extraP)
          elif type(extraP) == types.ListType:
            values = ' and ('
            for i in extraP:
              values += " s.extrapackages='%s' or " % (i)
            condition += values + ')'

        visible = dict.get('Visible', default)
        if visible != default:
          if type(visible) == types.StringType:
            condition += " and s.visible='%s'" % (visible)
          elif type(visible) == types.ListType:
            values = ' and ('
            for i in visible:
              values += " s.visible='%s' or " % (i)
            condition += values[:-3] + ')'

        procPass = dict.get('ProcessingPass', default)
        if procPass != default:
          if type(procPass) == types.StringType:
            condition += " and s.processingpass='%s'" % (procPass)
          elif type(procPass) == types.ListType:
            values = ' and ('
            for i in procPass:
              values += " s.processingpass='%s' or " % (i)
            condition += values[:-3] + ')'

        usable = dict.get('Usable', default)
        if usable != default:
          if type(usable) == types.StringType:
            condition += " and s.usable='%s'" % (usable)
          elif type(usable) == types.ListType:
            values = ' and ('
            for i in usable:
              values += " s.usable='%s' or " % (i)
            condition += values[:-3] + ')'

        runtimeProject = dict.get('RuntimeProjects', default)
        if runtimeProject != default:
          condition += " and s.runtimeProject=%d" % (runtimeProject)

        dqtag = dict.get('DQTag', default)
        if dqtag != default:
          if type(dqtag) == types.StringType:
            condition += " and s.dqtag='%s'" % (dqtag)
          elif type(dqtag) == types.ListType:
            values = ' and ('
            for i in dqtag:
              values += "  s.dqtag='%s' or " % (i)
            condition += values[:-3] + ')'

        optsf = dict.get('OptionsFormat', default)
        if optsf != default:
          if type(optsf) == types.StringType:
            condition += " and s.optionsFormat='%s'" % (optsf)
          elif type(optsf) == types.ListType:
            values = ' and ('
            for i in optsf:
              values += " s.optionsFormat='%s' or " % (i)
            condition += values[:-3] + ')'

        start = dict.get('StartItem', default)
        max = dict.get('MaxItem', default)

        if start != default and max != default:
          paging = True

        sort = dict.get('Sort', default)
        if sort != default:
          condition += 'Order by '
          order = sort.get('Order', 'Asc')
          if order.upper() not in ['ASC', 'DESC']:
            return S_ERROR("wrong sorting order!")
          items = sort.get('Items', default)
          if type(items) == types.ListType:
            ord = ''
            for item in items:
              ord += 's.%s,' % (item)
            condition += ' %s %s' % (ord[:-1], order)
          elif type(items) == types.StringType:
            condition += ' s.%s %s' % (items, order)
          else:
            result = S_ERROR('SortItems is not properly defined!')
        else:
          condition += ' order by s.inserttimestamps desc'

        if paging:
          command = " select sstepid, sname, sapplicationname, sapplicationversion, soptionfiles, sdddb, sconddb, sextrapackages, svisible, sprocessingpass, susable, sdqtag, soptsf \
           rsstepid, rsname, rsapplicationname, rsapplicationversion, rsoptionfiles, rsdddb, rsconddb, rsextrapackages, rsvisible, rsprocessingpass, rsusable, rdqtag, roptsf from \
    ( select ROWNUM r , sstepid, sname, sapplicationname, sapplicationversion, soptionfiles, sdddb, sconddb, sextrapackages, svisible, sprocessingpass, susable, sdqtag, soptsf, \
       rsstepid, rsname, rsapplicationname, rsapplicationversion, rsoptionfiles, rsdddb, rsconddb, rsextrapackages, rsvisible, rsprocessingpass, rsusable , rdqtag, roptsf from \
      ( select ROWNUM r, s.stepid sstepid ,s.stepname sname, s.applicationname sapplicationname,s.applicationversion sapplicationversion, s.optionfiles soptionfiles,\
      s.DDDB sdddb,s.CONDDB sconddb, s.extrapackages sextrapackages,s.Visible svisible , s.ProcessingPass sprocessingpass, s.Usable susable, s.dqtag sdqtag, s.optionsFormat soptsf, \
      r.stepid rsstepid ,r.stepname rsname, r.applicationname rsapplicationname, r.applicationversion rsapplicationversion, r.optionfiles rsoptionfiles,\
      r.DDDB rsdddb,r.CONDDB rsconddb, r.extrapackages rsextrapackages,r.Visible rsvisible , r.ProcessingPass rsprocessingpass, r.Usable rsusable, r.dqtag rdqtag, r.optionsFormat roptsf \
      from %s where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid %s \
       ) where rownum <=%d ) where r >%d" % (tables, condition, max, start)
        else:
          command = 'select s.stepid,s.stepname, s.applicationname,s.applicationversion,s.optionfiles,s.DDDB,s.CONDDB, s.extrapackages,s.Visible, s.ProcessingPass, s.Usable, s.dqtag, s.optionsformat, \
          r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat \
          from %s where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid  %s ' % (tables, condition)
        retVal = self.dbR_._query(command)
    else:
        command = 'select s.stepid, s.stepname, s.applicationname,s.applicationversion,s.optionfiles,s.DDDB,s.CONDDB, s.extrapackages,s.Visible, s.ProcessingPass, s.Usable, s.dqtag, s.optionsformat,\
        r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat \
        from %s where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid ' % (tables)
        retVal = self.dbR_._query(command)

    if retVal['OK']:
      parameters = ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion', 'OptionFiles', 'DDDB', 'CONDDB', 'ExtraPackages', 'Visible', 'ProcessingPass', 'Usable', 'DQTag', 'OptionsFormat', 'RuntimeProjects']
      rParameters = ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion', 'OptionFiles', 'DDDB', 'CONDDB', 'ExtraPackages', 'Visible', 'ProcessingPass', 'Usable', 'DQTag', 'OptionsFormat']
      records = []
      for record in retVal['Value']:
        step = list(record[0:13])
        runtimeProject = []
        runtimeProject = [ rec for rec in list(record[13:]) if rec != None]
        if len(runtimeProject) > 0: runtimeProject = [runtimeProject]
        step += [{'ParameterNames':rParameters, 'Records':runtimeProject, 'TotalRecords':len(runtimeProject) + 1}]
        records += [step]
      if paging:
        command = "select count(*) from steps s where s.stepid>0 %s " % (condition)
        retVal = self.dbR_._query(command)
        if retVal['OK']:
          totrec = retVal['Value'][0][0]
          result = S_OK({'ParameterNames':parameters, 'Records':records, 'TotalRecords':totrec})
        else:
          result = retVal
      else:
        result = S_OK({'ParameterNames':parameters, 'Records':records, 'TotalRecords':len(records)})
    else:
      result = S_ERROR(retVal['Message'])
    return result

  #############################################################################
  def getRuntimeProjects(self, dict):
    result = S_ERROR()
    condition = ''
    selection = 's.stepid,stepname, s.applicationname,s.applicationversion,s.optionfiles,s.DDDB,CONDDB, s.extrapackages,s.Visible, s.ProcessingPass, s.Usable'
    tables = 'steps s, runtimeprojects rp'
    stepId = dict.get('StepId', default)
    if stepId != default:
      condition += " rp.stepid=%d" % (stepId)
      command = " select %s from %s where s.stepid=rp.runtimeprojectid and %s" % (selection, tables, condition)
      retVal = self.dbR_._query(command)
      if retVal['OK']:
        parameters = ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion', 'OptionFiles', 'DDDB', 'CONDDB', 'ExtraPackages', 'Visible', 'ProcessingPass', 'Usable', 'DQTag', 'OptionsFormat']
        records = []
        for record in retVal['Value']:
          records += [list(record)]
        result = S_OK({'ParameterNames':parameters, 'Records':records, 'TotalRecords':len(records)})
      else:
        result = retVal
    else:
      result = S_ERROR('You must provide a StepId!')
    return result


  #############################################################################
  def getStepInputFiles(self, StepId):
    command = 'select inputFiletypes.name,inputFiletypes.visible from steps, table(steps.InputFileTypes) inputFiletypes where  steps.stepid=' + str(StepId)
    return self.dbR_._query(command)

  #############################################################################
  def setStepInputFiles(self, stepid, files):
    files = sorted(files, key=lambda k: k['FileType'])
    if len(files) == 0:
      values = 'null'
    else:
      values = 'filetypesARRAY('
      for i in files:
        f = i.get('FileType', default)
        v = i.get('Visible', default)
        if f != default and v != default:
          values += "ftype('%s','%s')," % (f, v)
      values = values[:-1]
      values += ')'
    command = "update steps set inputfiletypes=%s where stepid=%s" % (values, str(stepid))
    return self.dbW_._query(command)

  #############################################################################
  def setStepOutputFiles(self, stepid, files):
    files = sorted(files, key=lambda k: k['FileType'])
    if len(files) == 0:
      values = 'null'
    else:
      values = 'filetypesARRAY('
      for i in files:
        f = i.get('FileType', default)
        v = i.get('Visible', default)
        if f != default and v != default:
          values += "ftype('%s','%s')," % (f, v)
      values = values[:-1]
      values += ')'
    command = "update steps set Outputfiletypes=%s  where stepid=%s" % (values, str(stepid))
    return self.dbW_._query(command)

  #############################################################################
  def getStepOutputFiles(self, StepId):
    command = 'select outputfiletypes.name,outputfiletypes.visible from steps, table(steps.outputfiletypes) outputfiletypes where  steps.stepid=' + str(StepId)
    return self.dbR_._query(command)

  #############################################################################
  def getProductionOutputFileTypes(self, prod):
    command = "select o.name,o.visible from steps s, table(s.outputfiletypes) o, stepscontainer st \
            where st.stepid=s.stepid and st.production=%d" % (int(prod))
    retVal = self.dbR_._query(command)
    values = {}
    if retVal['OK']:
      for i in retVal['Value']:
        values[i[0]] = i[1]
      result = S_OK(values)
    else:
      result = retVal
    return result

  #############################################################################
  def getAvailableFileTypes(self):
    return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getAvailableFileTypes', [])

  #############################################################################
  def insertFileTypes(self, ftype, desc, fileType):
    return self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.insertFileTypes', LongType, [ftype, desc, fileType])

  #############################################################################
  def insertStep(self, dict):
    ### Dictionary format: {'Step': {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': '', 'ApplicationVersion': 'v29r1', 'ext-comp-1273': 'CHARM.MDST (Charm micro dst)', 'ExtraPackages': '', 'StepName': 'davinci prb2', 'ProcessingPass': 'WG-Coool', 'ext-comp-1264': 'CHARM.DST (Charm stream)', 'Visible': 'Y', 'DDDB': '', 'OptionFiles': '', 'CONDDB': ''}, 'OutputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.MDST'}], 'InputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.DST'}],'RuntimeProjects':[{StepId:13878}]}
    result = S_ERROR()
    values = ''
    command = "SELECT applications_index_seq.nextval from dual"
    sid = 0
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      result = retVal
    else:
      sid = retVal['Value'][0][0]

    selection = 'insert into steps(stepid,stepname,applicationname,applicationversion,OptionFiles,dddb,conddb,extrapackages,visible, processingpass, usable, DQTag, optionsformat '
    inFileTypes = dict.get('InputFileTypes', default)
    if inFileTypes != default:
      inFileTypes = sorted(inFileTypes, key=lambda k: k['FileType'])
      values = ',filetypesARRAY('
      selection += ',InputFileTypes'
      for i in inFileTypes:
        values += "ftype(' %s', '%s')," % (i.get('FileType', None), i.get('Visible', None))
      values = values[:-1]
      values += ')'

    outFileTypes = dict.get('OutputFileTypes', default)
    if outFileTypes != default:
      outFileTypes = sorted(outFileTypes, key=lambda k: k['FileType'])
      values += ' , filetypesARRAY('
      selection += ',OutputFileTypes'
      for i in outFileTypes:
        values += "ftype(' %s', '%s')," % (i.get('FileType', None), i.get('Visible', None))
      values = values[:-1]
      values += ')'

    step = dict.get('Step', default)
    if step != default:
      command = selection + ")values(%d" % (sid)
      command += ",'%s'" % (step.get('StepName', 'NULL'))
      command += ",'%s'" % (step.get('ApplicationName', 'NULL'))
      command += ",'%s'" % (step.get('ApplicationVersion', 'NULL'))
      command += ",'%s'" % (step.get('OptionFiles', 'NULL'))
      command += ",'%s'" % (step.get('DDDB', 'NULL'))
      command += ",'%s'" % (step.get('CONDDB', 'NULL'))
      command += ",'%s'" % (step.get('ExtraPackages', 'NULL'))
      command += ",'%s'" % (step.get('Visible', 'NULL'))
      command += ",'%s'" % (step.get('ProcessingPass', 'NULL'))
      command += ",'%s'" % (step.get('Usable', 'Not ready'))
      command += ",'%s'" % (step.get('DQTag', ''))
      command += ",'%s'" % (step.get('OptionsFormat', ''))
      command += values + ")"
      retVal = self.dbW_._query(command)
      if retVal['OK']:
        r_project = dict.get('RuntimeProjects', step.get('RuntimeProjects', default))
        if r_project != default:
          for i in r_project:
            rid = i['StepId']
            retVal = self.insertRuntimeProject(sid, rid)
            if not retVal['OK']:
              result = retVal
            else:
              result = S_OK(sid)
        else:
          result = S_OK(sid)
      else:
        result = retVal
    else:
      result = S_ERROR('The Step is not provided!')
    return result

  #############################################################################
  def deleteStep(self, stepid):
    result = S_ERROR()
    command = " delete runtimeprojects where stepid=%d" % (stepid)
    retVal = self.dbW_._query(command)
    if not retVal['OK']:
      result = retVal
    else:
      # now we can delete the step
      command = "delete steps where stepid=%d" % (stepid)
      result = self.dbW_._query(command)
    return result

  #############################################################################
  def deleteSetpContiner(self, prod):
    result = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.deleteSetpContiner', [prod], False)
    return result

  #############################################################################
  def deleteProductionsContiner(self, prod):
    result = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.deleteProductionsCont', [prod], False)
    return result

  #############################################################################
  def updateStep(self, dict):
    #input data {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': '13860', 'ApplicationVersion': 'v29r1', 'ExtraPackages': '', 'StepName': 'davinci prb3', 'ProcessingPass': 'WG-Coool-new', 'InputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.DST'}], 'Visible': 'Y', 'DDDB': '', 'OptionFiles': '', 'CONDDB': '', 'OutputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.MDST'}], 'RuntimeProjects':[{'StepId':13879}]}
    result = S_ERROR()
    ok = True
    rProjects = dict.get('RuntimeProjects', default)
    if rProjects != default:
      if len(rProjects) > 0:
        for i in rProjects:
          if 'StepId' not in dict:
            result = S_ERROR('The runtime project can not changed, because the StepId is missing!')
            ok = False
          else:
            retVal = self.updateRuntimeProject(dict['StepId'], i['StepId'])
            if not retVal['OK']:
              result = retVal
              ok = False
            else:
              dict.pop('RuntimeProjects')
      else:
        retVal = self.removeRuntimeProject(dict['StepId'])
        if not retVal['OK']:
          result = retVal
          ok = False
        else:
          dict.pop('RuntimeProjects')

    if ok:
      stepid = dict.get('StepId', default)
      if stepid != default:
        condition = " where stepid=%s" % (str(stepid))
        command = 'update steps set '
        for i in dict:
          if type(dict[i]) == types.StringType:
            command += " %s='%s'," % (i, str(dict[i]))
          else:
            if len(dict[i]) > 0:
              values = 'filetypesARRAY('
              ftypes = dict[i]
              ftypes = sorted(ftypes, key=lambda k: k['FileType'])
              for j in ftypes:
                f = j.get('FileType', default)
                v = j.get('Visible', default)
                if f != default and v != default:
                  values += "ftype('%s','%s')," % (f, v)
              values = values[:-1]
              values += ')'
              command += i + '=' + values + ','
            else:
              command += i + '=null,'
        command = command[:-1]
        command += condition
        result = self.dbW_._query(command)
      else:
        result = S_ERROR('Please provide a StepId!')

    return result

  #############################################################################
  def getAvailableConfigNames(self):
    command = ' select distinct Configname from prodview'
    return self.dbR_._query(command)

  ##############################################################################
  def getAvailableConfigurations(self):
    return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getAvailableConfigurations', [])

  #############################################################################
  def getConfigVersions(self, configname):
    result = S_ERROR()
    if configname != default:
      command = " select distinct configversion from prodview where configname='%s'" % (configname)
      result = self.dbR_._query(command)
    else:
      result = S_ERROR('You must provide a Configuration Name!')
    return result

  #############################################################################
  def getConditions(self, configName, configVersion, evt):

    condition = ''
    if configName != default:
      condition += " and prodview.configname='%s' " % (configName)

    if configVersion != default:
      condition += " and prodview.configversion='%s' " % (configVersion)


    if evt != default:
      condition += " and prodview.eventtypeid=%s" % (str(evt))

    command = 'select distinct simulationConditions.SIMID,data_taking_conditions.DAQPERIODID,simulationConditions.SIMDESCRIPTION, simulationConditions.BEAMCOND, \
    simulationConditions.BEAMENERGY, simulationConditions.GENERATOR,simulationConditions.MAGNETICFIELD,simulationConditions.DETECTORCOND, simulationConditions.LUMINOSITY, simulationconditions.G4settings, \
    data_taking_conditions.DESCRIPTION,data_taking_conditions.BEAMCOND, data_taking_conditions.BEAMENERGY,data_taking_conditions.MAGNETICFIELD, \
    data_taking_conditions.VELO,data_taking_conditions.IT, data_taking_conditions.TT,data_taking_conditions.OT,data_taking_conditions.RICH1,data_taking_conditions.RICH2, \
    data_taking_conditions.SPD_PRS, data_taking_conditions.ECAL, data_taking_conditions.HCAL, data_taking_conditions.MUON, data_taking_conditions.L0, data_taking_conditions.HLT,\
     data_taking_conditions.VeloPosition from simulationConditions,data_taking_conditions,prodview where \
      prodview.simid=simulationConditions.simid(+) and \
      prodview.DAQPERIODID=data_taking_conditions.DAQPERIODID(+)' + condition

    return self.dbR_._query(command)

  #############################################################################
  def getProcessingPass(self, configName, configVersion, conddescription, runnumber, production, eventType=default, path='/'):

    erecords = []
    eparameters = []
    precords = []
    pparameters = []

    condition = ''
    if configName != default:
      condition += " and prodview.configname='%s' " % (configName)

    if configVersion != default:
      condition += " and prodview.configversion='%s' " % (configVersion)

    if conddescription != default:
      retVal = self._getConditionString(conddescription)
      if not retVal['OK']:
        return  retVal
      else:
        condition += retVal['Value']

    if production != default:
      condition += ' and prodview.production=' + str(production)
    tables = ''
    if runnumber != default:
      tables += ' , prodrunview '
      condition += ' and prodrunview.production=prodview.production and prodrunview.runnumber=%s' % (str(runnumber))

    if eventType != default:
      condition += " and prodview.eventtypeid=%s" % (str(eventType))

    proc = path.split('/')[len(path.split('/')) - 1]
    if proc != '':
      command = "select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='%s'" % (path.split('/')[1], path)
      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        return retVal
      pro = ''
      for i in retVal['Value']:
        pro += "%s," % (str(i[0]))
      pro = pro[:-1]

      if pro == '':
        return S_ERROR('Empty Directory')
      command = 'select distinct eventTypes.EventTypeId, eventTypes.Description from eventtypes,prodview,productionscontainer,processing %s where \
        prodview.production=productionscontainer.production and \
        eventTypes.EventTypeId=prodview.eventtypeid and \
        productionscontainer.processingid=processing.id and \
        processing.id in (%s) %s' % (tables, pro, condition)

      retVal = self.dbR_._query(command)
      if retVal['OK']:
        eparameters = ['EventTypeId', 'Description']
        for record in retVal['Value']:
          erecords += [list(record)]
      else:
        return retVal

      command = "SELECT distinct name \
      FROM processing   where parentid in (select id from  processing where name='%s') START WITH id in (select distinct productionscontainer.processingid from productionscontainer,prodview %s where \
      productionscontainer.production=prodview.production  %s )  CONNECT BY NOCYCLE PRIOR  parentid=id order by name desc" % (str(proc), tables, condition)
    else:
      command = 'SELECT distinct name \
      FROM processing  where parentid is null START WITH id in (select distinct productionscontainer.processingid from productionscontainer, prodview %s where \
      productionscontainer.production=prodview.production %s ) CONNECT BY NOCYCLE PRIOR  parentid=id order by name desc' % (tables, condition)
    retVal = self.dbR_._query(command)
    if retVal['OK']:
      precords = []
      pparameters = ['Name']
      for record in retVal['Value']:
        precords += [[record[0]]]
    else:
      return retVal

    return S_OK([{'ParameterNames':pparameters, 'Records':precords, 'TotalRecords':len(precords)}, {'ParameterNames':eparameters, 'Records':erecords, 'TotalRecords':len(erecords)}])

  #############################################################################
  def _getConditionString(self, conddescription, table='productionscontainer'):
    condition = ''
    retVal = self._getDataTakingConditionId(conddescription)
    if retVal['OK']:
      if retVal['Value'] != -1:
        condition += " and %s.DAQPERIODID=%s and %s.DAQPERIODID is not null " % (table, str(retVal['Value']), table)
      else:
        retVal = self._getSimulationConditioId(conddescription)
        if retVal['OK']:
          if retVal['Value'] != -1:
            condition += " and %s.simid=%s and %s.simid is not null " % (table, str(retVal['Value']), table)
          else:
            return S_ERROR('Condition does not exists!')
        else:
          return retVal
    else:
      return retVal
    return S_OK(condition)

  #############################################################################
  def _getDataTakingConditionId(self, desc):
    command = 'select DAQPERIODID from data_taking_conditions where DESCRIPTION=\'' + str(desc) + '\''
    retVal = self.dbR_._query(command)
    if retVal['OK']:
      if len(retVal['Value']) > 0:
        return S_OK(retVal['Value'][0][0])
      else:
        return S_OK(-1)
    else:
      return retVal

  #############################################################################
  def _getSimulationConditioId(self, desc):
    command = 'select simid from simulationconditions where simdescription=\'' + str(desc) + '\''
    retVal = self.dbR_._query(command)
    if retVal['OK']:
      if len(retVal['Value']) > 0:
        return S_OK(retVal['Value'][0][0])
      else:
        return S_OK(-1)
    else:
      return retVal

  #############################################################################
  def getProductions(self, configName=default, configVersion=default, conddescription=default, processing=default, evt=default):

    condition = ''
    if configName != default:
      condition += " and bview.configname='%s'" % (configName)
    if configVersion != default:
      condition += " and bview.configversion='%s'" % (configVersion)

    if conddescription != default:
      retVal = self._getConditionString(conddescription, 'pcont')
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal

    if evt != default:
      condition += ' and bview.eventtypeid=' + str(evt)

    if processing != default:
      command = "select distinct pcont.production from \
                 productionscontainer pcont,prodview bview \
                 where pcont.processingid in \
                    (select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='" + str(processing.split('/')[1]) + "') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='" + processing + "') \
                  and bview.production=pcont.production " + condition
    else:
      command = "select distinct pcont.production from productionscontainer pcont,prodview bview where \
                 bview.production=pcont.production " + condition
    return self.dbR_._query(command)

  #############################################################################
  def getFileTypes(self, configName, configVersion, conddescription=default, processing=default, evt=default, runnb=default, production=default):

    condition = ''

    if configName != default:
      condition += " and bview.configname='%s' " % (configName)

    if configVersion != default:
      condition += " and bview.configversion='%s' " % (configVersion)

    if conddescription != default:
      retVal = self._getConditionString(conddescription, 'pcont')
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal

    if evt != default:
      condition += ' and bview.eventtypeid=' + str(evt)

    if production != default and type(production) == types.StringType:
      condition += ' and bview.production=' + str(production)
    elif production != default and type(production) == types.ListType:
      cond = ' and ('
      for i in production:
        cond += ' bview.production=%d or ' % (i)
      condition += cond[:-3] + ') '

    tables = ''
    if runnb != default:
      tables = ' , prodrunview prview'
      condition += ' and prview.production=bview.production and prview.runnumber=' + str(runnb)


    if processing != default:
      command = "select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='%s'" % (processing.split('/')[1], processing)
      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        return retVal
      pro = '('
      for i in retVal['Value']:
        pro += "%s," % (str(i[0]))
      pro = pro[:-1]
      pro += (')')
      command = "select distinct ftypes.name from \
                 productionscontainer pcont,prodview bview, filetypes ftypes  %s \
                 where pcont.processingid in %s \
                  and bview.production=pcont.production and bview.filetypeId=ftypes.filetypeid  %s" % (tables, pro, condition)
    else:
      command = "select distinct ftypes.name  from productionscontainer pcont, prodview bview,  filetypes ftypes %s where \
                 bview.production=pcont.production and bview.filetypeId=ftypes.filetypeid %s" % (tables, condition)
    return self.dbR_._query(command)

  #############################################################################
  def getFilesWithMetadata(self, configName, configVersion, conddescription=default, processing=default, evt=default, production=default, filetype=default, quality=default, runnb=default):
    condition = ''
    if configName != default:
      condition += " and c.configname='%s' " % (configName)

    if configVersion != default:
      condition += " and c.configversion='%s' " % (configVersion)

    if conddescription != default:
      retVal = self._getConditionString(conddescription, 'prod')
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal

    tables = ''
    if evt != default:
      tables += ' ,prodview bview'
      condition += '  and j.production=bview.production and bview.production=prod.production and bview.eventtypeid=%s and f.eventtypeid=bview.eventtypeid ' % (evt)

    if production != default:
      condition += ' and j.production=' + str(production)

    if runnb != default:
      condition += ' and j.runnumber=' + str(runnb)

    if filetype != default:
      condition += " and ftypes.name='%s' and bview.filetypeid=ftypes.filetypeid " % (str(filetype))

    if quality != default:
      if type(quality) == types.StringType:
        command = "select QualityId from dataquality where dataqualityflag='" + str(quality) + "'"
        res = self.dbR_._query(command)
        if not res['OK']:
          gLogger.error('Data quality problem:', res['Message'])
        elif len(res['Value']) == 0:
            return S_ERROR('Dataquality is missing!')
        else:
          quality = res['Value'][0][0]
        condition += ' and f.qualityid=' + str(quality)
      else:
        if len(quality) > 0:
          conds = ' ('
          for i in quality:
            quality = None
            command = "select QualityId from dataquality where dataqualityflag='" + str(i) + "'"
            res = self.dbR_._query(command)
            if not res['OK']:
              gLogger.error('Data quality problem:', res['Message'])
            elif len(res['Value']) == 0:
                return S_ERROR('Dataquality is missing!')
            else:
              quality = res['Value'][0][0]
            conds += ' f.qualityid=' + str(quality) + ' or'
          condition += 'and' + conds[:-3] + ')'

    if processing != default:
      command = "select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='%s'" % (processing.split('/')[1], processing)
      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        return retVal
      pro = '('
      for i in retVal['Value']:
        pro += "%s," % (str(i[0]))
      pro = pro[:-1]
      pro += (')')

      condition += " and prod.processingid in %s" % (pro)

    command = "select distinct f.FileName, f.EventStat, f.FileSize, f.CreationDate, j.JobStart, j.JobEnd, j.WorkerNode, ftypes.Name, j.runnumber, j.fillnumber, f.fullstat, d.dataqualityflag, \
    j.eventinputstat, j.totalluminosity, f.luminosity, f.instLuminosity, j.tck from files f, jobs j, productionscontainer prod, configurations c, dataquality d, filetypes ftypes %s  where \
    j.jobid=f.jobid and \
    d.qualityid=f.qualityid and \
    f.gotreplica='Yes' and \
    f.visibilityFlag='Y' and \
    ftypes.filetypeid=f.filetypeid and \
    j.configurationid=c.configurationid %s" % (tables, condition)
    return self.dbR_._query(command)

  #############################################################################
  def getAvailableDataQuality(self):
    result = S_ERROR()
    command = ' select dataqualityflag from dataquality'
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      result = retVal
    else:
      flags = retVal['Value']
      values = []
      for i in flags:
        values += [i[0]]
      result = S_OK(values)
    return result

  #############################################################################
  def getAvailableProductions(self):
    command = ' select distinct production from prodview where production > 0'
    res = self.dbR_._query(command)
    return res

  #############################################################################
  def getAvailableRuns(self):
    command = ' select distinct runnumber from prodrunview'
    res = self.dbR_._query(command)
    return res

  #############################################################################
  def getAvailableEventTypes(self):
    return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getAvailableEventTypes', [])

  #############################################################################
  def getProductionProcessingPass(self, prodid):
    return self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.getProductionProcessingPass', StringType, [prodid])

  #############################################################################
  def getRunProcessingPass(self, runnumber):
    return self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.getProductionProcessingPass', StringType, [-1 * runnumber])

  #############################################################################
  def getProductionProcessingPassID(self, prodid):
    return self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.getProductionProcessingPassId', LongType, [prodid])

  #############################################################################
  def getMoreProductionInformations(self, prodid):
    command = 'select prodview.configname, prodview.configversion, prodview.ProgramName, prodview.programversion from \
    prodview  where prodview.production=' + str(prodid)
    res = self.dbR_._query(command)
    if not res['OK']:
      return S_ERROR(res['Message'])
    else:
      record = res['Value']
      cname = record[0][0]
      cversion = record[0][1]
      pname = record[0][2]
      pversion = record[0][3]


    retVal = self.getProductionProcessingPass(prodid)
    if retVal['OK']:
      procdescription = retVal['Value']
    else:
      return retVal

    simdesc = None
    daqdesc = None

    command = "select distinct sim.simdescription, daq.description from simulationconditions sim, data_taking_conditions daq,productionscontainer prod \
              where sim.simid(+)=prod.simid and daq.daqperiodid(+)=prod.daqperiodid and prod.production=" + str(prodid)
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return retVal
    else:
      value = retVal['Value']
      if len(value) != 0:
        simdesc = value[0][0]
        daqdesc = value[0][1]
      else:
        return S_ERROR('Simulation condition or data taking condition not exist!')
    if simdesc != None:
      return S_OK({'ConfigName':cname, 'ConfigVersion':cversion, 'ProgramName':pname, 'ProgramVersion':pversion, 'Processing pass':procdescription, 'Simulation conditions':simdesc})
    else:
      return S_OK({'ConfigName':cname, 'ConfigVersion':cversion, 'ProgramName':pname, 'ProgramVersion':pversion, 'Processing pass':procdescription, 'Data taking conditions':daqdesc})


  #############################################################################
  def getJobInfo(self, lfn):
    return self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getJobInfo', [lfn])

  #############################################################################
  def getRunNumber(self, lfn):
    return self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.getRunNumber', LongType, [lfn])

  #############################################################################
  def getRunNbAndTck(self, lfn):
    return self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getRunNbAndTck', [lfn])

  #############################################################################
  def getProductionFiles(self, prod, ftype, gotreplica=default):

    result = S_ERROR()
    value = {}
    condition = ''
    if gotreplica != default:
      condition += " and files.gotreplica='%s'" % (str(gotreplica))

    if ftype != default:
      condition += " and filetypes.name='%s'" % (ftype)

    command = "select files.filename, files.gotreplica, files.filesize,files.guid, filetypes.name, files.inserttimestamp from jobs,files,filetypes where\
    jobs.jobid=files.jobid and files.filetypeid=filetypes.filetypeid and jobs.production=%d %s" % (prod, condition)

    res = self.dbR_._query(command)
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        value[record[0]] = {'GotReplica':record[1], 'FileSize':record[2], 'GUID':record[3], 'FileType':record[4]}
      result = S_OK(value)
    else:
      result = S_ERROR(res['Message'])
    return result

  #############################################################################
  def getRunFiles(self, runid):

    result = S_ERROR()
    value = {}
    res = self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getRunFiles', [runid])
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        value[record[0]] = {'GotReplica':record[1], 'FileSize':record[2], 'GUID':record[3]}
      result = S_OK(value)
    else:
      result = res
    return result

  #############################################################################
  def updateFileMetaData(self, filename, fileAttr):
    utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    command = "update files Set inserttimestamp=TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS') ," % (str(utctime))
    for attribute in fileAttr:
      if type(fileAttr[attribute]) == types.StringType:
        command += "%s='%s'," % (attribute, str(fileAttr[attribute]))
      else:
        command += "%s=%s," % (str(attribute), str(fileAttr[attribute]))

    command = command[:-1]
    command += " where filaName='%s'" % (filename)
    res = self.dbW_._query(command)
    return res

  #############################################################################
  def renameFile(self, oldLFN, newLFN):
    utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    command = " update files set inserttimestamp=TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS'), filename ='%s' where filename='%s'" % (str(utctime), newLFN, oldLFN)
    res = self.dbW_._query(command)
    return res

  #############################################################################
  def getJobInputAndOutputJobFiles(self, jobids):
    result = S_ERROR()
    jobs = {}
    for jobid in jobids:
      tmp = {}
      res = self.getInputFiles(jobid)
      if not res['OK']:
        result = res
        break
      else:
        input = res['Value']
        inputs = []
        for lfn in input:
          inputs += [lfn]
        res = self.getOutputFiles(jobid)
        if not res['OK']:
          result = res
          break
        else:
          output = res['Value']
          outputs = []
          for lfn in output:
            if lfn not in inputs:
              outputs += [lfn]
          tmp = {'InputFiles':inputs, 'OutputFiles':outputs}
          jobs[jobid] = tmp
    return S_OK(result)

  #############################################################################
  def getInputFiles(self, jobid):
    command = ' select files.filename from inputfiles,files where files.fileid=inputfiles.fileid and inputfiles.jobid=' + str(jobid)
    res = self.dbR_._query(command)
    return res

  #############################################################################
  def getOutputFiles(self, jobid):
    command = ' select files.filename from files where files.jobid =' + str(jobid)
    res = self.dbR_._query(command)
    return res

  #############################################################################
  def insertTag(self, name, tag):
    return self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.insertTag', [name, tag], False)

  #############################################################################
  def existsTag(self, name, value): #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    result = False
    command = "select count(*) from tags where name='%s' and tag='%s'" % (str(name), str(value))
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      result = retVal
    elif retVal['Value'][0][0] > 0:
      result = True
    return S_OK(result)

  #############################################################################
  def setFileDataQuality(self, lfns, flag):
    result = S_ERROR()
    values = {}
    retVal = self._getDataQualityId(flag)
    if not retVal['OK']:
      result = retVal
    else:
      qid = retVal['Value']
      failed = []
      succ = []
      for lfn in lfns:
        retVal = self.__updateQualityFlag(lfn, qid)
        if not retVal['OK']:
          failed += [lfn]
          gLogger.error(retVal['Message'])
        else:
          succ += [lfn]
      values['Successful'] = succ
      values['Failed'] = failed
      result = S_OK(values)
    return result

  #############################################################################
  def  _getProcessingPassId(self, root, fullpath):
    return  self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.getProcessingPassId', LongType, [root, fullpath])

  #############################################################################
  def getProcessingPassId(self, fullpath):
    return self._getProcessingPassId(fullpath.split('/')[1:][0], fullpath)

  #############################################################################
  def _getDataQualityId(self, name):
    return  self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.getDataQualityId', LongType, [name])

  #############################################################################
  def setRunAndProcessingPassDataQuality(self, runNB, procpass, flag):
    result = S_ERROR()
    retVal = self._getProcessingPassId(procpass.split('/')[1:][0], procpass)
    if retVal['OK']:
      processingid = retVal['Value']
      retVal = self._getDataQualityId(flag)
      if retVal['OK']:
        flag = retVal['Value']
        result = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.insertRunquality', [runNB, flag, processingid], False)
      else:
        result = retVal
    else:
      result = retVal
    return result

  #############################################################################
  def setRunDataQuality(self, runNb, flag):
    result = S_ERROR()
    command = 'select distinct j.runnumber from  jobs j, productionscontainer prod where \
    j.production=prod.production and \
    j.production<0 and \
    j.runnumber=%s' % (str(runNb))
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      result = retVal
    else:

      if len(retVal['Value']) == 0:
        result = S_ERROR('This ' + str(runNb) + ' run is missing in the BKK DB!')
      else:
        retVal = self._getDataQualityId(flag)

        if not retVal['OK']:
          result = retVal
        else:
          qid = retVal['Value']
          utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
          command = " update files set inserttimestamp=TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS'), qualityId=%d where fileid in ( select files.fileid from jobs, files where jobs.jobid=files.jobid and \
            jobs.runnumber=%d)" % (str(utctime), qid, runNb)
          retVal = self.dbW_._query(command)

          if not retVal['OK']:
            result = retVal
          else:
            command = 'select files.filename from jobs, files where jobs.jobid=files.jobid and \
              jobs.runnumber=%s' % (runNb)

            retVal = self.dbR_._query(command)

            if not retVal['OK']:
              result = retVal
            else:
              succ = []
              records = retVal['Value']
              for record in records:
                succ += [record[0]]
              values = {}
              values['Successful'] = succ
              values['Failed'] = []
              result = S_OK(values)
    return result

  #############################################################################
  def setProductionDataQuality(self, prod, flag):
    result = S_ERROR()
    command = "select distinct jobs.production  from jobs where jobs.production=%d" % (prod)
    retVal = self.dbR_._query(command)

    if not retVal['OK']:
      result = retVal
    else:

      if len(retVal['Value']) == 0:
        result = S_ERROR('This ' + str(prod) + ' production is missing in the BKK DB!')
      else:
        retVal = self._getDataQualityId(flag)

        if not retVal['OK']:
          result = retVal
        else:
          qid = retVal['Value']
          utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
          command = " update files set inserttimestamp=TO_TIMESTAMP('%s' + str(utctime),'YYYY-MM-DD HH24:MI:SS'), qualityId=%d where fileid in ( select files.fileid from jobs, files where jobs.jobid=files.jobid and \
            jobs.production=%d)" % (str(utctime), qid, prod)
          retVal = self.dbW_._query(command)

          if not retVal['OK']:
            result = retVal
          else:
            command = "select files.filename from jobs, files where jobs.jobid=files.jobid and \
              jobs.production=%d" % (prod)
            retVal = self.dbR_._query(command)

            if not retVal['OK']:
              result = retVal
            else:
              succ = []
              records = retVal['Value']
              for record in records:
                succ += [record[0]]
              values = {}
              values['Successful'] = succ
              values['Failed'] = []
              result = S_OK(values)
    return result

  #############################################################################
  def __updateQualityFlag(self, lfn, qid):
    result = S_ERROR()
    utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    command = "update files set inserttimestamp=TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS), qualityid=%d where filename='%s'" %(str(utctime), qid, lfn)
    retVal = self.dbW_._query(command)
    if not retVal['OK']:
      result = retVal
    else:
      result =  S_OK('Quality flag has been updated!')
    return result

  #############################################################################
  def getFileAncestorHelper(self, fileName, files, depth, counter, replica):
    failed = []
    if counter < depth:
      counter += 1
      result = self.dbR_.executeStoredFunctions('BKK_MONITORING.getJobIdWithoutReplicaCheck', LongType, [fileName])

      if not result["OK"]:
        gLogger.error('Ancestor', result['Message'])
      else:
        job_id = int(result['Value'])
      if job_id != 0:
        command = "select files.fileName,files.jobid, files.gotreplica, files.eventstat, files.eventtypeid, files.luminosity, files.instLuminosity, filetypes.name \
        from inputfiles,files, filetypes where files.filetypeid=filetypes.filetypeid  and inputfiles.fileid=files.fileid and inputfiles.jobid=%d" % (job_id)
        res = self.dbR_._query(command)
        if not res['OK']:
          gLogger.error('Ancestor', result["Message"])
        else:
          dbResult = res['Value']
          for record in dbResult:
            if replica and (record[2] == 'No'):
              self.getFileAncestorHelper(record[0], files, depth, counter-1, replica)
              continue
            files += [{'FileName':record[0], 'GotReplica':record[2], 'EventStat':record[3], 'EventType':record[4], 'Luminosity':record[5], 'InstLuminosity':record[6], 'FileType':record[7]}]
            self.getFileAncestorHelper(record[0], files, depth, counter, replica)
      else:
        failed += [fileName]
    return failed

  #############################################################################
  def getFileAncestors(self, lfn, depth=0, replica=True):
    odepth = -1
    if depth > 10:
      depth = 10
    elif depth < 1:
      depth = 1
      odepth = depth
    else:
      odepth = depth

    logicalFileNames = {}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    gLogger.debug('original', lfn)
    for fileName in lfn:
      depth = odepth
      files = []
      failed = self.getFileAncestorHelper(fileName, files, depth, 0, replica)
      logicalFileNames['Failed'] = failed
      if len(files) > 0:
        ancestorList[fileName] = files
    logicalFileNames['Successful'] = ancestorList
    return S_OK(logicalFileNames)

  #############################################################################
  def getFileDescendentsHelper(self, fileName, files, depth, production, counter, checkreplica):

    failed = []
    notprocessed = []
    if counter < depth:
      counter += 1
      res = self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.getFileID', LongType, [fileName])
      if not res["OK"]:
        gLogger.error('Ancestor', res['Message'])
      elif res['Value'] == None:
        failed += [fileName]
      else:
        file_id = res['Value']
        if file_id != 0:
          res = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getJobIdFromInputFiles', [file_id])
          if not res["OK"]:
            gLogger.error('Ancestor', res['Message'])
            if not fileName in failed:
              failed += [fileName]
          elif  len(res['Value']) != 0:
            job_ids = res['Value']
            for i in job_ids:
              job_id = i[0]
              getProd = False
              if production:
                getProd = True

              res = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getFileAndJobMetadata', [job_id, getProd])
              if not res["OK"]:
                gLogger.error('Ancestor', res['Message'])
                if not fileName in failed:
                  failed += [fileName]
              elif len(res['Value']) == 0:
                notprocessed += [fileName]
              else:
                for record in res['Value']:
                  if checkreplica and (record[2] == 'No'):
                    self.getFileDescendentsHelper(record[0], files, depth, production, counter - 1, checkreplica)
                    continue
                  if production and (int(record[3]) != int(production)):
                    self.getFileDescendentsHelper(record[0], files, depth, production, counter - 1, checkreplica)
                    continue
                  files += [record[0]]
                  self.getFileDescendentsHelper(record[0], files, depth, production, counter, checkreplica)
    return failed, notprocessed

  #############################################################################
  def getFileDescendents(self, lfn, depth=0, production=0, checkreplica=True):
    logicalFileNames = {}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    logicalFileNames['NotProcessed'] = []

    odepth = -1
    if depth > 10:
      depth = 10
      odepth = 10
    elif depth < 1:
      depth = 1
      odepth = depth
    else:
      odepth = depth

    for fileName in lfn:
      depth = odepth
      files = []

      failed, notprocessed = self.getFileDescendentsHelper(fileName, files, depth, production, 0, checkreplica)
      logicalFileNames['Failed'] = failed
      logicalFileNames['NotProcessed'] = notprocessed
      if len(files) > 0:
        ancestorList[fileName] = files
    logicalFileNames['Successful'] = ancestorList
    return S_OK(logicalFileNames)

  '''
  #############################################################################
  def getFileDescendents(self, lfn, depth=0, production=0, checkreplica=True):
    logicalFileNames = {}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    logicalFileNames['NotProcessed'] = []
    file_id = -1
    fileids = []
    odepth = -1
    if depth > 10:
      depth = 10
    elif depth < 1:
      depth = 1
      odepth = depth
    else:
      odepth = depth + 1

    gLogger.debug('original', lfn)
    for fileName in lfn:
      depth = odepth
      gLogger.debug('filename', fileName)
      fileids = []
      res = self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.getFileID', LongType, [fileName])
      if not res["OK"]:
        gLogger.error('Ancestor', res['Message'])
      elif res['Value'] == None:
        logicalFileNames['Failed'] += [fileName]
      else:
        file_id = res['Value']
      if file_id != 0:
        fileids += [file_id]
        files = []
        print fileids
        while (depth - 1) and fileids:
          print '!!!', depth, fileids
          for file_id in fileids:
            gLogger.debug('Check the following file:'+str(file_id))
            res = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getJobIdFromInputFiles', [file_id])
            fileids.remove(file_id)
            if not res["OK"]:
              gLogger.error('Ancestor', res['Message'])
              if not fileName in logicalFileNames['Failed']:
                logicalFileNames['Failed'] += [fileName]
            elif  len(res['Value']) != 0:
              job_ids = res['Value']
              for i in job_ids:
                job_id = i[0]
                getProd = False
                if production:
                  getProd = True

                res = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getFileAndJobMetadata', [job_id, getProd])
                if not res["OK"]:
                  gLogger.error('Ancestor', res['Message'])
                  if not fileName in logicalFileNames['Failed']:
                    logicalFileNames['Failed'] += [fileName]
                elif len(res['Value']) == 0:
                  logicalFileNames['NotProcessed'] += [fileName]
                else:
                  dbResult = res['Value']
                  for record in dbResult:
                    fileids += [record[1]]
                    if checkreplica and (record[2] == 'No'):
                      continue
                    if production and (int(record[3]) != int(production)):
                      continue
                    gLogger.debug('It is added to the list:'+str(record[0]))
                    files += [record[0]]
                    depth -= 1
                    print 'cool', depth

          depth -= 1
        ancestorList[fileName] = files
      else:
        logicalFileNames['Failed'] += [fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK(logicalFileNames)
   '''
  """
  data insertation into the database
  """
  #############################################################################
  def checkfile(self, fileName): #file

    result = self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.checkfile', [fileName])
    if result['OK']:
      res = result['Value']
      if len(res) != 0:
        return S_OK(res)
      else:
        gLogger.warn("File not found! ", str(fileName))
        return S_ERROR("File not found!" + str(fileName))
    else:
      return S_ERROR(result['Message'])
    return result

  #############################################################################
  def checkFileTypeAndVersion(self, type, version): #fileTypeAndFileTypeVersion(self, type, version):
    result = self.dbR_.executeStoredFunctions('BOOKKEEPINGORACLEDB.checkFileTypeAndVersion', LongType, [type, version])
    '''
    if result['OK']:
      res = result['Value']
      if len(res) != 0:
        return S_OK(res)
      else:
        gLogger.error("File type not found! ", str(type))
        return S_ERROR("File type not found!" + str(type))
    else:
      return S_ERROR(result['Message'])
    '''
    return result

  #############################################################################
  def checkEventType(self, eventTypeId):  #eventType(self, eventTypeId):

    result = S_ERROR()

    retVal = self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.checkEventType', [eventTypeId])
    if retVal['OK']:
      value = retVal['Value']
      if len(value) != 0:
        result = S_OK(value)
      else:
        gLogger.error("Event type not found! ", str(eventTypeId))
        result = S_ERROR("Event type not found!" + str(eventTypeId))
    else:
      result = retVal
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
                 'WNCPUHS06': 0, \
                 'TotalLuminosity':0, \
                 'Tck':'None'}

    for param in job:
      if not attrList.__contains__(param):
        gLogger.error("insert job error: ", " the job table not contains " + param + " this attributte!!")
        return S_ERROR(" The job table not contains " + param + " this attributte!!")

      if param == 'JobStart' or param == 'JobEnd': # We have to convert data format
        dateAndTime = job[param].split(' ')
        date = dateAndTime[0].split('-')
        time = dateAndTime[1].split(':')
        if len(time) > 2:
          timestamp = datetime.datetime(int(date[0]), int(date[1]), int(date[2]), int(time[0]), int(time[1]), int(time[2]), 0)
        else:
          timestamp = datetime.datetime(int(date[0]), int(date[1]), int(date[2]), int(time[0]), int(time[1]), 0, 0)
        attrList[param] = timestamp
      else:
        attrList[param] = job[param]

    try:
      conv = int(attrList['Tck'])
      attrList['Tck'] = str(hex(conv))
    except ValueError:
      pass #it is already defined


    result = self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.insertJobsRow', LongType, [ attrList['ConfigName'], attrList['ConfigVersion'], \
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
                  attrList['WNCPUHS06'], \
                  attrList['TotalLuminosity'], \
                  attrList['Tck'] ])
    return result

  #############################################################################
  def insertInputFile(self, jobID, FileId):
    result = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.insertInputFilesRow', [FileId, jobID], False)
    return result

  #############################################################################
  def insertOutputFile(self, file):

      attrList = {  'Adler32':None, \
                    'CreationDate':None, \
                    'EventStat':None, \
                    'EventTypeId':None, \
                    'FileName':None, \
                    'FileTypeId':None, \
                    'GotReplica':None, \
                    'Guid':None, \
                    'JobId':None, \
                    'MD5Sum':None, \
                    'FileSize':0, \
                    'FullStat':None, \
                    'QualityId': 'UNCHECKED', \
                    'Luminosity':0, \
                    'InstLuminosity':0, \
                    'VisibilityFlag':'Y'}

      for param in file:
        if not attrList.__contains__(param):
          gLogger.error("insert file error: ", " the files table not contains " + param + " this attributte!!")
          return S_ERROR(" The files table not contains " + param + " this attributte!!")

        if param == 'CreationDate': # We have to convert data format
          dateAndTime = file[param].split(' ')
          date = dateAndTime[0].split('-')
          time = dateAndTime[1].split(':')
          timestamp = datetime.datetime(int(date[0]), int(date[1]), int(date[2]), int(time[0]), int(time[1]), 0, 0)
          attrList[param] = timestamp
        else:
          attrList[param] = file[param]
      utctime = datetime.datetime.utcnow()
      result = self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.insertFilesRow', LongType, [  attrList['Adler32'], \
                    attrList['CreationDate'], \
                    attrList['EventStat'], \
                    attrList['EventTypeId'], \
                    attrList['FileName'], \
                    attrList['FileTypeId'], \
                    attrList['GotReplica'], \
                    attrList['Guid'], \
                    attrList['JobId'], \
                    attrList['MD5Sum'], \
                    attrList['FileSize'], \
                    attrList['FullStat'], utctime, \
                    attrList['QualityId'], \
                    attrList['Luminosity'], \
                    attrList['InstLuminosity'], \
                    attrList['VisibilityFlag'] ])
      return result

  #############################################################################
  def updateReplicaRow(self, fileID, replica): #, name, location):
    result = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.updateReplicaRow', [fileID, replica], False)
    return result

  #############################################################################
  def deleteJob(self, jobID):
    result = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.deleteJob', [jobID], False)
    return result

  #############################################################################
  def deleteInputFiles(self, jobid):
    result = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.deleteInputFiles', [jobid], False)
    return result

  #############################################################################
  def deleteFile(self, fileid):
    result = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.deletefile', [fileid], False)
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
  def insertSimConditions(self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity, G4settings):
    return self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.insertSimConditions', LongType, [simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity, G4settings])

  #############################################################################
  def getSimConditions(self):
    return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getSimConditions', [])

  #############################################################################
  def insertDataTakingCond(self, conditions):
    datataking = {  'Description':None, \
                    'BeamCond':None, \
                    'BeamEnergy':None, \
                    'MagneticField':None, \
                    'VELO':None, \
                    'IT':None, \
                    'TT':None, \
                    'OT':None, \
                    'RICH1':None, \
                    'RICH2':None, \
                    'SPD_PRS':None, \
                    'ECAL':None, \
                    'HCAL':None, \
                    'MUON':None, \
                    'L0':None, \
                    'HLT':None,
                    'VeloPosition':None}

    for param in conditions:
      if not datataking.__contains__(param):
        gLogger.error("insert datataking error: ", " the files table not contains " + param + " this attributte!!")
        return S_ERROR(" The datatakingconditions table not contains " + param + " this attributte!!")
      datataking[param] = conditions[param]

    res = self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.insertDataTakingCond', LongType, [datataking['Description'], datataking['BeamCond'], datataking['BeamEnergy'], \
                                                                                  datataking['MagneticField'], datataking['VELO'], \
                                                                                  datataking['IT'], datataking['TT'], datataking['OT'], \
                                                                                  datataking['RICH1'], datataking['RICH2'], \
                                                                                  datataking['SPD_PRS'], datataking['ECAL'], \
                                                                                  datataking['HCAL'], datataking['MUON'], datataking['L0'], datataking['HLT'], datataking['VeloPosition'] ])
    return res


  #############################################################################
  def removeReplica(self, fileName):
    result = S_ERROR()
    retVal = self.checkfile(fileName)
    if retVal['OK']:
      fileID = long(retVal['Value'][0][0])
      res = self.updateReplicaRow(fileID, 'No')
      if res['OK']:
        result = S_OK("Replica has ben removed!!!")
      else:
        result = res
    else:
      result = S_ERROR('The file ' + fileName + 'not exist in the BKK database!!!')
    return result

  #############################################################################
  def getFileMetadata(self, lfns):
    result = {}
    for file in lfns:
      res = self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getFileMetaData', [file])
      if not res['OK']:
        result[file] = res['Message']
      else:
        records = res['Value']
        for record in records:
          row = {'ADLER32':record[1], 'CreationDate':record[2], 'EventStat':record[3], 'FullStat':record[10], 'EventTypeId':record[4], 'FileType':record[5], 'GotReplica':record[6], 'GUID':record[7], 'MD5SUM':record[8], 'FileSize':record[9], 'DQFlag':record[11], 'JobId':record[12], 'RunNumber':record[13], 'InsertTimeStamp':record[14], 'Luminosity':record[15], 'InstLuminosity':record[16]}
          result[file] = row
    return S_OK(result)

  #############################################################################
  def getFileMetaDataForWeb(self, lfns):
    totalrecords = len(lfns)
    parametersNames = ['Name', 'FileSize', 'FileType', 'CreationDate', 'EventTypeId', 'EventStat', 'GotReplica']
    records = []
    for file in lfns:
      res = self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getFileMetaData', [file])
      if not res['OK']:
        records = [str(res['Message'])]
      else:
        values = res['Value']
        for record in values:
          row = [file, record[9], record[5], record[2], record[4], record[3], record[6]]
          records += [row]
    return S_OK({'TotalRecords':totalrecords, 'ParameterNames':parametersNames, 'Records':records})

  #############################################################################
  def __getProductionStatisticsForUsers(self, prod):
    command = "select count(*), SUM(files.EventStat), SUM(files.FILESIZE), sum(files.Luminosity), sum(files.instLuminosity) from files ,jobs where jobs.jobid=files.jobid and jobs.production=%d" % (prod)
    res = self.dbR_._query(command)
    return res

  #############################################################################
  def getProductionFilesForWeb(self, prod, ftypeDict, SortDict, StartItem, Maxitems):

    command = ''
    parametersNames = ['Name', 'FileSize', 'FileType', 'CreationDate', 'EventTypeId', 'EventStat', 'GotReplica', 'InsertTimeStamp', 'Luminosity', 'InstLuminosity']
    records = []
    result = S_ERROR()

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

      command = "select rnum, filename, filesize, name , creationdate, eventtypeId, eventstat,gotreplica, inserttimestamp , luminosity ,instLuminosity from \
                ( select rownum rnum, filename, filesize, name , creationdate, eventtypeId, eventstat, gotreplica, inserttimestamp, luminosity,instLuminosity \
                from ( select files.filename, files.filesize, filetypes.name , files.creationdate, files.eventtypeId, files.eventstat,files.gotreplica, files.inserttimestamp, files.luminosity, files.instLuminosity \
                           from jobs,files, filetypes where \
                           jobs.jobid=files.jobid and \
                           jobs.production=%s and filetypes.filetypeid=files.filetypeid and filetypes.name='%s' Order by files.filename) where rownum <= %d ) where rnum > %d" % (prod, ftype, Maxitems, StartItem)
    else:

      command = "select rnum, fname, fsize, name, fcreation, feventtypeid, feventstat, fgotreplica, finst, flumi, finstlumy from \
      (select rownum rnum, fname, fsize, ftypeid, fcreation, feventtypeid, feventstat, fgotreplica, finst, flumi, finstlumy\
      from ( select files.filename fname, files.filesize fsize, filetypeid ftypeid, files.creationdate fcreation, files.eventtypeId feventtypeid, \
          files.eventstat feventstat, files.gotreplica fgotreplica, files.inserttimestamp finst, files.luminosity flumi, files.instLuminosity finstlumy\
            from jobs,files where\
            jobs.jobid=files.jobid and\
            jobs.production=%d\
            Order by files.filename) where rownum <=%d)f , filetypes ft where rnum > %d and ft.filetypeid=f.ftypeid" % (prod, Maxitems, StartItem)

    res = self.dbR_._query(command)
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        row = [record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8]]
        records += [row]
      result = S_OK({'TotalRecords':totalrecords, 'ParameterNames':parametersNames, 'Records':records, 'Extras': {'GlobalStatistics':{'Number of Events':nbOfEvents, 'Files Size':filesSize }}})
    else:
      result = res
    return result

  #############################################################################
  def exists(self, lfns):
    result = {}
    for file in lfns:
      res = self.dbR_.executeStoredFunctions('BOOKKEEPINGORACLEDB.fileExists', LongType, [file])
      if not res['OK']:
        return S_ERROR(res['Message'])
      if res['Value'] == 0:
        result[file] = False
      else:
        result[file] = True
    return S_OK(result)

  #############################################################################
  def addReplica(self, fileName):
    result = S_ERROR()
    retVal = self.checkfile(fileName)
    if retVal['OK']:
      fileID = long(retVal['Value'][0][0])
      res = self.updateReplicaRow(fileID, 'Yes')
      if res['OK']:
        result = S_OK("Replica has ben added!!!")
      else:
        result = res
    else:
      result = S_ERROR('The file ' + fileName + 'not exist in the BKK database!!!')
    return result


  #############################################################################
  def getRunInformations(self, runnb):

    result = S_ERROR()
    command = "select distinct j.fillnumber, conf.configname, conf.configversion, daq.description, j.jobstart, j.jobend, j.tck \
        from jobs j, configurations conf,data_taking_conditions daq, productionscontainer prod \
         where j.configurationid=conf.configurationid and j.production<0 and prod.daqperiodid=daq.daqperiodid and j.production=prod.production and j.runnumber=%d" % (runnb)
    retVal = self.dbR_._query(command)

    if not retVal['OK']:
      result = retVal
    else:
      value = retVal['Value']
      if len(value) == 0:
        result = S_ERROR('This run is missing in the BKK DB!')
      else:
        values = {'Configuration Name':value[0][1], 'Configuration Version':value[0][2], 'FillNumber':value[0][0]}
        values['DataTakingDescription'] = value[0][3]
        values['RunStart'] = value[0][4]
        values['RunEnd'] = value[0][5]
        values['Tck'] = value[0][6]

        retVal = self.getRunProcessingPass(runnb)
        if not retVal['OK']:
          result = retVal
        else:
          values['ProcessingPass'] = retVal['Value']
          command = ' select count(*), SUM(files.EventStat), SUM(files.FILESIZE), sum(files.fullstat), files.eventtypeid , sum(files.luminosity), sum(files.instLuminosity)  from files,jobs \
               where files.JobId=jobs.JobId and  \
               files.gotReplica=\'Yes\' and \
               jobs.production<0 and \
               jobs.runnumber=' + str(runnb) + ' Group by files.eventtypeid'
          retVal = self.dbR_._query(command)
          if not retVal['OK']:
            result = retVal
          else:
            value = retVal['Value']
            if len(value) == 0:
              result = S_ERROR('Replica flag is not set!')
            else:
              nbfile = []
              nbevent = []
              fsize = []
              fstat = []
              stream = []
              luminosity = []
              ilumi = []
              for i in value:
                nbfile += [i[0]]
                nbevent += [i[1]]
                fsize += [i[2]]
                fstat += [i[3]]
                stream += [i[4]]
                luminosity += [i[5]]
                ilumi += [i[6]]

              values['Number of file'] = nbfile
              values['Number of events'] = nbevent
              values['File size'] = fsize
              values['FullStat'] = fstat
              values['Stream'] = stream
              values['luminosity'] = luminosity
              values['InstLuminosity'] = ilumi
              result = S_OK(values)

    return result

  #############################################################################
  def getProductionFilesStatus(self, productionid=None, lfns=[]):
    result = {}
    missing = []
    replicas = []
    noreplicas = []
    if productionid != None:
      command = "select files.filename, files.gotreplica from files,jobs where \
                 files.jobid=jobs.jobid and \
                 jobs.production=%d" % (productionid)
      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        return S_ERROR(retVal['Message'])
      files = retVal['Value']
      for file in files:
        if file[1] == 'Yes':
          replicas += [file[0]]
        else:
          noreplicas += [file[0]]
      result['replica'] = replicas
      result['noreplica'] = noreplicas
    elif len(lfns) != 0:
      for file in lfns:
        command = " select files.filename, files.gotreplica from files where filename='%s'" % (file)
        retVal = self.dbR_._query(command)
        if not retVal['OK']:
          return S_ERROR(retVal['Message'])
        value = retVal['Value']
        if len(value) == 0:
          missing += [file]
        else:
          for i in value:
            if i[1] == 'Yes':
              replicas += [i[0]]
            else:
              noreplicas += [i[0]]
      result['replica'] = replicas
      result['noreplica'] = noreplicas
      result['missing'] = missing

    return S_OK(result)

  #############################################################################
  def getFileCreationLog(self, lfn):

    result = S_ERROR('getLogfile error!')
    command = "select files.jobid from files where files.filename='%s'" % (lfn)
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      result = retVal
    elif len(retVal['Value']) == 0:
      result = S_ERROR('Job not in the DB')
    else:
      jobid = retVal['Value'][0][0]
      command = "select filename from files where (files.filetypeid=17 or files.filetypeid=9) and files.jobid=%d" % (jobid)
      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        result = retVal
      elif len(retVal['Value']) == 0:
          result = S_ERROR('Log file is not exist!')
      else:
        result = S_OK(retVal['Value'][0][0])
    return result

  #############################################################################
  def insertEventTypes(self, evid, desc, primary):
    return self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.insertEventTypes', [desc, evid, primary], False)

  #############################################################################
  def updateEventType(self, evid, desc, primary):
    return self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.updateEventTypes', [desc, evid, primary], False)


  #############################################################################
  def getProductionSummary(self, cName, cVersion, conddesc=default, processing=default, production=default, ftype=default, evttype=default):

    result = S_ERROR()
    conditions = ''

    if cName != default:
      conditions += " and bview.configname='%s'" % (cName)

    if cVersion != default:
      conditions += " and bview.configversion='%s'" % (cVersion)

    if ftype != default:
      conditions += " and f.filetypeid=ftypes.filetypeid and ftypes.name='%s'" % (ftype)
    else:
      conditions += ' and bview.filetypeid= f.filetypeid  \
                      and bview.filetypeid= ftypes.filetypeid '
    if evttype != default:
      conditions += " and bview.eventtypeid=%d" % (int(evttype))
    else:
      conditions += ' and bview.eventtypeid=f.eventtypeid '

    if production != default:
      conditions += " and j.production=%d" % (int(production))
      conditions += ' and j.production= bview.production '
    else:
      conditions += ' and j.production= bview.production '

    if processing != default:
      command = "select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='%s'" % (processing.split('/')[1], processing)
      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        return retVal
      else:
        pro = '('
        for i in retVal['Value']:
          pro += "%s," % (str(i[0]))
        pro = pro[:-1]
        pro += (')')
        if len(pro) > 1:
          conditions += " and prod.processingid in %s" % (pro)

    if conddesc != default:
      retVal = self._getConditionString(conddesc, 'prod')
      if not retVal['OK']:
        result = retVal
      else:
        conditions += retVal['Value']

        command = " select bview.configname, bview.configversion, sim.simdescription, daq.description, \
     prod.processingid, bview.eventtypeid,bview.description, bview.production, ftypes.name, sum(f.eventstat) \
from jobs j, prodview bview, files f, filetypes ftypes, productionscontainer prod, simulationconditions sim, data_taking_conditions daq \
where j.jobid= f.jobid and \
      f.gotreplica='Yes' and \
      bview.programname= j.programname and \
      bview.programversion= j.programversion and \
      sim.simid(+)=bview.simid and \
      daq.daqperiodid(+)=bview.daqperiodid  and \
      bview.production = prod.production " + conditions

        command += "group by bview.configname, bview.configversion, sim.simdescription, daq.description, prod.processingid, bview.eventtypeid, bview.description, bview.production, ftypes.name"
        retVal = self.dbR_._query(command)
        if not retVal['OK']:
          return S_ERROR(retVal['Message'])

        parameters = ['ConfigurationName', 'ConfigurationVersion', 'ConditionDescription', 'Processing pass ', 'EventType', 'EventType description', 'Production', 'FileType', 'Number of events']
        dbResult = retVal['Value']
        records = []
        nbRecords = 0
        for record in dbResult:
          if record[2] != None:
            conddesc = record[2]
          else:
            conddesc = record[3]
          row = [record[0], record[1], conddesc, record[4], record[5], record[6], record[7], record[8], record[9]]
          records += [row]
          nbRecords += 1
        result = S_OK({'TotalRecords':nbRecords, 'ParameterNames':parameters, 'Records':records, 'Extras': {}})

    return result

  #############################################################################
  def getProductionSimulationCond(self, prod):
    simdesc = None
    daqdesc = None

    command = "select distinct sim.simdescription, daq.description from simulationconditions sim, data_taking_conditions daq,productionscontainer prod \
              where sim.simid(+)=prod.simid and daq.daqperiodid(+)=prod.daqperiodid and prod.production=" + str(prod)
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return retVal
    else:
      value = retVal['Value']
      if len(value) != 0:
        simdesc = value[0][0]
        daqdesc = value[0][1]
      else:
        return S_ERROR('Simulation condition or data taking condition not exist!')

    if simdesc != None:
      return S_OK(simdesc)
    else:
      return S_OK(daqdesc)

  #############################################################################
  def getFileHistory(self, lfn):
    command = 'select  files.fileid, files.filename,files.adler32,files.creationdate,files.eventstat,files.eventtypeid,files.gotreplica, \
files.guid,files.jobid,files.md5sum, files.filesize,files.fullstat, dataquality.dataqualityflag, files.inserttimestamp, files.luminosity, files.instLuminosity from files, dataquality \
where files.fileid in ( select inputfiles.fileid from files,inputfiles where files.jobid= inputfiles.jobid and files.filename=\'' + str(lfn) + '\')\
and files.qualityid= dataquality.qualityid'

    res = self.dbR_._query(command)
    return res

  #############################################################################
  def getProductionInformationsFromView(self, prodid):
    command = 'select * from productioninformations where production=' + str(prodid)
    res = self.dbR_._query(command)
    return res

  #############################################################################
  #
  #          MONITORING
  #############################################################################
  def getProductionNbOfJobs(self, prodid):
    return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getJobsNb', [prodid])

  #############################################################################
  def getProductionNbOfEvents(self, prodid):
    return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getNumberOfEvents', [prodid])

  #############################################################################
  def getProductionSizeOfFiles(self, prodid):
    return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getSizeOfFiles', [prodid])

  #############################################################################
  def getProductionNbOfFiles(self, prodid):
    return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getNbOfFiles', [prodid])

  #############################################################################
  def getProductionInformation(self, prodid):
    return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getProductionInformation', [prodid])

  #############################################################################
  def getSteps(self, prodid):
    return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getSteps', [prodid])

  #############################################################################
  def getNbOfJobsBySites(self, prodid):
    return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getJobsbySites', [prodid])

  #############################################################################
  def getConfigsAndEvtType(self, prodid):
    return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getConfigsAndEvtType', [prodid])

  #############################################################################
  def getAvailableTags(self):
    result = S_ERROR()
    command = 'select name, tag from tags order by inserttimestamp desc'
    retVal = self.dbR_._query(command)
    if retVal['OK']:
      parameters = ['TagName', 'TagValue']
      dbResult = retVal['Value']
      records = []
      nbRecords = 0
      for record in dbResult:
        row = [record[0], record[1]]
        records += [row]
        nbRecords += 1
      result = S_OK({'TotalRecords':nbRecords, 'ParameterNames':parameters, 'Records':records, 'Extras': {}})
    else:
      result = retVal
    return result

  #############################################################################
  def getProductionProcessedEvents(self, prodid):
    return self.dbR_.executeStoredFunctions('BOOKKEEPINGORACLEDB.getProcessedEvents', LongType, [prodid])

  #############################################################################
  def getRunsForAGivenPeriod(self, dict):
    condition = ''
    startDate = dict.get('StartDate', default)
    endDate = dict.get('EndDate', default)
    allowOutside = dict.get('AllowOutsideRuns', default)

    if allowOutside != default:
      if startDate == default and endDate == default:
        return S_ERROR('The Start and End date must be given!')
      else:
        condition += " and jobs.jobstart >= TO_TIMESTAMP ('%s','YYYY-MM-DD HH24:MI:SS')" % (startDate)
        condition += " and jobs.jobstart <= TO_TIMESTAMP ('%s','YYYY-MM-DD HH24:MI:SS')" % (endDate)
    else:
      if startDate != default:
        condition += " and jobs.jobstart >= TO_TIMESTAMP ('%s','YYYY-MM-DD HH24:MI:SS')" % (startDate)
      if endDate != default:
        condition += " and jobs.jobend <= TO_TIMESTAMP ('%s','YYYY-MM-DD HH24:MI:SS')" % (endDate)
      elif startDate != default and endDate == default:
        d = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        condition += ' and jobs.jobend <= TO_TIMESTAMP (\'' + str(d) + '\',\'YYYY-MM-DD HH24:MI:SS\')'
        condition += " and jobs.jobend <= TO_TIMESTAMP ('%s','YYYY-MM-DD HH24:MI:SS')" % (str(d))

    command = ' select jobs.runnumber from jobs where jobs.production < 0' + condition
    retVal = self.dbR_._query(command)
    runIds = []
    if retVal['OK']:
      records = retVal['Value']
      for record in records:
        if record[0] != None:
          runIds += [record[0]]
    else:
      return S_ERROR(retVal['Message'])

    check = dict.get('CheckRunStatus', default)
    if check:
      processedRuns = []
      notProcessedRuns = []
      for i in runIds:
        command = "select files.filename from files,jobs where jobs.jobid=files.jobid and files.gotreplica='Yes' and jobs.production<0 and jobs.runnumber=%d" % (i)
        retVal = self.dbR_._query(command)
        if retVal['OK']:
          files = retVal['Value']
          ok = True
          for file in files:
            name = file[0]
            retVal = self.getFileDescendents([name], 1, 0, True)
            files = retVal['Value']
            successful = files['Successful']
            failed = files['Failed']
            if len(successful[successful.keys()[0]]) == 0:
              ok = False
          if ok:
            processedRuns += [i]
          else:
            notProcessedRuns += [i]
      return S_OK({'Runs':runIds, 'ProcessedRuns':processedRuns, 'NotProcessedRuns':notProcessedRuns})
    else:
      return S_OK({'Runs':runIds})
    return S_ERROR()

  #############################################################################
  def getProductionsFromView(self, dict):

    run = dict.get('RunNumber', dict.get('Runnumber', default))
    proc = dict.get('ProcessingPass', dict.get('ProcPass', default))
    result = S_ERROR()
    if 'Runnumber' in dict:
      gLogger.error('The Runnumber has changed to RunNumber!')

    if run != default:
      if proc != default:
        retVal = self._getProcessingPassId(proc.split('/')[1:][0], proc)
        if retVal['OK']:
          processingid = retVal['Value']
          command = "select distinct bview.production  from prodview bview, prodrunview prview, productionscontainer prod where \
      bview.production=prview.production and prview.runnumber=%d and bview.production>0 and bview.production=prod.production and prod.processingid=%d" % (run, processingid)
          result = self.dbR_._query(command)
      else:
        result = S_ERROR('The processing pass is missing!')
    else:
      result = S_ERROR('The run number is missing!')

    return result

  #############################################################################
  def getRunFilesDataQuality(self, runs):
    retVal = self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getRunQuality', [], True, runs)
    return retVal

  #############################################################################
  def getRunAndProcessingPassDataQuality(self, runnb, processing):
    return self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.getQFlagByRunAndProcId', StringType, [runnb, processing])

  #############################################################################
  def getRunWithProcessingPassAndDataQuality(self, procpass, flag='ALL'):
    retVal = self._getProcessingPassId(procpass.split('/')[1:][0], procpass)
    if retVal['OK']:
      processingid = retVal['Value']
      id = None
      if flag != default:
        retVal = self._getDataQualityId(flag)
        if retVal['OK']:
          id = retVal['Value']
        else:
          return retVal
    return self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.getRunByQflagAndProcId', LongType, [processingid, id])

  #############################################################################
  def setFilesInvisible(self, lfns):
    for i in lfns:
      retVal = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.setFileInvisible', [i], False)
      if not retVal['OK']:
        return retVal
    return S_OK('The files are invisible!')

  #############################################################################
  def setFilesVisible(self, lfns):
    for i in lfns:
      res = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.setFileVisible', [i], False)
      if not res['OK']:
        return res
    return S_OK('The files are visible!')

  #############################################################################
  def getFiles(self, simdesc, datataking, procPass, ftype, evt, configName='ALL', configVersion='ALL', production='ALL', flag='ALL', startDate=None, endDate=None, nbofEvents=False, startRunID=None, endRunID=None, runnumbers=[], replicaFlag='Yes', visible='ALL', filesize=False, tcks=[]):

    condition = ''
    tables = ' files f,jobs j '

    if configName != default and configVersion != default:
      tables += ' ,configurations c'
      condition = "  and c.ConfigName='%s' and c.ConfigVersion='%s' and j.configurationid=c.configurationid " % (configName, configVersion)


    if production != default:
      if type(production) == types.ListType:
        condition += ' and '
        cond = ' ( '
        for i in production:
          cond += 'j.production=' + str(i) + ' or '
        cond = cond[:-3] + ')'
        condition += cond
      else:
        condition += ' and j.production=' + str(production)

    if len(runnumbers) > 0:
      if type(runnumbers) == types.ListType:
        cond = ' ( '
        for i in runnumbers:
          cond += 'j.runnumber=' + str(i) + ' or '
        cond = cond[:-3] + ')'
      if startRunID == None and endRunID == None:
        condition += " and %s " % (cond)
      elif startRunID != None and endRunID != None:
        condition += ' and (j.runnumber>=%s and j.runnumber<=%s or %s)' % (str(startRunID), str(endRunID), cond)
    else:
      if startRunID != None:
        condition += ' and j.runnumber>=' + str(startRunID)
      if endRunID != None:
        condition += ' and j.runnumber<=' + str(endRunID)

    if len(tcks) > 0:
      if type(tcks) == types.ListType:
        cond = '('
        for i in tcks:
          cond += "j.tck='%s' or " % (i)
        cond = cond[:-3] + ')'
        condition = " and %s " % (cond)
      elif type(tcks) == types.StringType:
        condition += " and j.tck='%s'" % (tcks)
      else:
        return S_ERROR('The TCK should be a list or a string')

    if procPass != default:
      if not re.search('^/', procPass): procPass = procPass.replace(procPass, '/%s' % procPass)
      command = "select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='%s'" % (procPass.split('/')[1], procPass)
      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        return retVal
      pro = '('
      for i in retVal['Value']:
        pro += "%s," % (str(i[0]))
      pro = pro[:-1]
      pro += ')'

      condition += " and j.production=prod.production \
                     and prod.processingid in %s" % (pro);
      tables += ',productionscontainer prod'

    if ftype != default:
      tables += ' ,filetypes ft'
      if type(ftype) == types.ListType:
        condition += ' and '
        cond = ' ( '
        for i in ftype:
          cond += " ft.name='%s' or " % (i)
        cond = cond[:-3] + ')'
        condition += cond
      elif type(ftype) == types.StringType:
        condition += " and ft.name='%s'" % (ftype)
      else:
        return S_ERROR('File type problem!')
      condition += ' and f.filetypeid=ft.filetypeid'

    if evt != 0:
      if type(evt) in (types.ListType, types.TupleType):
        condition += ' and '
        cond = ' ( '
        for i in evt:
          cond += ' f.eventtypeid=' + str(i) + ' or '
        cond = cond[:-3] + ')'
        condition += cond
      elif type(evt) in (types.StringTypes + (types.IntType, types.LongType)):
        condition += ' and f.eventtypeid=' + str(evt)

    if startDate != None:
      condition += ' and f.inserttimestamp >= TO_TIMESTAMP (\'' + str(startDate) + '\',\'YYYY-MM-DD HH24:MI:SS\')'

    if endDate != None:
      condition += ' and f.inserttimestamp <= TO_TIMESTAMP (\'' + str(endDate) + '\',\'YYYY-MM-DD HH24:MI:SS\')'
    elif startDate != None and endDate == None:
      d = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
      condition += ' and f.inserttimestamp <= TO_TIMESTAMP (\'' + str(d) + '\',\'YYYY-MM-DD HH24:MI:SS\')'

    if flag != default:
      if type(flag) in (types.ListType, types.TupleType):
        conds = ' ('
        for i in flag:
          quality = None
          command = 'select QualityId from dataquality where dataqualityflag=\'' + str(i) + '\''
          res = self.dbR_._query(command)
          if not res['OK']:
            gLogger.error('Data quality problem:', res['Message'])
          elif len(res['Value']) == 0:
              return S_ERROR('Dataquality is missing!')
          else:
            quality = res['Value'][0][0]
          conds += ' f.qualityid=' + str(quality) + ' or'
        condition += 'and' + conds[:-3] + ')'
      else:
        quality = None
        command = 'select QualityId from dataquality where dataqualityflag=\'' + str(flag) + '\''
        res = self.dbR_._query(command)
        if not res['OK']:
          gLogger.error('Data quality problem:', res['Message'])
        elif len(res['Value']) == 0:
            return S_ERROR('Dataquality is missing!')
        else:
          quality = res['Value'][0][0]

        condition += ' and f.qualityid=' + str(quality)


    if replicaFlag in ['Yes', 'No']:
      condition += ' and f.gotreplica=\'' + replicaFlag + '\''

    visibleFlags = {'Yes':'Y', 'No':'N'}
    if visible != default and visible in visibleFlags:
      condition += " and f.visibilityflag='%s'" % (visibleFlags[visible])

    if simdesc != default:
      condition += " and prod.simid=sim.simid and j.production=prod.production and \
                     sim.simdescription='%s' " % (simdesc)
      if re.search('productionscontainer', tables):
        tables += ',simulationconditions sim'
      else:
        tables += ',simulationconditions sim, productionscontainer prod'


    if datataking != default:
      condition += " and prod.DAQPERIODID =daq.daqperiodid and j.production=prod.production and \
                    daq.description='%s' " % (datataking)
      if re.search('productionscontainer', tables):
        tables += ',data_taking_conditions daq'
      else:
        tables += ',data_taking_conditions daq, productionscontainer prod'


    if nbofEvents:
      command = " select sum(f.eventstat) from %s where f.jobid= j.jobid %s " % (tables, condition)
    elif filesize:
      command = " select sum(f.filesize) from %s where f.jobid= j.jobid %s " % (tables, condition)
    else:
      command = " select distinct f.filename from %s where f.jobid= j.jobid %s " % (tables, condition)

    res = self.dbR_._query(command)

    return res

  #############################################################################
  def getVisibleFilesWithMetadata(self, simdesc, datataking, procPass, ftype, evt, configName='ALL', configVersion='ALL', production='ALL', flag='ALL', startDate=None, endDate=None, nbofEvents=False, startRunID=None, endRunID=None, runnumbers=[], replicaFlag='Yes', tcks=[]):

    condition = ''


    tables = ' jobs j, files f'
    yes = False
    if configName != default:
      condition += " and c.configname='%s' " % (configName)
      yes = True

    if configVersion != default:
      condition += " and c.configversion='%s' " % (configVersion)
      yes = True

    if yes:
      condition += ' and j.configurationid=c.configurationid'
      tables += ' ,configurations c '

    if simdesc != default:
      conddescription = simdesc
    else:
      conddescription = datataking

    sim_dq_conditions = ''
    if conddescription != default:
      retVal = self._getConditionString(conddescription, 'prod')
      if retVal['OK']:
        sim_dq_conditions = retVal['Value']
      else:
        return retVal

    if production != default:
      if type(production) == types.ListType:
        condition += ' and '
        cond = ' ( '
        for i in production:
          cond += 'j.production=' + str(i) + ' or '
        cond = cond[:-3] + ')'
        condition += cond
      else:
        condition += ' and j.production=' + str(production)

    if len(runnumbers) > 0:
      if type(runnumbers) == types.ListType:
        condition += ' and '
        cond = ' ( '
        for i in runnumbers:
          cond += 'j.runnumber=' + str(i) + ' or '
        cond = cond[:-3] + ')'
        condition += cond


    fcond = ''
    tables2 = ' ,filetypes ftypes '
    tables += ' ,filetypes ftypes '
    if ftype != 'ALL':
      if type(ftype) == types.ListType:
        for i in ftype:
          condition += " and ftypes.Name='%s'" % (str(i))
          fcond += " and ftypes.Name='%s'" % (str(i))

      elif type(ftype) == types.StringType:
        condition += " and ftypes.Name='%s'" % (str(ftype))
        fcond += " and ftypes.Name='%s'" % (str(ftype))
      fcond += 'and bview.filetypeid=ftypes.filetypeid '
      condition += ' and f.filetypeid=ftypes.filetypeid '
    econd = ''
    if evt != 0:
      if type(evt) in (types.ListType, types.TupleType):
        econd += " and bview.eventtypeid=%s" % (str(i))
        condition += " and f.eventtypeid=%s" % (str(i))

      elif type(evt) in (types.StringTypes + (types.IntType, types.LongType)):
        econd += " and bview.eventtypeid='%s'" % (str(evt))
        condition += " and f.eventtypeid=%s" % (str(evt))

    if len(tcks) > 0:
      if type(tcks) == types.ListType:
        cond = '('
        for i in tcks:
          cond += "j.tck='%s' or " % (i)
        cond = cond[:-3] + ')'
        condition = " and %s " % (cond)
      elif type(tcks) == types.StringType:
        condition += " and j.tck='%s'" % (tcks)
      else:
        return S_ERROR('The TCK should be a list or a string')

    if procPass != default:
      if not re.search('^/', procPass): procPass = procPass.replace(procPass, '/%s' % procPass)
      condition += " and j.production in (select bview.production from productionscontainer prod, ( select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID FROM processing v \
                     START WITH id in (select distinct id from processing where name='%s') \
                                           CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                         where v.path='%s') proc, prodview bview %s \
                             where \
                               prod.processingid=proc.id and \
                               bview.production=prod.production \
                               %s %s %s \
                )" % (procPass.split('/')[1], procPass, tables2, fcond, econd, sim_dq_conditions)

    if startDate != None:
      condition += ' and f.inserttimestamp >= TO_TIMESTAMP (\'' + str(startDate) + '\',\'YYYY-MM-DD HH24:MI:SS\')'

    if endDate != None:
      condition += ' and f.inserttimestamp <= TO_TIMESTAMP (\'' + str(endDate) + '\',\'YYYY-MM-DD HH24:MI:SS\')'
    elif startDate != None and endDate == None:
      d = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
      condition += ' and f.inserttimestamp <= TO_TIMESTAMP (\'' + str(d) + '\',\'YYYY-MM-DD HH24:MI:SS\')'

    if flag != default:
      if type(flag) in (types.ListType, types.TupleType):
        conds = ' ('
        for i in flag:
          quality = None
          command = 'select QualityId from dataquality where dataqualityflag=\'' + str(i) + '\''
          res = self.dbR_._query(command)
          if not res['OK']:
            gLogger.error('Data quality problem:', res['Message'])
          elif len(res['Value']) == 0:
              return S_ERROR('Dataquality is missing!')
          else:
            quality = res['Value'][0][0]
          conds += ' f.qualityid=' + str(quality) + ' or'
        condition += 'and' + conds[:-3] + ')'
      else:
        quality = None
        command = 'select QualityId from dataquality where dataqualityflag=\'' + str(flag) + '\''
        res = self.dbR_._query(command)
        if not res['OK']:
          gLogger.error('Data quality problem:', res['Message'])
        elif len(res['Value']) == 0:
            return S_ERROR('Dataquality is missing!')
        else:
          quality = res['Value'][0][0]

        condition += ' and f.qualityid=' + str(quality)

    if startRunID != None:
      condition += ' and j.runnumber>=' + str(startRunID)
    if endRunID != None:
      condition += ' and j.runnumber<=' + str(endRunID)

    command = " select distinct f.filename, f.eventstat, j.eventinputstat, j.runnumber, j.fillnumber, f.filesize, j.totalluminosity, f.luminosity, f.instLuminosity, j.tck from %s where f.jobid= j.jobid and f.visibilityflag='Y'  and f.gotreplica='Yes' %s " % (tables, condition)

    res = self.dbR_._query(command)

    return res

  #############################################################################
  def getFilesSummary(self, configName, configVersion, conddescription=default, processing=default, evt=default, production=default, filetype=default, quality=default, runnb=default, startrun=default, endrun=default):

    condition = ''
    tables = ' jobs j, files f'
    if configName != default:
      tables += ' , configurations c'
      condition += " and c.configname='%s' " % (configName)
      condition += ' and j.configurationid=c.configurationid '

    if configVersion != default:
      condition += " and c.configversion='%s' " % (configVersion)

    sim_dq_conditions = ''
    if conddescription != default:
      retVal = self._getConditionString(conddescription, 'prod')
      if retVal['OK']:
        sim_dq_conditions = retVal['Value']
      else:
        return retVal

    econd = ''
    if evt != default:
      econd += " and bview.eventtypeid='%s'" % (str(evt))
      condition += " and f.eventtypeid=%s" % (str(evt))

    if production != default:
      condition += ' and j.production=' + str(production)

    if runnb != default and (type(runnb) != types.ListType):
      condition += ' and j.runnumber=' + str(runnb)
    elif type(runnb) == types.ListType:
      cond = ' ( '
      for i in runnb:
        cond += 'j.runnumber=' + str(i) + ' or '
      condition += " and %s )" % (cond[:-3])


    tables += ' ,filetypes ftypes '
    tables2 = ''
    fcond = ''
    if filetype != default:
      tables2 = ' ,filetypes ftypes '
      condition += " and f.filetypeid=ftypes.filetypeid and ftypes.Name='%s'" % (str(filetype))
      fcond += " and ftypes.Name='%s'" % (str(filetype))
      fcond += 'and bview.filetypeid=ftypes.filetypeid '


    if quality != default:
      tables += ' , dataquality d'
      if type(quality) == types.StringType:
        command = "select QualityId from dataquality where dataqualityflag='%s'" % (quality)
        res = self.dbR_._query(command)
        if not res['OK']:
          gLogger.error('Data quality problem:', res['Message'])
        elif len(res['Value']) == 0:
            return S_ERROR('Dataquality is missing!')
        else:
          quality = res['Value'][0][0]
        condition += ' and f.qualityid=' + str(quality)
      else:
        conds = ' ('
        for i in quality:
          quality = None
          command = "select QualityId from dataquality where dataqualityflag='%s'" % (str(i))
          res = self.dbR_._query(command)
          if not res['OK']:
            gLogger.error('Data quality problem:', res['Message'])
          elif len(res['Value']) == 0:
              return S_ERROR('Dataquality is missing!')
          else:
            quality = res['Value'][0][0]
          conds += ' f.qualityid=' + str(quality) + ' or'
        condition += 'and' + conds[:-3] + ')'
      condition += ' and d.qualityid=f.qualityid '

    if processing != default:
      if not re.search('^/', processing): processing = processing.replace(processing, '/%s' % processing)
      condition += " and j.production in (select bview.production from productionscontainer prod, ( select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID FROM processing v \
                     START WITH id in (select distinct id from processing where name='%s') \
                                           CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                         where v.path='%s') proc, prodview bview %s \
                             where \
                               prod.processingid=proc.id and \
                               bview.production=prod.production \
                               %s %s %s \
                )" % (processing.split('/')[1], processing, tables2, fcond, econd, sim_dq_conditions)

    if startrun != default:
      condition += " and j.runnumber > %d" % (startrun)

    if endrun != default:
      condition += " and j.runnumber < %d" % (endrun)

    command = "select count(*), SUM(f.EventStat), SUM(f.FILESIZE), SUM(f.luminosity),SUM(f.instLuminosity) from  %s  where \
    j.jobid=f.jobid and \
    ftypes.filetypeid=f.filetypeid and \
    f.gotreplica='Yes' and \
    f.visibilityflag='Y' and \
    ftypes.filetypeid=f.filetypeid  %s" % (tables, condition)
    return self.dbR_._query(command)

  #############################################################################
  def getLimitedFiles(self, configName, configVersion, conddescription=default, processing=default, evt=default, production=default, filetype=default, quality=default, runnb=default, startitem=0, maxitems=10):

    condition = ''
    if configName != default:
      condition += " and c.configname='%s' " % (configName)

    if configVersion != default:
      condition += " and c.configversion='%s' " % (configVersion)

    sim_dq_conditions = ''
    if conddescription != default:
      retVal = self._getConditionString(conddescription, 'prod')
      if retVal['OK']:
        sim_dq_conditions = retVal['Value']
        condition += sim_dq_conditions
      else:
        return retVal

    tables = ''
    if evt != default:
      tables += ' ,prodview bview'
      condition += '  and j.production=bview.production and bview.production=prod.production and bview.eventtypeid=%s and f.eventtypeid=bview.eventtypeid ' % (evt)

    if production != default:
      condition += ' and j.production=%d' % (production)

    if runnb != default:
      condition += ' and j.runnumber=%d' % (runnb)

    if filetype != default:
      condition += " and ftypes.name='%s' and bview.filetypeid=ftypes.filetypeid " % (filetype)

    if quality != 'ALL':
      tables += ' ,dataquality d '
      condition += 'and d.qualityid=f.qualityid '
      if type(quality) == types.StringType:
        command = "select QualityId from dataquality where dataqualityflag='%s'" % (quality)
        res = self.dbR_._query(command)
        if not res['OK']:
          gLogger.error('Data quality problem:', res['Message'])
        elif len(res['Value']) == 0:
            return S_ERROR('Dataquality is missing!')
        else:
          quality = res['Value'][0][0]
        condition += ' and f.qualityid=' + str(quality)
      else:
        if type(quality) == types.ListType and len(quality) > 0:
          conds = ' ('
          for i in quality:
            quality = None
            command = "select QualityId from dataquality where dataqualityflag='%s'" % (i)
            res = self.dbR_._query(command)
            if not res['OK']:
              gLogger.error('Data quality problem:', res['Message'])
            elif len(res['Value']) == 0:
                return S_ERROR('Dataquality is missing!')
            else:
              quality = res['Value'][0][0]
            conds += ' f.qualityid=' + str(quality) + ' or'
          condition += 'and' + conds[:-3] + ')'

    if processing != default:
      command = "select distinct prod.processingid from productionscontainer prod where \
           prod.processingid in (select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID FROM processing v \
         START WITH id in (select distinct id from processing where name='%s') \
         CONNECT BY NOCYCLE PRIOR  id=parentid) v \
        where v.path='%s') %s" % (processing.split('/')[1], processing, sim_dq_conditions)

      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        return retVal
      pro = '('
      for i in retVal['Value']:
        pro += "%s," % (str(i[0]))
      pro = pro[:-1]
      pro += (')')

      condition += " and prod.processingid in %s" % (pro)

    command = "select fname, fstat, fsize, fcreation, jstat, jend, jnode, ftypen, evttypeid, jrun, jfill, ffull, dflag,   jevent, jtotal, flum, finst, jtck from \
              (select rownum r, fname, fstat, fsize, fcreation, jstat, jend, jnode, ftypen, evttypeid, jrun, jfill, ffull, dflag,   jevent, jtotal, flum, finst, jtck from \
                  (select ROWNUM r, f.FileName fname, f.EventStat fstat, f.FileSize fsize, f.CreationDate fcreation, j.JobStart jstat, j.JobEnd jend, j.WorkerNode jnode, ftypes.Name ftypen, f.eventtypeid evttypeid,\
                          j.runnumber jrun, j.fillnumber jfill, f.fullstat ffull, d.dataqualityflag dflag,j.eventinputstat jevent, j.totalluminosity jtotal,\
                           f.luminosity flum, f.instLuminosity finst, j.tck jtck from files f, jobs j, productionscontainer prod, configurations c, filetypes ftypes %s where \
    j.jobid=f.jobid and \
    ftypes.filetypeid=f.filetypeid and \
    f.gotreplica='Yes' and \
    f.visibilityflag='Y' and \
    j.configurationid=c.configurationid  %s) where rownum <=%d ) where r >%d" % (tables, condition, int(maxitems), int(startitem))
    return self.dbR_._query(command)

  #############################################################################
  def getDataTakingCondId(self, condition):
    command = 'select DaqPeriodId from data_taking_conditions where '
    for param in condition:
      if type(condition[param]) == types.StringType and len(condition[param].strip()) == 0:
        command += str(param) + ' is NULL and '
      elif condition[param] != None:
        command += str(param) + '=\'' + condition[param] + '\' and '
      else:
        command += str(param) + ' is NULL and '

    command = command[:-4]
    res = self.dbR_._query(command)
    if res['OK']:
        if len(res['Value']) == 0:
          command = 'select DaqPeriodId from data_taking_conditions where '
          for param in condition:
            if param != 'Description':
              if type(condition[param]) == types.StringType and len(condition[param].strip()) == 0:
                command += str(param) + ' is NULL and '
              elif condition[param] != None:
                command += str(param) + '=\'' + condition[param] + '\' and '
              else:
                command += str(param) + ' is NULL and '

          command = command[:-4]
          retVal = self.dbR_._query(command)
          if retVal['OK']:
            if len(retVal['Value']) != 0:
              return S_ERROR('Only the Description is different, the other attributes are the same and they are exists in the DB!')
    return res

  #############################################################################
  def getDataTakingCondDesc(self, condition):
    command = 'select description from data_taking_conditions where '
    for param in condition:
      if type(condition[param]) == types.StringType and len(condition[param].strip()) == 0:
        command += str(param) + ' is NULL and '
      elif condition[param] != None:
        command += str(param) + '=\'' + condition[param] + '\' and '
      else:
        command += str(param) + ' is NULL and '

    command = command[:-4]
    res = self.dbR_._query(command)
    if res['OK']:
        if len(res['Value']) == 0:
          command = 'select DaqPeriodId from data_taking_conditions where '
          for param in condition:
            if param != 'Description':
              if type(condition[param]) == types.StringType and len(condition[param].strip()) == 0:
                command += str(param) + ' is NULL and '
              elif condition[param] != None:
                command += str(param) + '=\'' + condition[param] + '\' and '
              else:
                command += str(param) + ' is NULL and '

          command = command[:-4]
          retVal = self.dbR_._query(command)
          if retVal['OK']:
            if len(retVal['Value']) != 0:
              return S_ERROR('Only the Description is different, the other attributes are the same and they are exists in the DB!')
    return res

  #############################################################################
  def getStepIdandNameForRUN(self, programName, programVersion):
    command = "select stepid, stepname from steps where applicationname='%s' and applicationversion='%s'" % (programName, programVersion)
    retVal = self.dbR_._query(command)
    if retVal['OK']:
      stepName = 'Real Data'
      if len(retVal['Value']) == 0:
        retVal = self.insertStep({'Step':{'StepName':stepName, 'ApplicationName':programName, 'ApplicationVersion':programVersion, 'ProcessingPass':stepName}, 'OutputFileTypes':[{'FileType':'RAW', 'Visible':'Y'}]})
        if retVal['OK']:
          return S_OK([retVal['Value'], stepName])
        else:
          return retVal
      else:
        return S_OK([retVal['Value'][0][0], retVal['Value'][0][1]])
    else:
      return retVal

  #############################################################################
  def __getPassIds(self, name):
    command = "select id from processing where name='%s'" % (name)
    retVal = self.dbR_._query(command)
    if retVal['OK']:
      result = []
      for i in retVal['Value']:
        result += [i[0]]
      return S_OK(result)
    else:
      return retVal

  #############################################################################
  def __getprocessingid(self, id):
    command = 'SELECT name "Name", CONNECT_BY_ISCYCLE "Cycle", \
   LEVEL, SYS_CONNECT_BY_PATH(name, \'/\') "Path", id "ID" \
   FROM processing \
   START WITH id=' + str(id) + '\
   CONNECT BY NOCYCLE PRIOR  parentid=id AND LEVEL <= 5 \
   ORDER BY  Level desc, "Name", "Cycle", "Path"'
    return self.dbR_._query(command)

  #############################################################################
  def __checkprocessingpass(self, opath, values):
    if len(opath) != len(values):
      return False
    else:
      j = 0
      for i in values:
        if i[0] != opath[j]:
          return False
        j += 1
      return True

  #############################################################################
  def __insertprocessing(self, values, parentid=None, ids=[]):

    for i in values:
      command = ''
      if parentid != None:
        command = "select id from processing where name='%s' and parentid=%s" % (i, parentid)
      else:
        command = "select id from processing where name='%s' and parentid is null" % (i)
      retVal = self.dbR_._query(command)
      if retVal['OK']:
        if len(retVal['Value']) == 0:
          if parentid != None:
            command = 'select max(id)+1 from processing'
            retVal = self.dbR_._query(command)
            if retVal['OK']:
              id = retVal['Value'][0][0]
              ids += [id]
              command = "insert into processing(id,parentid,name)values(%d,%d,'%s')" % (id, parentid, i)
              retVal = self.dbW_._query(command)
              if not retVal['OK']:
                gLogger.error(retVal['Message'])
              values.remove(i)
              self.__insertprocessing(values, id, ids)
          else:
            command = 'select max(id)+1 from processing'
            retVal = self.dbR_._query(command)
            if retVal['OK']:
              id = retVal['Value'][0][0]
              if id == None:
                id = 1
              ids += [id]
              command = "insert into processing(id,parentid,name)values(%d,null,'%s')" % (id, i)
              retVal = self.dbW_._query(command)
              if not retVal['OK']:
                gLogger.error(retVal['Message'])
              values.remove(i)
              self.__insertprocessing(values, id, ids)
        else:

          values.remove(i)
          parentid = retVal['Value'][0][0]
          ids += [parentid]
          self.__insertprocessing(values, parentid, ids)


  #############################################################################
  def addProcessing(self, path):

    lastindex = len(path) - 1
    retVal = self.__getPassIds(path[lastindex])
    stepids = []
    if not retVal['OK']:
      return retVal
    else:
      ids = retVal['Value']
      if len(ids) == 0:
        newpath = list(path)
        self.__insertprocessing(newpath, None, stepids)
        return S_OK(stepids[-1:])
      else:
        for i in ids:
          procs = self.__getprocessingid(i)
          if len(procs) > 0:
            if self.__checkprocessingpass(path, procs):
              return S_OK()
        newpath = list(path)
        self.__insertprocessing(newpath, None, stepids)
        return S_OK(stepids[-1:])
    return S_ERROR()

  #############################################################################
  def insertStepsContainer(self, prod, stepid, step):
    return self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.insertStepsContainer', [prod, stepid, step], False)

  #############################################################################
  def insertproductionscontainer(self, prod, processingid, simid, daqperiodid):
    return self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.insertproductionscontainer', [ prod, processingid, simid, daqperiodid], False)

  #############################################################################
  def addProductionSteps(self, steps, prod):
    level = 1
    for i in steps:
      retVal = self.insertStepsContainer(prod, i['StepId'], level)
      if not retVal['OK']:
        return retVal
      level += 1
    return S_OK()

  #############################################################################
  def checkProcessingPassAndSimCond(self, production):
    command = ' select count(*) from productionscontainer where production=' + str(production)
    res = self.dbR_._query(command)
    return res

  #############################################################################
  def addProduction(self, production, simcond=None, daq=None, steps=default, inputproc=''):

    path = []
    if inputproc != '':
      if inputproc[0] != '/': inputproc = '/' + inputproc
      path = inputproc.split('/')[1:]

    for i in steps:
      if i['Visible'] == 'Y':
        res = self.getAvailableSteps({'StepId':i['StepId']})
        if not res['OK']:
          gLogger.error(res['Message'])
          return res
        if res['Value']['TotalRecords'] > 0:
          procpas = res['Value']['Records'][0][9]
          path += [procpas]
        else:
          return S_ERROR('This step is missing')

    if len(path) == 0:
      return S_ERROR('You have to define the input processing pass or you have to have a visible step!')
    processingid = None
    retVal = self.addProcessing(path)
    if not retVal['OK']:
      return retVal
    else:
      if len(retVal['Value']) > 0:
        processingid = retVal['Value'][0]
      else:
        return S_ERROR('The proccesing pass exist! You have to ask Zoltan!')
    retVal = self.addProductionSteps(steps, production)
    if retVal['OK']:
      sim = None
      did = None
      if daq != None:
        retVal = self._getDataTakingConditionId(daq)
        if retVal['OK'] and retVal['Value'] > -1:
          did = retVal['Value']
        else:
          return S_ERROR('Data taking condition is missing!!')
      if simcond != None:
        retVal = self._getSimulationConditioId(simcond)
        if retVal['OK'] and retVal['Value'] > -1:
          sim = retVal['Value']
        else:
          return S_ERROR('Data taking condition is missing!!')
      return self.insertproductionscontainer(production, processingid, sim, did)
    else:
      return retVal
    return S_OK('The production processing pass is entered to the bkk')


  #############################################################################
  def getEventTypes(self, configName=default, configVersion=default, prod=default):
    condition = ''
    result = S_ERROR()
    if configName != default:
      condition += " prodview.configname='%s' " % (configName)

    if configVersion != default:
      condition += " and prodview.configversion='%s' " % (configVersion)

    if prod != default:
      if condition == '':
        condition += " prodview.production=%d" % (prod)
      else:
        condition += " and prodview.production=%d" % (prod)

    command = ' select distinct prodview.eventtypeid, prodview.description from  prodview where ' + condition
    retVal = self.dbR_._query(command)
    records = []
    if retVal['OK']:
      parameters = ['EventTypeId', 'Description']
      for record in retVal['Value']:
        records += [list(record)]
      result = S_OK({'ParameterNames':parameters, 'Records':records, 'TotalRecords':len(records)})
    else:
      result = retVal
    return result

  #############################################################################
  def getProcessingPassSteps(self, procpass=default, cond=default, stepname=default):

    processing = {}
    condition = ''

    if procpass != default:
      condition += " and prod.processingid in ( \
                    select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                        FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                        CONNECT BY NOCYCLE PRIOR  id=parentid) v where v.path='%s' \
                       )" % (procpass.split('/')[1], procpass)

    if cond != default:
      retVal = self._getConditionString(cond, 'prod')
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal


    if stepname != default:
      condition += " and s.processingpass='%s' " % (stepname)

    command = "select distinct s.stepid,s.stepname,s.applicationname,s.applicationversion, s.optionfiles,s.dddb, s.conddb,s.extrapackages,s.visible, cont.step \
                from steps s, productionscontainer prod, stepscontainer cont \
               where \
              cont.stepid=s.stepid and \
              prod.production=cont.production %s order by cont.step" % (condition)

    retVal = self.dbR_._query(command)
    records = []
    #parametersNames = [ 'StepId', 'StepName','ApplicationName', 'ApplicationVersion','OptionFiles','DDDB','CONDDB','ExtraPackages','Visible']
    parametersNames = ['id', 'name']
    if retVal['OK']:
      nb = 0
      for i in retVal['Value']:
        #records = [[i[0],i[1],i[2],i[3],i[4],i[5],i[6], i[7], i[8]]]
        records = [ ['StepId', i[0]], ['StepName', i[1]], ['ApplicationName', i[2]], ['ApplicationVersion', i[3]], ['OptionFiles', i[4]], ['DDDB', i[5]], ['CONDDB', i[6]], ['ExtraPackages', i[7]], ['Visible', i[8]]]
        step = 'Step-%s' % (i[0])
        processing[step] = records
        nb += 1
    else:
      return retVal

    return S_OK({'Parameters':parametersNames, 'Records':processing, 'TotalRecords':nb})

  #############################################################################
  def getProductionProcessingPassSteps(self, prod):
    processing = {}
    retVal = self.getProductionProcessingPass(prod)
    if retVal['OK']:
      procpass = retVal['Value']

    condition = ''


    if procpass != default:
      condition += " and prod.processingid in ( \
                    select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                        FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                        CONNECT BY NOCYCLE PRIOR  id=parentid) v where v.path='%s' \
                       )" % (procpass.split('/')[1], procpass)

    command = "select distinct s.stepid,s.stepname,s.applicationname,s.applicationversion, s.optionfiles,s.dddb, s.conddb,s.extrapackages,s.visible, cont.step \
                from steps s, productionscontainer prod, stepscontainer cont \
               where \
              cont.stepid=s.stepid and \
              prod.production=cont.production %s and prod.production=%dorder by cont.step" % (condition, prod)

    retVal = self.dbR_._query(command)
    records = []
    #parametersNames = [ 'StepId', 'StepName','ApplicationName', 'ApplicationVersion','OptionFiles','DDDB','CONDDB','ExtraPackages','Visible']
    parametersNames = ['id', 'name']
    if retVal['OK']:
      nb = 0
      for i in retVal['Value']:
        #records = [[i[0],i[1],i[2],i[3],i[4],i[5],i[6], i[7], i[8]]]
        records = [ ['StepId', i[0]], ['ProcessingPass', procpass], ['ApplicationName', i[2]], ['ApplicationVersion', i[3]], ['OptionFiles', i[4]], ['DDDB', i[5]], ['CONDDB', i[6]], ['ExtraPackages', i[7]], ['Visible', i[8]]]
        step = i[1]
        processing[step] = records
        nb += 1
    else:
      return retVal

    return S_OK({'Parameters':parametersNames, 'Records':processing, 'TotalRecords':nb})

  #############################################################################
  def getRuns(self, cName, cVersion):
    return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getRuns', [cName, cVersion])

  #############################################################################
  def getRunAndProcessingPass(self, runnb):
    command = "select distinct runnumber, processingpass from table (BOOKKEEPINGORACLEDB.getRunProcPass(%d))" % (runnb)
    return self.dbR_._query(command)
    #return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getRunProcPass', [runnb])

  #############################################################################
  def getNbOfRawFiles(self, runid, eventtype):
    condition = ''
    if eventtype != default:
      condition = ' and f.eventtypeid=%d' % (eventtype)

    command = ' select count(*) from jobs j, files f where j.jobid=f.jobid and j.production<0 and j.runnumber=%d %s' % (runid, condition)
    return self.dbR_._query(command)

  #############################################################################
  def getFileTypeVersion(self, lfn):
    result = {}
    for i in lfn:
      res = self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getTypeVesrsion', [i])
      if res['OK']:
        if len(res['Value']) > 0:
          result[i] = res['Value'][0][0]
      else:
        result[i] = res['Message']
    return S_OK(result)

  #############################################################################
  def insertRuntimeProject(self, projectid, runtimeprojectid):
    result = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.insertRuntimeProject', [projectid, runtimeprojectid], False)
    return result

  #############################################################################
  def updateRuntimeProject(self, projectid, runtimeprojectid):
    result = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.updateRuntimeProject', [projectid, runtimeprojectid], False)
    return result

  def removeRuntimeProject(self, stepid):
    result = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.removeRuntimeProject', [stepid], False)
    return result

  #############################################################################
  def getTCKs(self, configName, configVersion, conddescription=default, processing=default, evt=default, production=default, filetype=default, quality=default, runnb=default):
    condition = ''
    if configName != default:
      condition += " and c.configname='%s' " % (configName)

    if configVersion != default:
      condition += " and c.configversion='%s' " % (configVersion)

    if conddescription != default:
      retVal = self._getConditionString(conddescription, 'prod')
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal

    tables = ''
    if evt != default:
      tables += ' ,prodview bview'
      condition += '  and j.production=bview.production and bview.production=prod.production and bview.eventtypeid=%s and f.eventtypeid=bview.eventtypeid ' % (evt)

    if production != default:
      condition += ' and j.production=' + str(production)

    if runnb != default:
      condition += ' and j.runnumber=' + str(runnb)

    if filetype != default:
      condition += " and ftypes.name='%s' and bview.filetypeid=ftypes.filetypeid " % (str(filetype))

    if quality != default:
      if type(quality) == types.StringType:
        command = "select QualityId from dataquality where dataqualityflag='" + str(quality) + "'"
        res = self.dbR_._query(command)
        if not res['OK']:
          gLogger.error('Data quality problem:', res['Message'])
        elif len(res['Value']) == 0:
            return S_ERROR('Dataquality is missing!')
        else:
          quality = res['Value'][0][0]
        condition += ' and f.qualityid=' + str(quality)
      else:
        if len(quality) > 0:
          conds = ' ('
          for i in quality:
            quality = None
            command = "select QualityId from dataquality where dataqualityflag='" + str(i) + "'"
            res = self.dbR_._query(command)
            if not res['OK']:
              gLogger.error('Data quality problem:', res['Message'])
            elif len(res['Value']) == 0:
                return S_ERROR('Dataquality is missing!')
            else:
              quality = res['Value'][0][0]
            conds += ' f.qualityid=' + str(quality) + ' or'
          condition += 'and' + conds[:-3] + ')'

    if processing != default:
      command = "select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='%s'" % (processing.split('/')[1], processing)
      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        return retVal
      pro = '('
      for i in retVal['Value']:
        pro += "%s," % (str(i[0]))
      pro = pro[:-1]
      pro += (')')

      condition += " and prod.processingid in %s" % (pro)

    command = "select distinct j.tck from files f, jobs j, productionscontainer prod, configurations c, dataquality d, filetypes ftypes %s  where \
    j.jobid=f.jobid and \
    d.qualityid=f.qualityid and \
    f.gotreplica='Yes' and \
    f.visibilityFlag='Y' and \
    ftypes.filetypeid=f.filetypeid and \
    j.configurationid=c.configurationid %s" % (tables, condition)
    return self.dbR_._query(command)


  #############################################################################
  def getStepsMetadata(self, configName, configVersion, cond=default, procpass=default, evt=default, production=default, filetype=default, runnb=default):
    processing = {}
    condition = ''
    tables = 'steps s, productionscontainer prod, stepscontainer cont, prodview bview'
    if configName != default:
      condition += " and bview.configname='%s' " % (configName)

    if configVersion != default:
      condition += " and bview.configversion='%s' " % (configVersion)

    if procpass != default:
      condition += " and prod.processingid in ( \
                    select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                        FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                        CONNECT BY NOCYCLE PRIOR  id=parentid) v where v.path='%s' \
                       )" % (procpass.split('/')[1], procpass)

    if cond != default:
      retVal = self._getConditionString(cond, 'prod')
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal

    if evt != default:
      condition += '  and bview.eventtypeid=%s ' % (str(evt))

    if production != default:
      condition += ' and bview.production=' + str(production)

    if runnb != default:
      tables += ' ,prodrunview rview'
      condition += ' and rview.production=bview.production and rview.runnumber=%d and bview.production<0' % (runnb)

    if filetype != default:
      tables += ', filetypes ftypes'
      condition += " and ftypes.name='%s' and bview.filetypeid=ftypes.filetypeid " % (filetype)

    command = "select distinct s.stepid,s.stepname,s.applicationname,s.applicationversion, s.optionfiles,s.dddb, s.conddb,s.extrapackages,s.visible, cont.step \
                from  %s \
               where \
              cont.stepid=s.stepid and \
              prod.production=bview.production and \
              prod.production=cont.production %s order by cont.step" % (tables, condition)

    retVal = self.dbR_._query(command)
    records = []
    #parametersNames = [ 'StepId', 'StepName','ApplicationName', 'ApplicationVersion','OptionFiles','DDDB','CONDDB','ExtraPackages','Visible']
    parametersNames = ['id', 'name']
    if retVal['OK']:
      nb = 0
      for i in retVal['Value']:
        #records = [[i[0],i[1],i[2],i[3],i[4],i[5],i[6], i[7], i[8]]]
        records = [ ['StepId', i[0]], ['StepName', i[1]], ['ApplicationName', i[2]], ['ApplicationVersion', i[3]], ['OptionFiles', i[4]], ['DDDB', i[5]], ['CONDDB', i[6]], ['ExtraPackages', i[7]], ['Visible', i[8]]]
        step = 'Step-%s' % (i[0])
        processing[step] = records
        nb += 1
    else:
      return retVal

    return S_OK({'Parameters':parametersNames, 'Records':processing, 'TotalRecords':nb})

  #############################################################################
  def getDirectoryMetadata(self, lfn):
    result = S_ERROR()
    lfn += '%'
    retVal = self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getDirectoryMetadata', [lfn])
    records = []
    if retVal['OK']:
      for i in retVal['Value']:
        records += [dict(zip(('Production', 'ConfigName','ConfigVersion','EventType','FileType','ProcessingPass','ConditionDescription'), i))]
      result = S_OK(records)
    else:
      result = retVal
    return result

