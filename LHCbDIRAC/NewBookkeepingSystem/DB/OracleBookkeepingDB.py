########################################################################
# $Id$
########################################################################
"""

"""

__RCSID__ = "$Id$"

from LHCbDIRAC.NewBookkeepingSystem.DB.IBookkeepingDB                import IBookkeepingDB
from types                                                           import *
from DIRAC                                                           import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config                         import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder                     import getDatabaseSection
from DIRAC.Core.Utilities.OracleDB                                   import OracleDB
import datetime
import types, re
global ALLOWED_ALL
ALLOWED_ALL = 2

global default
default = 'ALL'

class OracleBookkeepingDB( IBookkeepingDB ):

  #############################################################################
  def __init__( self ):
    """
    """
    super( OracleBookkeepingDB, self ).__init__()
    self.cs_path = getDatabaseSection( 'Bookkeeping/BookkeepingDB' )

    self.dbHost = ''
    result = gConfig.getOption( self.cs_path + '/LHCbDIRACBookkeepingTNS' )
    if not result['OK']:
      gLogger.error( 'Failed to get the configuration parameters: Host' )
      return
    self.dbHost = result['Value']

    self.dbUser = ''
    result = gConfig.getOption( self.cs_path + '/LHCbDIRACBookkeepingUser' )
    if not result['OK']:
      gLogger.error( 'Failed to get the configuration parameters: User' )
      return
    self.dbUser = result['Value']

    self.dbPass = ''
    result = gConfig.getOption( self.cs_path + '/LHCbDIRACBookkeepingPassword' )
    if not result['OK']:
      gLogger.error( 'Failed to get the configuration parameters: User' )
      return
    self.dbPass = result['Value']


    self.dbServer = ''
    result = gConfig.getOption( self.cs_path + '/LHCbDIRACBookkeepingServer' )
    if not result['OK']:
      gLogger.error( 'Failed to get the configuration parameters: User' )
      return
    self.dbServer = result['Value']

    self.dbW_ = OracleDB( self.dbServer, self.dbPass, self.dbHost )
    self.dbR_ = OracleDB( self.dbUser, self.dbPass, self.dbHost )

  #############################################################################
  def __isSpecialFileType( self, flist ):
    if ( 'RAW' in flist ) or ( 'MDF' in flist ):
      return True
    else:
      for i in flist:
        if re.search( 'HIST', i ):
          return True
    return False

  #############################################################################
  def getAvailableSteps( self, dict = {} ):
    condition = ''
    selection = 'stepid,stepname, applicationname,applicationversion,optionfiles,DDDB,CONDDB, extrapackages,Visible, ProcessingPass, Usable'
    if len(dict) > 0:
      tables = 'steps'
      if dict.has_key( 'StartDate' ):
        condition += ' steps.inserttimestamps >= TO_TIMESTAMP (\'' + dict['StartDate'] + '\',\'YYYY-MM-DD HH24:MI:SS\')'
      if dict.has_key( 'StepId' ):
        if len( condition ) > 0:
          condition += ' and '
        condition += ' stepid=' + str( dict['StepId'] )
      if dict.has_key( 'StepName' ):
        if len( condition ) > 0:
          condition += ' and '
        condition += " stepname='%s'" % ( dict['StepName'] )

      if dict.has_key( 'InputFileTypes' ):
        flist = dict['InputFileTypes']
        flist.sort()
        if self.__isSpecialFileType( flist ):
          return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getAvailebleStepsRealAndMC', [], True, flist )
        else:
          return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getAvailebleSteps', [], True, flist )
      command = 'select ' + selection + ' from ' + tables + ' where ' + condition + 'order by inserttimestamps desc'
      return self.dbR_._query( command )
    else:
      command = 'select ' + selection + ' from steps order by inserttimestamps desc'
      return self.dbR_._query( command )

  #############################################################################
  def getStepInputFiles( self, StepId ):
    command = 'select inputFiletypes.name,inputFiletypes.visible from steps, table(steps.InputFileTypes) inputFiletypes where  steps.stepid=' + str( StepId )
    return self.dbR_._query( command )

  #############################################################################
  def setStepInputFiles( self, stepid, files ):
    files = sorted( files, key = lambda k: k['FileType'] )
    if len( files ) == 0:
      values = 'null'
    else:
      values = 'filetypesARRAY('
      for i in files:
        if i.has_key( 'FileType' ) and i.has_key( 'Visible' ):
          values += 'ftype(\'' + i['FileType'] + '\',\'' + i['Visible'] + '\'),'
      values = values[:-1]
      values += ')'
    command = 'update steps set inputfiletypes=' + values + ' where stepid=' + str( stepid )
    return self.dbW_._query( command )

  #############################################################################
  def setStepOutputFiles( self, stepid, files ):
    files = sorted( files, key = lambda k: k['FileType'] )
    if len( files ) == 0:
      values = 'null'
    else:
      values = 'filetypesARRAY('
      for i in files:
        if i.has_key( 'FileType' ) and i.has_key( 'Visible' ):
          values += 'ftype(\'' + i['FileType'] + '\',\'' + i['Visible'] + '\'),'
      values = values[:-1]
      values += ')'
    command = 'update steps set Outputfiletypes=' + values + ' where stepid=' + str( stepid )
    return self.dbW_._query( command )

  #############################################################################
  def getStepOutputFiles( self, StepId ):
    command = 'select outputfiletypes.name,outputfiletypes.visible from steps, table(steps.outputfiletypes) outputfiletypes where  steps.stepid=' + str( StepId )
    return self.dbR_._query( command )

  #############################################################################
  def getProductionOutputFiles( self, prod ):
    command = "select o.name,o.visible from steps s, table(s.outputfiletypes) o, stepscontainer st \
            where st.stepid=s.stepid and st.production="+str(prod)
    retVal = self.dbR_._query(command)
    result = {}
    if retVal['OK']:
      for i in retVal['Value']:
        result[i[0]] = i[1]
    else:
      return retVal
    return S_OK( result )

  #############################################################################
  def getAvailableFileTypes( self ):
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getAvailableFileTypes', [] )

  #############################################################################
  def insertFileTypes( self, ftype, desc ):
    return self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.insertFileTypes', LongType, [ftype, desc, 'ROOT_All'] )

  #############################################################################
  def insertStep( self, dict ):
    values = ''
    command = "SELECT applications_index_seq.nextval from dual"
    sid = 0
    retVal = self.dbR_._query( command )
    if not retVal['OK']:
      return retVal
    else:
      sid = retVal['Value'][0][0]
    selection = 'insert into steps(stepid,stepname,applicationname,applicationversion,OptionFiles,dddb,conddb,extrapackages,visible, processingpass, usable '
    if dict.has_key('InputFileTypes'):
      inputf = dict['InputFileTypes']
      inputf = sorted( inputf, key = lambda k: k['FileType'] )
      values = ',filetypesARRAY('
      selection += ',InputFileTypes'
      for i in inputf:
        if i.has_key( 'FileType' ) and i.has_key( 'Visible' ):
          values += 'ftype(\'' + i['FileType'] + '\',\'' + i['Visible'] + '\'),'
      values = values[:-1]
      values += ')'
    if dict.has_key( 'OutputFileTypes' ):
      outputf = dict['OutputFileTypes']
      outputf = sorted( outputf, key = lambda k: k['FileType'] )
      values += ' , filetypesARRAY('
      selection += ',OutputFileTypes'
      for i in outputf:
        if i.has_key( 'FileType' ) and i.has_key( 'Visible' ):
          values += 'ftype(\'' + i['FileType'] + '\',\'' + i['Visible'] + '\'),'
      values = values[:-1]
      values += ')'

    if dict.has_key( 'Step' ):
      step = dict['Step']
      command = selection + ")values(%d" % ( sid )
      if step.has_key( 'StepName' ):
        command += ",'%s'" % ( step['StepName'] )
      else:
        command += ',NULL'
      if step.has_key( 'ApplicationName' ):
        command += ",'%s'" % ( step['ApplicationName'] )
      else:
        command += ',NULL'
      if step.has_key( 'ApplicationVersion' ):
        command += ",'%s'" % ( step['ApplicationVersion'] )
      else:
        command += ',NULL'
      if step.has_key( 'OptionFiles' ):
        command += ",'%s'" % ( step['OptionFiles'] )
      else:
        command += ',NULL'
      if step.has_key( 'DDDB' ):
        command += ",'%s'" % ( step['DDDB'] )
      else:
        command += ',NULL'
      if step.has_key( 'CONDDB' ):
        command += ",'%s'" % ( step['CONDDB'] )
      else:
        command += ',NULL'
      if step.has_key( 'ExtraPackages' ):
        command += ",'%s'" % ( step['ExtraPackages'] )
      else:
        command += ',NULL'
      if step.has_key( 'Visible' ):
        command += ",'%s'" % ( step['Visible'] )
      else:
        command += ',NULL'

      if step.has_key('ProcessingPass'):
        command += ",'%s'"%(step['ProcessingPass'])
      else:
        command += ',NULL'

      if step.has_key('Usable'):
        command += ",'%s'"%(step['Usable'])
      else:
        command += ",'Not ready'"

      command += values+")"
      retVal = self.dbW_._query(command)
      if retVal['OK']:
        return S_OK( sid )
      else:
        return retVal

    return S_ERROR( 'The Filetypes and Step are missing!' )

  #############################################################################
  def deleteStep( self, stepid ):
    command = 'delete steps where stepid=' + str( stepid )
    return self.dbW_._query( command )

  #############################################################################
  def deleteSetpContiner(self, prod):
    result = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.deleteSetpContiner',[prod], False)
    return result

  #############################################################################
  def deleteProductionsContiner(self, prod):
    result = self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.deleteProductionsCont',[prod], False)
    return result

  #############################################################################
  def updateStep( self, dict ):
    if dict.has_key( 'StepId' ):
      stepid = dict.pop( 'StepId' )
      condition = ' where stepid=' + str( stepid )
      command = 'update steps set '
      for i in dict:
        if type( dict[i] ) == types.StringType:
          command += i + '=\'' + str( dict[i] ) + '\','
        else:
          if len( dict[i] ) > 0:
            values = 'filetypesARRAY('
            ftypes = dict[i]
            ftypes = sorted( ftypes, key = lambda k: k['FileType'] )
            for j in ftypes:
              if j.has_key( 'FileType' ) and j.has_key( 'Visible' ):
                values += 'ftype(\'' + j['FileType'] + '\',\'' + j['Visible'] + '\'),'
            values = values[:-1]
            values += ')'
            command += i + '=' + values + ','
          else:
            command += i + '=null,'
      command = command[:-1]
      command += condition
      return self.dbW_._query( command )

    else:
      return S_ERROR( 'Please give a StepId!' )
    return S_ERROR()

  #############################################################################
  def getAvailableConfigNames( self ):
    command = ' select distinct Configname from prodview'
    return self.dbR_._query( command )

  ##############################################################################
  def getAvailableConfigurations( self ):
    """
    """
    return self.dbR_.executeStoredProcedure( 'BKK_ORACLE.getAvailableConfigurations', [] )


  #############################################################################
  def getConfigVersions( self, configname ):
    command = ' select distinct configversion from prodview where configname=\'' + configname + '\''
    return self.dbR_._query( command )

  #############################################################################
  def getConditions( self, configName, configVersion, evt ):

    condition = ''
    if configName != default:
      condition += " and prodview.configname='%s' " % ( configName )

    if configVersion != default:
      condition += " and prodview.configversion='%s' " % ( configVersion )


    if evt != default:
      condition += " and prodview.eventtypeid=" + str( evt )

    command = 'select distinct simulationConditions.SIMID,data_taking_conditions.DAQPERIODID,simulationConditions.SIMDESCRIPTION, simulationConditions.BEAMCOND, \
    simulationConditions.BEAMENERGY, simulationConditions.GENERATOR,simulationConditions.MAGNETICFIELD,simulationConditions.DETECTORCOND, simulationConditions.LUMINOSITY, \
    data_taking_conditions.DESCRIPTION,data_taking_conditions.BEAMCOND, data_taking_conditions.BEAMENERGY,data_taking_conditions.MAGNETICFIELD, \
    data_taking_conditions.VELO,data_taking_conditions.IT, data_taking_conditions.TT,data_taking_conditions.OT,data_taking_conditions.RICH1,data_taking_conditions.RICH2, \
    data_taking_conditions.SPD_PRS, data_taking_conditions.ECAL, data_taking_conditions.HCAL, data_taking_conditions.MUON, data_taking_conditions.L0, data_taking_conditions.HLT,\
     data_taking_conditions.VeloPosition from simulationConditions,data_taking_conditions,prodview where \
      prodview.simid=simulationConditions.simid(+) and \
      prodview.DAQPERIODID=data_taking_conditions.DAQPERIODID(+)'+condition


    return self.dbR_._query(command)

  #############################################################################
  def getProcessingPass(self, configName, configVersion, conddescription, runnumber, production, path='/'):

    erecords = []
    eparameters = []
    precords = []
    pparameters = []

    condition = ''
    if configName != default:
      condition += " and prodview.configname='%s' " % ( configName )

    if configVersion != default:
      condition += " and prodview.configversion='%s' " % ( configVersion )

    if conddescription != default:
      retVal = self._getConditionString( conddescription )
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal
    if production != default:
      condition += ' and prodview.production=' + str( production )
    tables = ''
    if runnumber != default:
      tables += ' , prodrunview '
      condition += ' and prodrunview.production=prodview.production and prodrunview.runnumber=%s' % ( str( runnumber ) )

    proc = path.split( '/' )[len( path.split( '/' ) ) - 1]
    if proc != '':
      command = "select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='%s'"%(path.split('/')[1], path)
      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        return retVal
      pro = ''
      for i in retVal['Value']:
        pro += "%s," % ( str( i[0] ) )
      pro = pro[:-1]

      if pro == '':
        return S_ERROR( 'Empty Directory' )
      command = 'select distinct eventTypes.EventTypeId, eventTypes.Description from eventtypes,prodview,productionscontainer,processing %s where \
        prodview.production=productionscontainer.production and \
        eventTypes.EventTypeId=prodview.eventtypeid and \
        productionscontainer.processingid=processing.id and \
        processing.id in (%s) %s'% (tables, pro, condition )

      retVal = self.dbR_._query(command)
      if retVal['OK']:
        eparameters = ['EventTypeId', 'Description']
        for record in retVal['Value']:
          erecords += [[record[0], record[1]]]
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

    return S_OK( [{'ParameterNames':pparameters, 'Records':precords, 'TotalRecords':len( precords )}, {'ParameterNames':eparameters, 'Records':erecords, 'TotalRecords':len( erecords )}] )

  #############################################################################
  def getStandardProcessingPass( self, configName = default, configVersion = default, conddescription = default, eventType = default, production = default, path = '/' ):

    records = []
    parameters = []

    condition = ''
    if configName != default:
      condition += " and prodview.configname='%s' " % ( configName )

    if configVersion != default:
      condition += " and prodview.configversion='%s' " % ( configVersion )


    if eventType != default:
      condition += " and prodview.eventtypeid=" + str( eventType )

    if production != default:
      condition += " and prodview.production=" + str( production )

    if conddescription != default:
      retVal = self._getConditionString( conddescription )
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal

    proc = path.split( '/' )[len( path.split( '/' ) ) - 1]
    if proc != '':
      command = 'SELECT distinct name \
      FROM processing   where parentid in (select id from  processing where name=\''+str(proc)+'\') START WITH id in (select distinct productionscontainer.processingid from productionscontainer,prodview where \
      productionscontainer.production=prodview.production ' + condition +')  CONNECT BY NOCYCLE PRIOR  parentid=id order by name desc'
    else:
      command = 'SELECT distinct name \
      FROM processing   where parentid is null START WITH id in (select distinct productionscontainer.processingid from productionscontainer,prodview where \
      productionscontainer.production=prodview.production' + condition +') CONNECT BY NOCYCLE PRIOR  parentid=id order by name desc'
    retVal = self.dbR_._query(command)
    if retVal['OK']:
      records = []
      parameters = ['Name']
      for record in retVal['Value']:
        records += [[record[0]]]
    else:
      return retVal
    return S_OK( [{'ParameterNames':parameters, 'Records':records, 'TotalRecords':len( records )}] )


  #############################################################################
  def _getConditionString( self, conddescription, table = 'productionscontainer' ):
    condition = ''
    retVal = self._getDataTakingConditionId( conddescription )
    if retVal['OK']:
      if retVal['Value'] != -1:
        condition += " and %s.DAQPERIODID=%s and %s.DAQPERIODID is not null " % ( table, str( retVal['Value'] ), table )
      else:
        retVal = self._getSimulationConditioId( conddescription )
        if retVal['OK']:
          if retVal['Value'] != -1:
            condition += " and %s.simid=%s and %s.simid is not null " % ( table, str( retVal['Value'] ), table )
          else:
            return S_ERROR( 'Condition does not exists!' )
        else:
          return retVal
    else:
      return retVal
    return S_OK( condition )

  #############################################################################
  def _getDataTakingConditionId( self, desc ):
    command = 'select DAQPERIODID from data_taking_conditions where DESCRIPTION=\'' + str( desc ) + '\''
    retVal = self.dbR_._query( command )
    if retVal['OK']:
      if len( retVal['Value'] ) > 0:
        return S_OK( retVal['Value'][0][0] )
      else:
        return S_OK( -1 )
    else:
      return retVal

  #############################################################################
  def _getSimulationConditioId( self, desc ):
    command = 'select simid from simulationconditions where simdescription=\'' + str( desc ) + '\''
    retVal = self.dbR_._query( command )
    if retVal['OK']:
      if len( retVal['Value'] ) > 0:
        return S_OK( retVal['Value'][0][0] )
      else:
        return S_OK( -1 )
    else:
      return retVal

  #############################################################################
  def getProductions( self, configName = default, configVersion = default, conddescription = default, processing = default, evt = default ):

    condition = ''
    if configName != default:
      condition += " and bview.configname='%s'" % ( configName )
    if configVersion != default:
      condition += " and bview.configversion='%s'" % ( configVersion )

    if conddescription != default:
      retVal = self._getConditionString( conddescription, 'pcont' )
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal

    if evt != default:
      condition += ' and bview.eventtypeid=' + str( evt )

    if processing != default:
      command = "select distinct pcont.production from \
                 productionscontainer pcont,prodview bview \
                 where pcont.processingid in \
                    (select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='"+str(processing.split('/')[1])+"') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='"+processing+"') \
                  and bview.production=pcont.production "+condition
    else:
      command = "select distinct pcont.production from productionscontainer pcont,prodview bview where \
                 bview.production=pcont.production "+condition
    return self.dbR_._query(command)

  #############################################################################
  def getFileTypes( self, configName, configVersion, conddescription = default, processing = default, evt = default, runnb = default, production = default ):

    condition = ''

    if configName != default:
      condition += " and bview.configname='%s' " % ( configName )

    if configVersion != default:
      condition += " and bview.configversion='%s' " % ( configVersion )

    if conddescription != default:
      retVal = self._getConditionString( conddescription, 'pcont' )
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal

    if evt != default:
      condition += ' and bview.eventtypeid=' + str( evt )

    if production != default:
      condition += ' and bview.production=' + str( production )

    tables = ''
    if runnb != default:
      tables = ' , prodrunview prview'
      condition += ' and prview.production=bview.production and prview.runnumber=' + str( runnb )


    if processing != default:
      command = "select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='%s'"%(processing.split('/')[1], processing)
      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        return retVal
      pro = '('
      for i in retVal['Value']:
        pro += "%s," % ( str( i[0] ) )
      pro = pro[:-1]
      pro += ( ')' )
      command = "select distinct ftypes.name from \
                 productionscontainer pcont,prodview bview, filetypes ftypes  %s \
                 where pcont.processingid in %s \
                  and bview.production=pcont.production and bview.filetypeId=ftypes.filetypeid  %s"%(tables, pro, condition)
    else:
      command = "select distinct ftypes.name  from productionscontainer pcont, prodview bview,  filetypes ftypes %s where \
                 bview.production=pcont.production and bview.filetypeId=ftypes.filetypeid %s" % ( tables, condition )
    return self.dbR_._query(command)

  #############################################################################
  def getFiles( self, configName, configVersion, conddescription = default, processing = default, evt = default, production = default, filetype = default, quality = default, runnb = default ):
    condition = ''
    if configName != default:
      condition += " and c.configname='%s' " % ( configName )

    if configVersion != default:
      condition += " and c.configversion='%s' " % ( configVersion )

    if conddescription != default:
      retVal = self._getConditionString( conddescription, 'prod' )
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal

    tables = ''
    if evt != default:
      tables += ' ,prodview bview'
      condition += '  and j.production=bview.production and bview.production=prod.production and bview.eventtypeid=%s and f.eventtypeid=bview.eventtypeid '%(evt)

    if production != default:
      condition += ' and j.production=' + str( production )

    if runnb != default:
      condition += ' and j.runnumber=' + str( runnb )

    if filetype != default:
      condition += " and ftypes.name='%s' and bview.filetypeid=ftypes.filetypeid " % (str( filetype ))

    if quality != default:
      if type( quality ) == types.StringType:
        command = "select QualityId from dataquality where dataqualityflag='" + str( quality ) + "'"
        res = self.dbR_._query( command )
        if not res['OK']:
          gLogger.error( 'Data quality problem:', res['Message'] )
        elif len( res['Value'] ) == 0:
            return S_ERROR( 'Dataquality is missing!' )
        else:
          quality = res['Value'][0][0]
        condition += ' and f.qualityid=' + str( quality )
      else:
        if len( quality ) > 0:
          conds = ' ('
          for i in quality:
            quality = None
            command = "select QualityId from dataquality where dataqualityflag='" + str( i ) + "'"
            res = self.dbR_._query( command )
            if not res['OK']:
              gLogger.error( 'Data quality problem:', res['Message'] )
            elif len( res['Value'] ) == 0:
                return S_ERROR( 'Dataquality is missing!' )
            else:
              quality = res['Value'][0][0]
            conds += ' f.qualityid=' + str( quality ) + ' or'
          condition += 'and' + conds[:-3] + ')'

    if processing != default:
      command = "select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='%s'"%(processing.split('/')[1], processing)
      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        return retVal
      pro = '('
      for i in retVal['Value']:
        pro += "%s," % ( str( i[0] ) )
      pro = pro[:-1]
      pro += ( ')' )

      condition += " and prod.processingid in %s" % ( pro )

    command = "select distinct f.FileName, f.EventStat, f.FileSize, f.CreationDate, j.JobStart, j.JobEnd, j.WorkerNode, ftypes.Name, j.runnumber, j.fillnumber, f.fullstat, d.dataqualityflag, \
    j.eventinputstat, j.totalluminosity, f.luminosity, f.instLuminosity, j.tck from files f, jobs j, productionscontainer prod, configurations c, dataquality d, filetypes ftypes %s  where \
    j.jobid=f.jobid and \
    d.qualityid=f.qualityid and \
    f.gotreplica='Yes' and \
    f.visibilityFlag='Y' and \
    ftypes.filetypeid=f.filetypeid and \
    j.configurationid=c.configurationid %s" %(tables,condition)
    return self.dbR_._query(command)

  #############################################################################
  def getAvailableDataQuality(self):
    command = ' select dataqualityflag from dataquality'
    retVal = self.dbR_._query( command )
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )
    flags = retVal['Value']
    result = []
    for i in flags:
      result += [i[0]]
    return S_OK( result )

  #############################################################################
  def getAvailableProductions( self ):
    command = ' select distinct production from prodview where production > 0'
    res = self.dbR_._query( command )
    return res

  #############################################################################
  def getAvailableRuns( self ):
    command = ' select distinct runnumber from prodrunview'
    res = self.dbR_._query( command )
    return res

  #############################################################################
  def getAvailableEventTypes( self ):
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getAvailableEventTypes', [] )

  #############################################################################
  def getProductionProcessingPass( self, prodid ):
    return self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getProductionProcessingPass', StringType, [prodid] )

  #############################################################################
  def getRunProcessingPass( self, runnumber ):
    return self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getProductionProcessingPass', StringType, [-1 * runnumber] )

  #############################################################################
  def getProductionProcessingPassID( self, prodid ):
    return self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getProductionProcessingPassId', LongType, [prodid] )

  #############################################################################
  def getMoreProductionInformations( self, prodid ):
    command = 'select prodview.configname, prodview.configversion, prodview.ProgramName, prodview.programversion from \
    prodview  where prodview.production='+str(prodid)
    res = self.dbR_._query(command)
    if not res['OK']:
      return S_ERROR( res['Message'] )
    else:
      record = res['Value']
      cname = record[0][0]
      cversion = record[0][1]
      pname = record[0][2]
      pversion = record[0][3]


    retVal = self.getProductionProcessingPass( prodid )
    if retVal['OK']:
      procdescription = retVal['Value']
    else:
      return retVal

    simdesc = None
    daqdesc = None

    command = "select distinct sim.simdescription, daq.description from simulationconditions sim, data_taking_conditions daq,productionscontainer prod \
              where sim.simid(+)=prod.simid and daq.daqperiodid(+)=prod.daqperiodid and prod.production="+str(prodid)
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return retVal
    else:
      value = retVal['Value']
      if len( value ) != 0:
        simdesc = value[0][0]
        daqdesc = value[0][1]
      else:
        return S_ERROR( 'Simulation condition or data taking condition not exist!' )
    if simdesc != None:
      return S_OK( {'ConfigName':cname, 'ConfigVersion':cversion, 'ProgramName':pname, 'ProgramVersion':pversion, 'Processing pass':procdescription, 'Simulation conditions':simdesc} )
    else:
      return S_OK( {'ConfigName':cname, 'ConfigVersion':cversion, 'ProgramName':pname, 'ProgramVersion':pversion, 'Processing pass':procdescription, 'Data taking conditions':daqdesc} )


  #############################################################################
  def getJobInfo( self, lfn ):
    return self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getJobInfo', [lfn] )

  #############################################################################
  def getRunNumber( self, lfn ):
    return self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getRunNumber', LongType, [lfn] )

  #############################################################################
  def getRunNbAndTck(self, lfn):
    return self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getRunNbAndTck',[lfn])


  #############################################################################
  def getProductionFiles(self, prod, ftype, gotreplica='ALL'):
    command = ''
    value = {}
    condition = ''
    if gotreplica != 'ALL':
      condition += 'and files.gotreplica=\'' + str( gotreplica ) + '\''

    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\'' + str( ftype ) + '\''
      res = self.dbR_._query( fileType )
      if not res['OK']:
        gLogger.error( res['Message'] )
        return S_ERROR( 'Oracle error' + res['Message'] )
      else:
        if len( res['Value'] ) == 0:
          return S_ERROR( 'File Type not found:' + str( ftype ) )

        ftypeId = res['Value'][0][0]
        command = 'select files.filename, files.gotreplica, files.filesize,files.guid, \'' + ftype + '\', files.inserttimestamp from jobs,files where jobs.jobid=files.jobid and files.filetypeid=' + str( ftypeId ) + ' and jobs.production=' + str( prod ) + condition
    else:
      command = 'select files.filename, files.gotreplica, files.filesize,files.guid, filetypes.name, files.inserttimestamp from jobs,files,filetypes where jobs.jobid=files.jobid and files.filetypeid=filetypes.filetypeid and jobs.production=' + str( prod ) + condition

    res = self.dbR_._query( command )
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        value[record[0]] = {'GotReplica':record[1], 'FileSize':record[2], 'GUID':record[3], 'FileType':record[4]}
    else:
      return S_ERROR( res['Message'] )
    return S_OK( value )

  #############################################################################
  def getFilesForAGivenProduction( self, dict ):
    condition = ''
    if dict.has_key( 'Production' ):
      prod = dict['Production']
      condition = ' and jobs.production=' + str( prod )
    else:
      return S_ERROR( 'You need to give a production number!' )
    if dict.has_key( 'FileType' ):
      ftype = dict['FileType']
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\'' + str( ftype ) + '\''
      res = self.dbR_._query( fileType )
      if not res['OK']:
        gLogger.error( res['Message'] )
        return S_ERROR( 'Oracle error' + res['Message'] )
      else:
        if len( res['Value'] ) == 0:
          return S_ERROR( 'File Type not found:' + str( ftype ) )

        ftypeId = res['Value'][0][0]
        condition += ' and files.filetypeid=' + str( ftypeId )

    if dict.has_key( 'Replica' ):
      gotreplica = dict['Replica']
      condition += 'and files.gotreplica=\''+str(gotreplica)+'\''

    command = ''
    tables = ''
    if dict.has_key( 'DataQuality' ):
      tables = ', dataquality'
      quality = dict['DataQuality']
      if type( quality ) == types.ListType:
        condition += ' and '
        cond = ' ( '
        for i in quality:
          cond += 'dataquality.dataqualityflag=\'' + str( i ) + '\' and files.qualityId=dataquality.qualityid or '
        cond = cond[:-3] + ')'
        condition += cond
      else:
        condition += ' and dataquality.dataqualityflag=\'' + str( quality ) + '\' and files.qualityId=dataquality.qualityid '

    value = {}
    command = 'select files.filename, files.gotreplica, files.filesize,files.guid, filetypes.name from jobs,files,filetypes' + tables + ' where jobs.jobid=files.jobid and files.filetypeid=filetypes.filetypeid ' + condition

    res = self.dbR_._query( command )
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        value[record[0]] = {'GotReplica':record[1], 'FileSize':record[2], 'GUID':record[3], 'FileType':record[4]}
    else:
      return S_ERROR( res['Message'] )
    return S_OK( value )

  #############################################################################
  def getAvailableRunNumbers( self ):
    command = 'select distinct runnumber from prodrunview'
    res = self.dbR_._query( command )
    return res

  #############################################################################
  def getRunFiles( self, runid ):
    value = {}
    command = 'select files.filename, files.gotreplica, files.filesize,files.guid from jobs,files where jobs.jobid=files.jobid and files.filetypeid=23 and jobs.runnumber=' + str( runid )
    res = self.dbR_._query( command )
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        value[record[0]] = {'GotReplica':record[1], 'FileSize':record[2], 'GUID':record[3]}
    else:
      return S_ERROR( res['Message'] )
    return S_OK( value )

  #############################################################################
  def updateFileMetaData( self, filename, filesAttr ):
    utctime = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
    command = 'update files Set inserttimestamp=TO_TIMESTAMP(\'' + str( utctime ) + '\',\'YYYY-MM-DD HH24:MI:SS\') ,'
    for attribute in filesAttr.keys():
      if type( filesAttr[attribute] ) == types.StringType:
        command += str( attribute ) + '=\'' + str( filesAttr[attribute] ) + '\' ,'
      else:
        command += str( attribute ) + '=' + str( filesAttr[attribute] ) + ' ,'

    command = command[:-1]
    command += ' where fileName=\'' + filename + '\''
    res = self.dbW_._query( command )
    return res


  #############################################################################
  def renameFile( self, oldLFN, newLFN ):
    utctime = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
    command = ' update files Set inserttimestamp=TO_TIMESTAMP(\'' + str( utctime ) + '\',\'YYYY-MM-DD HH24:MI:SS\'), fileName = \'' + newLFN + '\' where filename=\'' + oldLFN + '\''
    res = self.dbW_._query( command )
    return res

  #############################################################################
  def getInputAndOutputJobFiles( self, jobids ):
    list = {}
    for jobid in jobids:
      tmp = {}
      res = self.getInputFiles( jobid )

      if not res['OK']:
        return S_ERROR( res['Message'] )
      input = res['Value']
      inputs = []
      for lfn in input:
        inputs += [lfn]

      res = self.getOutputFiles( jobid )
      if not res['OK']:
        return S_ERROR( res['Message'] )
      output = res['Value']
      outputs = []
      for lfn in output:
        if lfn not in inputs:
          outputs += [lfn]
      tmp = {'InputFiles':inputs, 'OutputFiles':outputs}
      list[jobid] = tmp
    return S_OK( list )

  #############################################################################
  def getInputFiles( self, jobid ):
    command = ' select files.filename from inputfiles,files where files.fileid=inputfiles.fileid and inputfiles.jobid=' + str( jobid )
    res = self.dbR_._query( command )
    return res

  #############################################################################
  def getOutputFiles( self, jobid ):
    command = ' select files.filename from files where files.jobid =' + str( jobid )
    res = self.dbR_._query( command )
    return res

  #############################################################################
  def getJobsIds( self, filelist ):
    list = {}
    for file in filelist:
      res = self.getJobInfo( file )
      if not res['OK']:
        return S_ERROR( res['Message'] )
      dbResult = res['Value']
      for record in dbResult:
        jobid = str( record[16] )
        value = {'DiracJobID':record[0], 'DiracVersion':record[1], 'EventInputStat':record[2], 'ExecTime':record[3], 'FirstEventNumber':record[4], \
                  'Location':record[5], 'Name':record[6], 'NumberofEvents':record[7], \
                  'StatistivsRequested':record[8], 'WNCPUPOWER':record[9], 'CPUTIME':record[10], 'WNCACHE':record[11], 'WNMEMORY':record[12], 'WNMODEL':record[13], 'WORKERNODE':record[14], 'WNCPUHS06':record[15], 'TotalLuminosity':record[17]}
      list[jobid] = value
    return S_OK( list )

  #############################################################################
  def insertTag( self, name, tag ):
    return self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.insertTag', [name, tag], False )

  #############################################################################
  def existsTag( self, name, value ): #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    command = 'select count(*) from tags where name=\'' + str( name ) + '\' and tag=\'' + str( value ) + '\''
    retVal = self.dbR_._query( command )
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )
    elif retVal['Value'][0][0] > 0:
      return S_OK( True )
    return S_OK( False )

  #############################################################################
  def setQuality( self, lfns, flag ):
    result = {}
    command = ' select qualityid from dataquality where dataqualityflag=\''+str(flag)+'\''
    retVal = self._getDataQualityId(flag)
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )
    else:
      qid = retVal['Value']

    failed = []
    succ = []
    for lfn in lfns:
      retVal = self.__updateQualityFlag( lfn, qid )
      if not retVal['OK']:
        failed += [lfn]
        gLogger.error( retVal['Message'] )
      else:
        succ += [lfn]
    result['Successful'] = succ
    result['Failed'] = failed
    return S_OK( result )

  #############################################################################
  def  _getProcessingPassId( self, root, fullpath ):
    return  self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getProcessingPassId', LongType, [root, fullpath] )

  #############################################################################
  def getProcessingPassId(self, fullpath):
    return self._getProcessingPassId(fullpath.split('/')[1:][0], fullpath)

  #############################################################################
  def _getDataQualityId( self, name ):
    return  self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getDataQualityId', LongType, [name] )

  #############################################################################
  def setRunQualityWithProcessing( self, runNB, procpass, flag ):
    retVal = self._getProcessingPassId( procpass.split( '/' )[1:][0], procpass )
    if retVal['OK']:
      processingid = retVal['Value']
      retVal = self._getDataQualityId( flag )
      if retVal['OK']:
        flag = retVal['Value']
        return self.dbW_.executeStoredProcedure('BOOKKEEPINGORACLEDB.insertRunquality',[runNB, flag, processingid], False)
      else:
        return retVal
    else:
      return retVal

  #############################################################################
  def setQualityRun( self, runNb, flag ):
    command = 'select distinct j.runnumber from  jobs j, productionscontainer prod where \
    j.production=prod.production and \
    j.production<0 and \
    j.runnumber='+str(runNb)
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )
    value = retVal['Value']
    if len( value ) == 0:
      return S_ERROR( 'This ' + str( runNb ) + ' run is missing in the BKK DB!' )

    retVal = self._getDataQualityId( flag )
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )

    qid = retVal['Value']

    utctime = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    command = ' update files set inserttimestamp=TO_TIMESTAMP(\''+str(utctime)+'\',\'YYYY-MM-DD HH24:MI:SS\'), qualityId='+str(qid)+' where fileid in ( select files.fileid from jobs, files where jobs.jobid=files.jobid and \
      jobs.runnumber='+str(runNb)+')'
    retVal = self.dbW_._query(command)
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )

    command = 'select files.filename from jobs, files where jobs.jobid=files.jobid and \
      jobs.runnumber='+str(runNb)

    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )

    succ = []
    records = retVal['Value']
    for record in records:
      succ += [record[0]]
    result = {}
    result['Successful'] = succ
    result['Failed'] = []
    return S_OK( result )

  #############################################################################
  def setQualityProduction( self, prod, flag ):
    command = 'select distinct jobs.production  from jobs where jobs.production=' + str( prod )
    retVal = self.dbR_._query( command )
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )
    value = retVal['Value']
    if len( value ) == 0:
      return S_ERROR( 'This ' + str( prod ) + ' production is missing in the BKK DB!' )

    retVal = self._getDataQualityId( flag )
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )

    qid = retVal['Value']

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
      return S_ERROR( retVal['Message'] )

    succ = []
    records = retVal['Value']
    for record in records:
      succ += [record[0]]
    result = {}
    result['Successful'] = succ
    result['Failed'] = []
    return S_OK( result )

  #############################################################################
  def __updateQualityFlag( self, lfn, qid ):
    utctime = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
    command = 'update files set inserttimestamp=TO_TIMESTAMP(\'' + str( utctime ) + '\',\'YYYY-MM-DD HH24:MI:SS\'), qualityId=' + str( qid ) + ' where filename=\'' + str( lfn ) + '\''
    retVal = self.dbW_._query( command )
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )
    else:
      return S_OK( 'Quality flag has been updated!' )

  #############################################################################
  def getLFNsByProduction( self, prodid ):
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getLFNsByProduction', [prodid] )

  #############################################################################
  def getAncestors( self, lfn, depth ):
    logicalFileNames = {}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    jobsId = []
    job_id = -1
    if depth < 1:
      return S_OK( {'Failed:':lfn} )
    odepth = depth
    gLogger.debug( 'original', lfn )
    for fileName in lfn:
      depth = odepth
      jobsId = []
      gLogger.debug( 'filename', fileName )
      jobsId = []
      result = self.dbR_.executeStoredFunctions( 'BKK_MONITORING.getJobId', LongType, [fileName] )
      if not result["OK"]:
        gLogger.error( 'Ancestor', result['Message'] )
      else:
        job_id = int( result['Value'] )
      if job_id != 0:
        jobsId = [job_id]
        files = []
        while ( depth - 1 ) and jobsId:
          for job_id in jobsId:
            command = 'select files.fileName,files.jobid, files.gotreplica from inputfiles,files where inputfiles.fileid=files.fileid and inputfiles.jobid=' + str( job_id )
            jobsId = []
            res = self.dbR_._query( command )
            if not res['OK']:
              gLogger.error( 'Ancestor', result["Message"] )
            else:
              dbResult = res['Value']
              for record in dbResult:
                jobsId += [record[1]]
                if record[2] != 'No':
                  files += [record[0]]
          depth -= 1

        ancestorList[fileName] = files
      else:
        logicalFileNames['Failed'] += [fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK( logicalFileNames )

  #############################################################################
  def getAllAncestors( self, lfn, depth ):
    logicalFileNames = {}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    jobsId = []
    job_id = -1
    if depth > 10:
      depth = 10
    elif depth < 1:
      depth = 1
    odepth = depth
    gLogger.debug( 'original', lfn )
    for fileName in lfn:
      depth = odepth
      jobsId = []
      gLogger.debug( 'filename', fileName )
      jobsId = []
      result = self.dbR_.executeStoredFunctions( 'BKK_MONITORING.getJobIdWithoutReplicaCheck', LongType, [fileName] )
      if not result["OK"]:
        gLogger.error( 'Ancestor', result['Message'] )
      else:
        job_id = int( result['Value'] )
      if job_id != 0:
        jobsId = [job_id]
        files = []
        while ( depth - 1 ) and jobsId:
          for job_id in jobsId:
            command = 'select files.fileName,files.jobid, files.gotreplica from inputfiles,files where inputfiles.fileid=files.fileid and inputfiles.jobid=' + str( job_id )
            jobsId = []
            res = self.dbR_._query( command )
            if not res['OK']:
              gLogger.error( 'Ancestor', result["Message"] )
            else:
              dbResult = res['Value']
              for record in dbResult:
                jobsId += [record[1]]
                files += [record[0]]
          depth -= 1

        ancestorList[fileName] = files
      else:
        logicalFileNames['Failed'] += [fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK( logicalFileNames )

  #############################################################################
  def getAllAncestorsWithFileMetaData( self, lfn, depth ):
    logicalFileNames = {}
    ancestorList = {}
    logicalFileNames['Failed'] = []
    jobsId = []
    job_id = -1
    if depth > 10:
      depth = 10
    elif depth < 1:
      depth = 1
    odepth = depth
    gLogger.debug( 'original', lfn )
    for fileName in lfn:
      depth = odepth
      jobsId = []
      gLogger.debug( 'filename', fileName )
      jobsId = []
      result = self.dbR_.executeStoredFunctions( 'BKK_MONITORING.getJobId', LongType, [fileName] )
      if not result["OK"]:
        gLogger.error( 'Ancestor', result['Message'] )
      else:
        job_id = int( result['Value'] )
      if job_id != 0:
        jobsId = [job_id]
        files = []
        while ( depth - 1 ) and jobsId:
          for job_id in jobsId:
            command = 'select files.fileName,files.jobid, files.gotreplica, files.eventstat, files.eventtypeid, files.luminosity, files.instLuminosity from inputfiles,files where inputfiles.fileid=files.fileid and inputfiles.jobid=' + str( job_id )
            jobsId = []
            res = self.dbR_._query( command )
            if not res['OK']:
              gLogger.error( 'Ancestor', result["Message"] )
            else:
              dbResult = res['Value']
              for record in dbResult:
                jobsId += [record[1]]
                files += [{'FileName':record[0], 'GotReplica':record[2], 'EventStat':record[3], 'EventType':record[4], 'Luminosity':record[5], 'InstLuminosity':record[6]}]
          depth -= 1

        ancestorList[fileName] = files
      else:
        logicalFileNames['Failed'] += [fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK( logicalFileNames )

  #############################################################################
  def getAllDescendents( self, lfn, depth = 0, production = 0, checkreplica = False ):
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

    tables = ''
    condition = ''
    gLogger.debug('original',lfn)
    for fileName in lfn:
      depth = odepth
      gLogger.debug( 'filename', fileName )
      fileids = []
      res = self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getFileID', LongType, [fileName] )
      if not res["OK"]:
        gLogger.error( 'Ancestor', res['Message'] )
      elif res['Value'] == None:
        logicalFileNames['Failed'] += [fileName]
      else:
        file_id = res['Value']
      if file_id != 0:
        fileids += [file_id]
        files = []
        while ( depth - 1 ) and fileids:
          for file_id in fileids:
            res = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getJobIdFromInputFiles', [file_id] )
            fileids.remove( file_id )
            if not res["OK"]:
              gLogger.error( 'Ancestor', res['Message'] )
              if not fileName in logicalFileNames['Failed']:
                logicalFileNames['Failed'] += [fileName]
            elif  len( res['Value'] ) != 0:
              job_ids = res['Value']
              for i in job_ids:
                job_id = i[0]
                command = 'select files.fileName,files.fileid,files.gotreplica, jobs.production from files, jobs where jobs.jobid=files.jobid and files.jobid=' + str( job_id )
                res = self.dbW_._query( command )
                if not res["OK"]:
                  gLogger.error( 'Ancestor', res['Message'] )
                  if not fileName in logicalFileNames['Failed']:
                    logicalFileNames['Failed'] += [fileName]
                elif len( res['Value'] ) == 0:
                  logicalFileNames['NotProcessed'] += [fileName]
                else:
                  dbResult = res['Value']
                  for record in dbResult:
                    fileids += [record[1]]
                    if checkreplica and ( record[2] == 'No' ):
                      continue
                    if production and ( int( record[3] ) != int( production ) ):
                      continue
                    files += [record[0]]
          depth -= 1

        ancestorList[fileName] = files
      else:
        logicalFileNames['Failed'] += [fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK( logicalFileNames )

  #############################################################################
  def getDescendents( self, lfn, depth = 0 ):
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

    gLogger.debug( 'original', lfn )
    for fileName in lfn:
      depth = odepth
      gLogger.debug( 'filename', fileName )
      fileids = []
      res = self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getFileID', LongType, [fileName] )
      if not res["OK"]:
        gLogger.error( 'Ancestor', res['Message'] )
      elif res['Value'] == None:
        logicalFileNames['Failed'] += [fileName]
      else:
        file_id = res['Value']
      if file_id != 0:
        fileids += [file_id]
        files = []
        while ( depth - 1 ) and fileids:
          for file_id in fileids:
            res = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getJobIdFromInputFiles', [file_id] )
            fileids.remove( file_id )
            if not res["OK"]:
              gLogger.error( 'Ancestor', res['Message'] )
              if not fileName in logicalFileNames['Failed']:
                logicalFileNames['Failed'] += [fileName]
            elif len( res['Value'] ) != 0:
              job_ids = res['Value']
              for i in job_ids:
                job_id = i[0]
                res = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getFNameFiDRepWithJID', [job_id] )
                if not res["OK"]:
                  gLogger.error( 'Ancestor', res['Message'] )
                  if not fileName in logicalFileNames['Failed']:
                    logicalFileNames['Failed'] += [fileName]
                elif len( res['Value'] ) == 0:
                  logicalFileNames['NotProcessed'] += [fileName]
                else:
                  dbResult = res['Value']
                  for record in dbResult:
                    fileids += [record[1]]
                    if record[2] != 'No':
                      files += [record[0]]

          depth -= 1

        ancestorList[fileName] = files
      else:
        logicalFileNames['Failed'] += [fileName]
      logicalFileNames['Successful'] = ancestorList
    return S_OK( logicalFileNames )

  """
  data insertation into the database
  """
  #############################################################################
  def checkfile( self, fileName ): #file

    result = self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.checkfile', [fileName] )
    if result['OK']:
      res = result['Value']
      if len( res ) != 0:
        return S_OK( res )
      else:
        gLogger.warn( "File not found! ", str( fileName ) )
        return S_ERROR( "File not found!" + str( fileName ) )
    else:
      return S_ERROR( result['Message'] )
    return result

  #############################################################################
  def checkFileTypeAndVersion( self, type, version ): #fileTypeAndFileTypeVersion(self, type, version):
    result = self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.checkFileTypeAndVersion', [type, version] )
    if result['OK']:
      res = result['Value']
      if len( res ) != 0:
        return S_OK( res )
      else:
        gLogger.error( "File type not found! ", str( type ) )
        return S_ERROR( "File type not found!" + str( type ) )
    else:
      return S_ERROR( result['Message'] )

    return result



  #############################################################################
  def checkEventType( self, eventTypeId ):  #eventType(self, eventTypeId):

    result = self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.checkEventType', [eventTypeId] )
    if result['OK']:
      res = result['Value']
      if len( res ) != 0:
        return S_OK( res )
      else:
        gLogger.error( "Event type not found! ", str( eventTypeId ) )
        return S_ERROR( "Event type not found!" + str( eventTypeId ) )
    else:
      return S_ERROR( result['Message'] )
    return result

  #############################################################################
  def insertJob( self, job ):

    gLogger.info( "Insert job into database!" )
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
      if not attrList.__contains__( param ):
        gLogger.error( "insert job error: ", " the job table not contains " + param + " this attributte!!" )
        return S_ERROR( " The job table not contains " + param + " this attributte!!" )

      if param == 'JobStart' or param == 'JobEnd': # We have to convert data format
        dateAndTime = job[param].split( ' ' )
        date = dateAndTime[0].split( '-' )
        time = dateAndTime[1].split( ':' )
        if len( time ) > 2:
          timestamp = datetime.datetime( int( date[0] ), int( date[1] ), int( date[2] ), int( time[0] ), int( time[1] ), int( time[2] ), 0 )
        else:
          timestamp = datetime.datetime( int( date[0] ), int( date[1] ), int( date[2] ), int( time[0] ), int( time[1] ), 0, 0 )
        attrList[param] = timestamp
      else:
        attrList[param] = job[param]

    try:
      conv = int(attrList['Tck'])
      attrList['Tck'] = str(hex(conv))
    except ValueError:
      pass #it is already defined


    result = self.dbW_.executeStoredFunctions('BOOKKEEPINGORACLEDB.insertJobsRow',LongType,[ attrList['ConfigName'], attrList['ConfigVersion'], \
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
  def insertInputFile( self, jobID, FileId ):
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.insertInputFilesRow', [FileId, jobID], False )
    return result

  #############################################################################
  def insertOutputFile( self, file ):

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
        if not attrList.__contains__( param ):
          gLogger.error( "insert file error: ", " the files table not contains " + param + " this attributte!!" )
          return S_ERROR( " The files table not contains " + param + " this attributte!!" )

        if param == 'CreationDate': # We have to convert data format
          dateAndTime = file[param].split( ' ' )
          date = dateAndTime[0].split( '-' )
          time = dateAndTime[1].split( ':' )
          timestamp = datetime.datetime( int( date[0] ), int( date[1] ), int( date[2] ), int( time[0] ), int( time[1] ), 0, 0 )
          attrList[param] = timestamp
        else:
          attrList[param] = file[param]
      utctime = datetime.datetime.utcnow()
      result = self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.insertFilesRow', LongType, [  attrList['Adler32'], \
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
                    attrList['VisibilityFlag'] ] )
      return result

  #############################################################################
  def updateReplicaRow( self, fileID, replica ): #, name, location):
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.updateReplicaRow', [fileID, replica], False )
    return result

  #############################################################################
  def deleteJob( self, jobID ):
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.deleteJob', [jobID], False )
    return result

  #############################################################################
  def deleteInputFiles( self, jobid ):
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.deleteInputFiles', [jobid], False )
    return result

  #############################################################################
  def deleteFile( self, fileid ):
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.deletefile', [fileid], False )
    return result

  #############################################################################
  def deleteFiles( self, lfns ):
    return S_ERROR( 'Not Implemented !!' )
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
  def insertSimConditions( self, simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity ):
    return self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.insertSimConditions', LongType, [simdesc, BeamCond, BeamEnergy, Generator, MagneticField, DetectorCond, Luminosity] )

  #############################################################################
  def getSimConditions( self ):
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getSimConditions', [] )

  #############################################################################
  def insertDataTakingCond( self, conditions ):
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
      if not datataking.__contains__( param ):
        gLogger.error( "insert datataking error: ", " the files table not contains " + param + " this attributte!!" )
        return S_ERROR( " The datatakingconditions table not contains " + param + " this attributte!!" )
      datataking[param] = conditions[param]

    res = self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.insertDataTakingCond', LongType, [datataking['Description'], datataking['BeamCond'], datataking['BeamEnergy'], \
                                                                                  datataking['MagneticField'], datataking['VELO'], \
                                                                                  datataking['IT'], datataking['TT'], datataking['OT'], \
                                                                                  datataking['RICH1'], datataking['RICH2'], \
                                                                                  datataking['SPD_PRS'], datataking['ECAL'], \
                                                                                  datataking['HCAL'], datataking['MUON'], datataking['L0'], datataking['HLT'], datataking['VeloPosition'] ] )
    return res


  #############################################################################
  def removeReplica( self, fileName ):
    result = self.checkfile( fileName )
    if result['OK']:
      fileID = long( result['Value'][0][0] )
      res = self.updateReplicaRow( fileID, 'No' )
      if res['OK']:
        return S_OK( "Replica has ben removed!!!" )
      else:
        return S_ERROR( res['Message'] )
    else:
      return S_ERROR( 'The file ' + fileName + 'not exist in the BKK database!!!' )

  #############################################################################
  def getFileMetadata( self, lfns ):
    result = {}
    for file in lfns:
      res = self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getFileMetaData', [file] )
      if not res['OK']:
        result[file] = res['Message']
      else:
        records = res['Value']
        for record in records:
          row = {'ADLER32':record[1], 'CreationDate':record[2], 'EventStat':record[3], 'FullStat':record[10], 'EventTypeId':record[4], 'FileType':record[5], 'GotReplica':record[6], 'GUID':record[7], 'MD5SUM':record[8], 'FileSize':record[9], 'DQFlag':record[11], 'JobId':record[12], 'RunNumber':record[13], 'InsertTimeStamp':record[14], 'Luminosity':record[15], 'InstLuminosity':record[16]}
          result[file] = row
    return S_OK( result )

  #############################################################################
  def getFilesInformations( self, lfns ):
    result = {}
    res = self.getFileMetadata( lfns )
    if not res['OK']:
      result = res
    else:
      records = res['Value']
      for file in records:
        value = records[file]
        command = 'select jobs.runnumber,jobs.fillnumber, configurations.configname,configurations.configversion from jobs,configurations where configurations.configurationid= jobs.configurationid and jobs.jobid=' + str( value['JobId'] )
        res = self.dbR_._query( command )
        if not res['OK']:
          result[file] = res['Message']
        else:
            info = res['Value']
            if len( info ) != 0:
              row = {'RunNumber':info[0][0], 'FillNumber':info[0][1], 'ConfigName':info[0][2], 'ConfigVersion':info[0][3]}
              value.pop( 'ADLER32' )
              value.pop( 'GUID' )
              value.pop( 'MD5SUM' )
              value.pop( 'JobId' )
              row.update( value )
              result[file] = row
            else:
              result[file] = {}
    return S_OK( result )

  #############################################################################
  def getFileMetaDataForUsers( self, lfns ):
    totalrecords = len( lfns )
    parametersNames = ['Name', 'FileSize', 'FileType', 'CreationDate', 'EventTypeId', 'EventStat', 'GotReplica']
    records = []
    for file in lfns:
      res = self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getFileMetaData', [file] )
      if not res['OK']:
        records = [str( res['Message'] )]
      else:
        values = res['Value']
        for record in values:
          row = [file, record[9], record[5], record[2], record[4], record[3], record[6]]
          records += [row]
    return S_OK( {'TotalRecords':totalrecords, 'ParameterNames':parametersNames, 'Records':records} )

  #############################################################################
  def __getProductionStatisticsForUsers( self, prod ):
    command = 'select count(*), SUM(files.EventStat), SUM(files.FILESIZE), sum(files.Luminosity), sum(files.instLuminosity) from files ,jobs where jobs.jobid=files.jobid and jobs.production=' + str( prod )
    res = self.dbR_._query( command )
    return res

  #############################################################################
  def getProductionFilesForUsers( self, prod, ftypeDict, SortDict, StartItem, Maxitems ):
    command = ''
    parametersNames = ['Name', 'FileSize', 'FileType', 'CreationDate', 'EventTypeId', 'EventStat', 'GotReplica', 'InsertTimeStamp', 'Luminosity', 'InstLuminosity']
    records = []

    totalrecords = 0
    nbOfEvents = 0
    filesSize = 0
    ftype = ftypeDict['type']
    if len( SortDict ) > 0:
      res = self.__getProductionStatisticsForUsers( prod )
      if not res['OK']:
        gLogger.error( res['Message'] )
      else:
        totalrecords = res['Value'][0][0]
        nbOfEvents = res['Value'][0][1]
        filesSize = res['Value'][0][2]

    if ftype != 'ALL':
      fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\'' + str( ftype ) + '\''
      res = self.dbR_._query( fileType )
      if not res['OK']:
        gLogger.error( res['Message'] )
        return S_ERROR( 'Oracle error' + res['Message'] )
      else:
        if len( res['Value'] ) == 0:
          return S_ERROR( 'File Type not found:' + str( ftype ) )

        ftypeId = res['Value'][0][0]

        command = 'select rnum, filename, filesize, \'' + str( ftype ) + '\' , creationdate, eventtypeId, eventstat,gotreplica, inserttimestamp , luminosity ,instLuminosity from \
                ( select rownum rnum, filename, filesize, \'' + str( ftype ) + '\' , creationdate, eventtypeId, eventstat, gotreplica, inserttimestamp, luminosity,instLuminosity \
                from ( select files.filename, files.filesize, \'' + str( ftype ) + '\' , files.creationdate, files.eventtypeId, files.eventstat,files.gotreplica, files.inserttimestamp, files.luminosity, files.instLuminosity \
                           from jobs,files where \
                           jobs.jobid=files.jobid and \
                           files.filetypeid='+str(ftypeId)+' and \
                           jobs.production='+str(prod)+' Order by files.filename) where rownum <='+str(Maxitems)+ ') where rnum >'+str(StartItem)
    else:

      command = 'select rnum, filename, filesize, name, creationdate, eventtypeId, eventstat,gotreplica, inserttimestamp, luminosity, instLuminosity from \
                ( select rownum rnum, filename, filesize, name, creationdate, eventtypeId, eventstat, gotreplica, inserttimestamp, luminosity, instLuminosity \
                from ( select files.filename, files.filesize, filetypes.name, files.creationdate, files.eventtypeId, files.eventstat,files.gotreplica, files.inserttimestamp, files.luminosity, files.instLuminosity  \
                           from jobs,files,filetypes where \
                           jobs.jobid=files.jobid and \
                           files.filetypeid=filetypes.filetypeid and \
                           jobs.production='+str(prod)+' Order by files.filename) where rownum <='+str(Maxitems)+ ') where rnum >'+str(StartItem)

    res = self.dbR_._query(command)
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        row = [record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8]]
        records += [row]
    else:
      return S_ERROR( res['Message'] )
    return S_OK( {'TotalRecords':totalrecords, 'ParameterNames':parametersNames, 'Records':records, 'Extras': {'GlobalStatistics':{'Number of Events':nbOfEvents, 'Files Size':filesSize }}} )

  #############################################################################
  def exists( self, lfns ):
    result = {}
    for file in lfns:
      res = self.dbR_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.fileExists', LongType, [file] )
      if not res['OK']:
        return S_ERROR( res['Message'] )
      if res['Value'] == 0:
        result[file] = False
      else:
        result[file] = True
    return S_OK( result )

  #############################################################################
  def addReplica( self, fileName ):
    result = self.checkfile( fileName )
    if result['OK']:
      fileID = long( result['Value'][0][0] )
      res = self.updateReplicaRow( fileID, 'Yes' )
      if res['OK']:
        return S_OK( "Replica has ben added!!!" )
      else:
        return S_ERROR( res['Message'] )
    else:
      return S_ERROR( 'The file ' + fileName + 'not exist in the BKK database!!!' )


  #############################################################################
  def getRunInformations( self, runnb ):
    command = 'select distinct j.fillnumber, conf.configname, conf.configversion, daq.description, j.jobstart, j.jobend, j.tck \
        from jobs j, configurations conf,data_taking_conditions daq, productionscontainer prod \
         where j.configurationid=conf.configurationid and j.production<0 and prod.daqperiodid=daq.daqperiodid and j.production=prod.production and j.runnumber='+str(runnb)
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )
    value = retVal['Value']
    if len( value ) == 0:
      return S_ERROR( 'This run is missing in the BKK DB!' )
    result = {'Configuration Name':value[0][1], 'Configuration Version':value[0][2], 'FillNumber':value[0][0]}
    result['DataTakingDescription'] = value[0][3]
    result['RunStart'] = value[0][4]
    result['RunEnd'] = value[0][5]
    result['Tck'] = value[0][6]

    retVal = self.getRunProcessingPass( runnb )
    if retVal['OK']:
      result['ProcessingPass'] = retVal['Value']
    else:
      return retVal
    command = ' select count(*), SUM(files.EventStat), SUM(files.FILESIZE), sum(files.fullstat), files.eventtypeid , sum(files.luminosity), sum(files.instLuminosity)  from files,jobs \
         where files.JobId=jobs.JobId and  \
         files.gotReplica=\'Yes\' and \
         jobs.production<0 and \
         jobs.runnumber='+str(runnb)+' Group by files.eventtypeid'
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )
    value = retVal['Value']
    if len( value ) == 0:
      return S_ERROR( 'Replica flag is not set!' )
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

    result['Number of file'] = nbfile
    result['Number of events'] = nbevent
    result['File size'] = fsize
    result['FullStat'] = fstat
    result['Stream'] = stream
    result['luminosity'] = luminosity
    result['InstLuminosity'] = ilumi

    return S_OK( result )

  #############################################################################
  def checkProductionStatus( self, productionid = None, lfns = [] ):
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
        return S_ERROR( retVal['Message'] )
      files = retVal['Value']
      for file in files:
        if file[1] == 'Yes':
          replicas += [file[0]]
        else:
          noreplicas += [file[0]]
      result['replica'] = replicas
      result['noreplica'] = noreplicas
    elif len( lfns ) != 0:
      for file in lfns:
        command = ' select files.filename, files.gotreplica from files where filename=\'' + str( file ) + '\''
        retVal = self.dbR_._query( command )
        if not retVal['OK']:
          return S_ERROR( retVal['Message'] )
        value = retVal['Value']
        if len( value ) == 0:
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

    return S_OK( result )

  #############################################################################
  def getLogfile( self, lfn ):
    command = 'select files.jobid from files where files.filename=\'' + str( lfn ) + '\''
    retVal = self.dbR_._query( command )
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )
    if len( retVal['Value'] ) == 0:
      return S_ERROR( 'Job not in the DB' )
    jobid = retVal['Value'][0][0]
    command = 'select filename from files where (files.filetypeid=17 or files.filetypeid=9) and files.jobid=' + str( jobid )
    retVal = self.dbR_._query( command )
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )
    else:
      if len( retVal['Value'] ) == 0:
        return S_ERROR( 'Log file is not exist!' )
      return S_OK( retVal['Value'][0][0] )
    return S_ERROR( 'getLogfile error!' )

  #############################################################################
  def insertEventTypes( self, evid, desc, primary ):
    return self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.insertEventTypes', [desc, evid, primary], False )

  #############################################################################
  def updateEventType( self, evid, desc, primary ):
    return self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.updateEventTypes', [desc, evid, primary], False )


  #############################################################################
  def getProductionSummary( self, cName, cVersion, simdesc = 'ALL', pgroup = 'ALL', production = 'ALL', ftype = 'ALL', evttype = 'ALL' ):

    conditions = ''

    if cName != 'ALL':
      conditions += ' and bview.configname=\'' + cName + '\''

    if cVersion != 'ALL':
      conditions += ' and bview.configversion=\'' + cVersion + '\''

    if ftype != 'ALL':
      conditions += "and f.filetypeid=ftypes.filetypeid and ftypes.name='" + str( ftype ) + "'"
    else:
      conditions += ' and bview.filetypeid= f.filetypeid  \
                      and bview.filetypeid= ftypes.filetypeid '
    if evttype != 'ALL':
      conditions += ' and bview.eventtypeid=' + str( evttype )
    else:
      conditions += ' and bview.eventtypeid=f.eventtypeid '

    if production != 'ALL':
      conditions += ' and j.production=' + str( production )
      conditions += ' and j.production= bview.production '
    else:
      conditions += ' and j.production= bview.production '

    if simdesc != 'ALL':
      conditions += 'and prod.simid = sim.simid '
      conditions += 'and sim.simdescription = \'' + str( simdesc ) + '\''
    else:
      conditions += 'and prod.simcondid = sim.simid '

    command = " select bview.configname, bview.configversion, sim.simdescription, \
       v.Path, bview.eventtypeid,bview.description, bview.production, ftypes.name, sum(f.eventstat) \
  from jobs j, prodview bview, files f, filetypes ftypes, productionscontainer prod, simulationconditions sim, \
  (SELECT distinct  LEVEL-1 Pathlen, SYS_CONNECT_BY_PATH(name, '/') Path \
   FROM processing \
   WHERE LEVEL > 0 and id in (select distinct processingid from productionscontainer prod where prod.production=5016) \
   CONNECT BY PRIOR id=parentid order by Pathlen desc) v \
        where rownum<=1 and \
        j.jobid= f.jobid and \
        f.gotreplica='Yes' and \
        bview.programname= j.programname and \
        bview.programversion= j.programversion and \
        bview.production = prod.production" + conditions

    command += "group by bview.configname, bview.configversion, sim.simdescription, v.Path, bview.eventtypeid, bview.description, bview.production, ftypes.name"
    retVal = self.dbR_._query( command )
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )

    parameters = ['ConfigurationName', 'ConfigurationVersion', 'SimulationDescription', 'Processing pass group', 'EventTypeId', 'EventType description', 'Production', 'FileType', 'Number of events']
    dbResult = retVal['Value']
    records = []
    nbRecords = 0
    for record in dbResult:
      row = [record[0], record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8]]
      records += [row]
      nbRecords += 1

    return S_OK( {'TotalRecords':nbRecords, 'ParameterNames':parameters, 'Records':records, 'Extras': {}} )

  #############################################################################
  def getProductionSimulationCond( self, prod ):
    simdesc = None
    daqdesc = None

    command = "select distinct sim.simdescription, daq.description from simulationconditions sim, data_taking_conditions daq,productionscontainer prod \
              where sim.simid(+)=prod.simid and daq.daqperiodid(+)=prod.daqperiodid and prod.production="+str(prod)
    retVal = self.dbR_._query(command)
    if not retVal['OK']:
      return retVal
    else:
      value = retVal['Value']
      if len( value ) != 0:
        simdesc = value[0][0]
        daqdesc = value[0][1]
      else:
        return S_ERROR( 'Simulation condition or data taking condition not exist!' )

    if simdesc != None:
      return S_OK( simdesc )
    else:
      return S_OK( daqdesc )

  #############################################################################
  def getFileHistory( self, lfn ):
    command = 'select  files.fileid, files.filename,files.adler32,files.creationdate,files.eventstat,files.eventtypeid,files.gotreplica, \
files.guid,files.jobid,files.md5sum, files.filesize,files.fullstat, dataquality.dataqualityflag, files.inserttimestamp, files.luminosity, files.instLuminosity from files, dataquality \
where files.fileid in ( select inputfiles.fileid from files,inputfiles where files.jobid= inputfiles.jobid and files.filename=\'' + str( lfn ) + '\')\
and files.qualityid= dataquality.qualityid'

    res = self.dbR_._query( command )
    return res

  #############################################################################
  def getProductionInformationsFromView( self, prodid ):
    command = 'select * from productioninformations where production=' + str( prodid )
    res = self.dbR_._query( command )
    return res

  #############################################################################
  #
  #          MONITORING
  #############################################################################
  def getJobsNb( self, prodid ):
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getJobsNb', [prodid] )

  #############################################################################
  def getNumberOfEvents( self, prodid ):
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getNumberOfEvents', [prodid] )

  #############################################################################
  def getSizeOfFiles( self, prodid ):
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getSizeOfFiles', [prodid] )

  #############################################################################
  def getNbOfFiles( self, prodid ):
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getNbOfFiles', [prodid] )

  #############################################################################
  def getProductionInformation( self, prodid ):
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getProductionInformation', [prodid] )

  #############################################################################
  def getSteps( self, prodid ):
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getSteps', [prodid] )

  #############################################################################
  def getNbOfJobsBySites( self, prodid ):
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getJobsbySites', [prodid] )

  #############################################################################
  def getConfigsAndEvtType( self, prodid ):
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getConfigsAndEvtType', [prodid] )

  #############################################################################
  def getAvailableTags( self ):
    command = 'select name, tag from tags order by inserttimestamp desc'
    retVal = self.dbR_._query( command )
    if retVal['OK']:
      parameters = ['TagName', 'TagValue']
      dbResult = retVal['Value']
      records = []
      nbRecords = 0
      for record in dbResult:
        row = [record[0], record[1]]
        records += [row]
        nbRecords += 1
      return S_OK( {'TotalRecords':nbRecords, 'ParameterNames':parameters, 'Records':records, 'Extras': {}} )
    else:
      return retVal

  #############################################################################
  def getProcessedEvents( self, prodid ):
    command = 'select sum(jobs.numberofevents) from jobs where jobs.production=' + str( prodid )
    res = self.dbR_._query( command )
    return res

  #############################################################################
  def getRunsWithAGivenDates( self, dict ):
    condition = ''
    if dict.has_key( 'AllowOutsideRuns' ) and dict['AllowOutsideRuns']:
      if not dict.has_key( 'StartDate' ) and not dict.has_key( 'EndDate' ):
        return S_ERROR( 'The Start and End date must be given!' )
      else:
        if dict.has_key( 'StartDate' ):
          condition += ' and jobs.jobstart >= TO_TIMESTAMP (\'' + str( dict['StartDate'] ) + '\',\'YYYY-MM-DD HH24:MI:SS\')'

        if dict.has_key( 'EndDate' ):
          condition += ' and jobs.jobstart <= TO_TIMESTAMP (\'' + str( dict['EndDate'] ) + '\',\'YYYY-MM-DD HH24:MI:SS\')'
    else:
      if dict.has_key( 'StartDate' ):
        condition += ' and jobs.jobstart >= TO_TIMESTAMP (\'' + str( dict['StartDate'] ) + '\',\'YYYY-MM-DD HH24:MI:SS\')'

      if dict.has_key( 'EndDate' ):
        condition += ' and jobs.jobend <= TO_TIMESTAMP (\'' + str( dict['EndDate'] ) + '\',\'YYYY-MM-DD HH24:MI:SS\')'
      elif dict.has_key( 'StartDate' ) and not dict.has_key( 'EndDate' ):
        d = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
        condition += ' and jobs.jobend <= TO_TIMESTAMP (\'' + str( d ) + '\',\'YYYY-MM-DD HH24:MI:SS\')'

    command = ' select jobs.runnumber from jobs where jobs.production < 0' + condition
    retVal = self.dbR_._query( command )
    runIds = []
    if retVal['OK']:
      records = retVal['Value']
      for record in records:
        if record[0] != None:
          runIds += [record[0]]
    else:
      return S_ERROR( retVal['Message'] )

    if dict.has_key( 'CheckRunStatus' ) and dict['CheckRunStatus']:
      processedRuns = []
      notProcessedRuns = []
      for i in runIds:
        command = 'select files.filename from files,jobs where jobs.jobid=files.jobid and files.gotreplica=\'Yes\' and jobs.production<0 and jobs.runnumber=' + str( i )
        retVal = self.dbR_._query( command )
        if retVal['OK']:
          files = retVal['Value']
          ok = True
          for file in files:
            name = file[0]
            retVal = self.getDescendents( [name], 1 )
            files = retVal['Value']
            successful = files['Successful']
            failed = files['Failed']
            if len(successful[successful.keys()[0]]) == 0:
              ok = False
          if ok:
            processedRuns += [i]
          else:
            notProcessedRuns += [i]

      return S_OK( {'Runs':runIds, 'ProcessedRuns':processedRuns, 'NotProcessedRuns':notProcessedRuns} )
    else:
      return S_OK( {'Runs':runIds} )
    return S_ERROR()

  #############################################################################
  def getProductiosWithAGivenRunAndProcessing( self, dict ):

    if dict.has_key( 'Runnumber' ):
      run = dict['Runnumber']
    else:
      return S_ERROR( 'The run number is missing!' )

    if dict.has_key( 'ProcPass' ):
      proc = dict['ProcPass']
    else:
      return S_ERROR( 'The processing pass is missing!' )

    retVal = self._getProcessingPassId( proc.split( '/' )[1:][0], proc )
    if retVal['OK']:
      processingid = retVal['Value']
      command = 'select distinct bview.production  from prodview bview, prodrunview prview, productionscontainer prod where \
      bview.production=prview.production and prview.runnumber='+str(run)+' and bview.production>0 and bview.production=prod.production and prod.processingid='+str(processingid)
      retVal = self.dbR_._query(command)
      return retVal
    return S_ERROR()

  #############################################################################
  def getDataQualityForRuns( self, runs ):
    command = ' select distinct jobs.runnumber,dataquality.dataqualityflag from files, jobs,dataquality where files.jobid=jobs.jobid and files.qualityid=dataquality.qualityid  and jobs.production<0 and ('
    conditions = ''
    for i in runs:
      conditions += ' jobs.runnumber=' + str( i ) + 'or'
    conditions += conditions[:-2] + ')'
    command += conditions
    retVal = self.dbR_._query( command )
    return retVal

  #############################################################################
  def getRunFlag( self, runnb, processing ):
    return self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getQFlagByRunAndProcId', StringType, [runnb, processing] )

  #############################################################################
  def getRunQuality( self, procpass, flag = 'ALL' ):
    retVal = self._getProcessingPassId( procpass.split( '/' )[1:][0], procpass )
    if retVal['OK']:
      processingid = retVal['Value']
      id = None
      if flag != 'ALL':
        retVal = self._getDataQualityId( flag )
        if retVal['OK']:
          id = retVal['Value']
        else:
          return retVal
    return self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getRunByQflagAndProcId', LongType, [processingid, id] )

  #############################################################################
  def setFilesInvisible( self, lfns ):
    for i in lfns:
      res = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.setFileInvisible', [i], False )
      if not res['OK']:
        return S_ERROR( res['Message'] )
    return S_OK( 'The files are invisible!' )

  #############################################################################
  def setFilesVisible( self, lfns ):
    for i in lfns:
      res = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.setFileVisible', [i], False )
      if not res['OK']:
        return S_ERROR( res['Message'] )
    return S_OK( 'The files are visible!' )

  #############################################################################
  def getFilesWithGivenDataSets(self, simdesc, datataking, procPass, ftype, evt, configName='ALL', configVersion='ALL', production='ALL', flag = 'ALL', startDate = None, endDate = None, nbofEvents=False, startRunID=None, endRunID=None, runnumbers = [], replicaFlag='Yes', visible='ALL', filesize=False):

    configid = None
    condition = ''

    if configName != 'ALL' and configVersion != 'ALL':
      command = ' select configurationid from configurations where configurations.ConfigName=\'' + configName + '\' and \
                    configurations.ConfigVersion=\'' + configVersion + '\''
      res = self.dbR_._query( command )
      if not res['OK']:
        return S_ERROR( res['Message'] )
      elif len( res['Value'] ) == 0:
        return S_ERROR( 'Config name and version dosnt exist!' )
      else:
        configid = res['Value'][0][0]
        if configid != 0:
          condition = ' and j.configurationid=' + str( configid )
        else:
          return S_ERROR( 'Wrong configuration name and version!' )

    if production != 'ALL':
      if type( production ) == types.ListType:
        condition += ' and '
        cond = ' ( '
        for i in production:
          cond += 'j.production=' + str( i ) + ' or '
        cond = cond[:-3] + ')'
        condition += cond
      else:
        condition += ' and j.production=' + str( production )

    if len( runnumbers ) > 0:
      if type( runnumbers ) == types.ListType:
        cond = ' ( '
        for i in runnumbers:
          cond += 'j.runnumber=' + str( i ) + ' or '
        cond = cond[:-3] + ')'
      if startRunID == None and endRunID == None:
        condition += " and %s "%(cond)
      elif startRunID != None and endRunID != None:
        condition += ' and (j.runnumber>=%s and j.runnumber<=%s or %s)' % ( str( startRunID ), str( endRunID ), cond)
    else:
      if startRunID != None:
        condition += ' and j.runnumber>=' + str( startRunID )
      if endRunID != None:
        condition += ' and j.runnumber<=' + str( endRunID )

    tables = ' files f,jobs j '
    if procPass != 'ALL':
      if not re.search( '^/', procPass ): procPass = procPass.replace( procPass, '/%s' % procPass )
      command = "select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='%s'"%(procPass.split('/')[1], procPass)
      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        return retVal
      pro = '('
      for i in retVal['Value']:
        pro += "%s," % ( str( i[0] ) )
      pro = pro[:-1]
      pro += ')'

      condition += " and j.production=prod.production \
                     and prod.processingid in %s"%(pro);
      tables += ',productionscontainer prod'

    if ftype != 'ALL':
      if type( ftype ) == types.ListType:
        condition += ' and '
        cond = ' ( '
        for i in ftype:
          fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\'' + str( i ) + '\''
          res = self.dbR_._query( fileType )
          if not res['OK']:
            gLogger.error( 'File Type not found:', res['Message'] )
          elif len( res['Value'] ) == 0:
            return S_ERROR( 'File type not found!' + str( i ) )
          else:
            ftypeId = res['Value'][0][0]
            cond += ' f.FileTypeId=' + str( ftypeId ) + ' or '
        cond = cond[:-3] + ')'
        condition += cond
      elif type( ftype ) == types.StringType:
        fileType = 'select filetypes.FileTypeId from filetypes where filetypes.Name=\'' + str( ftype ) + '\''
        res = self.dbR_._query( fileType )
        if not res['OK']:
          gLogger.error( 'File Type not found:', res['Message'] )
        elif len( res['Value'] ) == 0:
          return S_ERROR( 'File type not found!' + str( ftype ) )
        else:
          ftypeId = res['Value'][0][0]
          condition += ' and f.FileTypeId=' + str( ftypeId )

    if evt != 0:
      if type( evt ) in ( types.ListType, types.TupleType ):
        condition += ' and '
        cond = ' ( '
        for i in evt:
          cond += ' f.eventtypeid=' + str( i ) + ' or '
        cond = cond[:-3] + ')'
        condition += cond
      elif type( evt ) in ( types.StringTypes + ( types.IntType, types.LongType ) ):
        condition += ' and f.eventtypeid=' + str( evt )

    if startDate != None:
      condition += ' and f.inserttimestamp >= TO_TIMESTAMP (\'' + str( startDate ) + '\',\'YYYY-MM-DD HH24:MI:SS\')'

    if endDate != None:
      condition += ' and f.inserttimestamp <= TO_TIMESTAMP (\'' + str( endDate ) + '\',\'YYYY-MM-DD HH24:MI:SS\')'
    elif startDate != None and endDate == None:
      d = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
      condition += ' and f.inserttimestamp <= TO_TIMESTAMP (\'' + str( d ) + '\',\'YYYY-MM-DD HH24:MI:SS\')'

    if flag != 'ALL':
      if type( flag ) in ( types.ListType, types.TupleType ):
        conds = ' ('
        for i in flag:
          quality = None
          command = 'select QualityId from dataquality where dataqualityflag=\'' + str( i ) + '\''
          res = self.dbR_._query( command )
          if not res['OK']:
            gLogger.error( 'Data quality problem:', res['Message'] )
          elif len( res['Value'] ) == 0:
              return S_ERROR( 'Dataquality is missing!' )
          else:
            quality = res['Value'][0][0]
          conds += ' f.qualityid=' + str( quality ) + ' or'
        condition += 'and' + conds[:-3] + ')'
      else:
        quality = None
        command = 'select QualityId from dataquality where dataqualityflag=\'' + str( flag ) + '\''
        res = self.dbR_._query( command )
        if not res['OK']:
          gLogger.error( 'Data quality problem:', res['Message'] )
        elif len( res['Value'] ) == 0:
            return S_ERROR( 'Dataquality is missing!' )
        else:
          quality = res['Value'][0][0]

        condition += ' and f.qualityid=' + str( quality )


    if replicaFlag in ['Yes', 'No']:
      condition += ' and f.gotreplica=\'' + replicaFlag + '\''

    visibleFlags = {'Yes':'Y', 'No':'N'}
    if visible != 'ALL' and visibleFlags.has_key( visible ):
      condition += " and f.visibilityflag='%s'" % ( visibleFlags[visible] )

    if simdesc != 'ALL':
      condition += " and prod.simid=sim.simid and j.production=prod.production and \
                     sim.simdescription='%s' "%(simdesc)
      if re.search('productionscontainer',tables):
        tables += ',simulationconditions sim'
      else:
        tables += ',simulationconditions sim, productionscontainer prod'


    if datataking != 'ALL':
      condition += " and prod.DAQPERIODID =daq.daqperiodid and j.production=prod.production and \
                    daq.description='%s' "%(datataking)
      if re.search('productionscontainer',tables):
        tables += ',data_taking_conditions daq'
      else:
        tables += ',data_taking_conditions daq, productionscontainer prod'


    if nbofEvents:
      command = " select sum(f.eventstat) from %s where f.jobid= j.jobid %s " % (tables, condition)
    elif filesize:
      command = " select sum(f.filesize) from %s where f.jobid= j.jobid %s " % (tables, condition)
    else:
      command = " select distinct f.filename from %s where f.jobid= j.jobid %s " % ( tables, condition )

    res = self.dbR_._query( command )

    return res

  #############################################################################
  def getFilesWithGivenDataSetsForUsers( self, simdesc, datataking, procPass, ftype, evt, configName = 'ALL', configVersion = 'ALL', production = 'ALL', flag = 'ALL', startDate = None, endDate = None, nbofEvents = False, startRunID = None, endRunID = None, runnumbers = [], replicaFlag = 'Yes' ):

    condition = ''


    tables = ' jobs j, files f'
    yes = False
    if configName != default:
      condition += " and c.configname='%s' " % ( configName )
      yes = True

    if configVersion != default:
      condition += " and c.configversion='%s' " % ( configVersion )
      yes = True

    if yes:
      condition += ' and j.configurationid=c.configurationid'
      tables += ' ,configurations c '

    if simdesc != 'ALL':
      conddescription = simdesc
    else:
      conddescription = datataking

    sim_dq_conditions = ''
    if conddescription != default:
      retVal = self._getConditionString( conddescription, 'prod' )
      if retVal['OK']:
        sim_dq_conditions = retVal['Value']
      else:
        return retVal

    if production != 'ALL':
      if type( production ) == types.ListType:
        condition += ' and '
        cond = ' ( '
        for i in production:
          cond += 'j.production=' + str( i ) + ' or '
        cond = cond[:-3] + ')'
        condition += cond
      else:
        condition += ' and j.production=' + str( production )

    if len( runnumbers ) > 0:
      if type( runnumbers ) == types.ListType:
        condition += ' and '
        cond = ' ( '
        for i in runnumbers:
          cond += 'j.runnumber=' + str( i ) + ' or '
        cond = cond[:-3] + ')'
        condition += cond


    fcond = ''
    tables2 = ' ,filetypes ftypes '
    tables += ' ,filetypes ftypes '
    if ftype != 'ALL':
      if type( ftype ) == types.ListType:
        for i in ftype:
          condition += " and ftypes.Name='%s'"%(str( i ))
          fcond += " and ftypes.Name='%s'"%(str( i ))

      elif type( ftype ) == types.StringType:
        condition += " and ftypes.Name='%s'"%(str( ftype ))
        fcond += " and ftypes.Name='%s'"%(str( ftype ))
      fcond += 'and bview.filetypeid=ftypes.filetypeid '
      condition += ' and f.filetypeid=ftypes.filetypeid '
    econd = ''
    if evt != 0:
      if type( evt ) in ( types.ListType, types.TupleType ):
        econd += " and bview.eventtypeid=%s"%(str( i ))
        condition += " and f.eventtypeid=%s"%(str( i ))

      elif type( evt ) in ( types.StringTypes + ( types.IntType, types.LongType ) ):
        econd += " and bview.eventtypeid='%s'"%(str( evt ))
        condition += " and f.eventtypeid=%s"%(str( evt ))

    if procPass != 'ALL':
      if not re.search( '^/', procPass ): procPass = procPass.replace( procPass, '/%s' % procPass )
      condition += " and j.production in (select bview.production from productionscontainer prod, ( select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID FROM processing v \
                     START WITH id in (select distinct id from processing where name='%s') \
                                           CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                         where v.path='%s') proc, prodview bview %s \
                             where \
                               prod.processingid=proc.id and \
                               bview.production=prod.production \
                               %s %s %s \
                )"%(procPass.split('/')[1], procPass, tables2, fcond, econd, sim_dq_conditions)

    if startDate != None:
      condition += ' and f.inserttimestamp >= TO_TIMESTAMP (\'' + str( startDate ) + '\',\'YYYY-MM-DD HH24:MI:SS\')'

    if endDate != None:
      condition += ' and f.inserttimestamp <= TO_TIMESTAMP (\'' + str( endDate ) + '\',\'YYYY-MM-DD HH24:MI:SS\')'
    elif startDate != None and endDate == None:
      d = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
      condition += ' and f.inserttimestamp <= TO_TIMESTAMP (\'' + str( d ) + '\',\'YYYY-MM-DD HH24:MI:SS\')'

    if flag != 'ALL':
      if type( flag ) in ( types.ListType, types.TupleType ):
        conds = ' ('
        for i in flag:
          quality = None
          command = 'select QualityId from dataquality where dataqualityflag=\'' + str( i ) + '\''
          res = self.dbR_._query( command )
          if not res['OK']:
            gLogger.error( 'Data quality problem:', res['Message'] )
          elif len( res['Value'] ) == 0:
              return S_ERROR( 'Dataquality is missing!' )
          else:
            quality = res['Value'][0][0]
          conds += ' f.qualityid=' + str( quality ) + ' or'
        condition += 'and' + conds[:-3] + ')'
      else:
        quality = None
        command = 'select QualityId from dataquality where dataqualityflag=\'' + str( flag ) + '\''
        res = self.dbR_._query( command )
        if not res['OK']:
          gLogger.error( 'Data quality problem:', res['Message'] )
        elif len( res['Value'] ) == 0:
            return S_ERROR( 'Dataquality is missing!' )
        else:
          quality = res['Value'][0][0]

        condition += ' and f.qualityid=' + str( quality )

    if startRunID != None:
      condition += ' and j.runnumber>=' + str( startRunID )
    if endRunID != None:
      condition += ' and j.runnumber<=' + str( endRunID )

    command = " select distinct f.filename, f.eventstat, j.eventinputstat, j.runnumber, j.fillnumber, f.filesize, j.totalluminosity, f.luminosity, f.instLuminosity from %s where f.jobid= j.jobid and f.visibilityflag='Y'  and f.gotreplica='Yes' %s " % ( tables, condition )

    res = self.dbR_._query( command )

    return res

  #############################################################################
  def getFilesSumary( self, configName, configVersion, conddescription = default, processing = default, evt = default, production = default, filetype = default, quality = default, runnb = default ):

    condition = ''
    tables = ' jobs j, files f'
    if configName != default:
      tables += ' , configurations c'
      condition += " and c.configname='%s' " % ( configName )
      condition += ' and j.configurationid=c.configurationid '

    if configVersion != default:
      condition += " and c.configversion='%s' " % ( configVersion )

    sim_dq_conditions = ''
    if conddescription != default:
      retVal = self._getConditionString( conddescription, 'prod' )
      if retVal['OK']:
        sim_dq_conditions = retVal['Value']
      else:
        return retVal

    econd = ''
    if evt != default:
      econd += " and bview.eventtypeid='%s'"%(str( evt ))
      condition += " and f.eventtypeid=%s"%(str( evt ))

    if production != default:
      condition += ' and j.production=' + str( production )

    if runnb != default:
      condition += ' and j.runnumber=' + str( runnb )


    tables += ' ,filetypes ftypes '
    tables2 = ''
    fcond = ''
    if filetype != default:
      tables2 = ' ,filetypes ftypes '
      condition += " and f.filetypeid=ftypes.filetypeid and ftypes.Name='%s'"%(str( filetype ))
      fcond += " and ftypes.Name='%s'"%(str( filetype ))
      fcond += 'and bview.filetypeid=ftypes.filetypeid '


    if quality != 'ALL':
      tables += ' , dataquality d'
      if type(quality) == types.StringType:
        command = "select QualityId from dataquality where dataqualityflag='%s'"%(str(i))
        res = self.dbR_._query(command)
        if not res['OK']:
          gLogger.error( 'Data quality problem:', res['Message'] )
        elif len( res['Value'] ) == 0:
            return S_ERROR( 'Dataquality is missing!' )
        else:
          quality = res['Value'][0][0]
        condition += ' and f.qualityid=' + str( quality )
      else:
        conds = ' ('
        for i in quality:
          quality = None
          command = "select QualityId from dataquality where dataqualityflag='%s'"%(str(i))
          res = self.dbR_._query(command)
          if not res['OK']:
            gLogger.error( 'Data quality problem:', res['Message'] )
          elif len( res['Value'] ) == 0:
              return S_ERROR( 'Dataquality is missing!' )
          else:
            quality = res['Value'][0][0]
          conds += ' f.qualityid='+str(quality)+' or'
        condition += 'and'+conds[:-3] + ')'
      condition += ' and d.qualityid=f.qualityid '

    if processing != default:
      if not re.search( '^/', processing ): processing = processing.replace(processing, '/%s' %processing )
      condition += " and j.production in (select bview.production from productionscontainer prod, ( select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID FROM processing v \
                     START WITH id in (select distinct id from processing where name='%s') \
                                           CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                         where v.path='%s') proc, prodview bview %s \
                             where \
                               prod.processingid=proc.id and \
                               bview.production=prod.production \
                               %s %s %s \
                )"%(processing.split('/')[1], processing, tables2, fcond, econd, sim_dq_conditions)

    command = "select count(*), SUM(f.EventStat), SUM(f.FILESIZE), SUM(f.luminosity),SUM(f.instLuminosity) from  %s  where \
    j.jobid=f.jobid and \
    ftypes.filetypeid=f.filetypeid and \
    f.gotreplica='Yes' and \
    f.visibilityflag='Y' and \
    ftypes.filetypeid=f.filetypeid  %s"%(tables,condition)
    return self.dbR_._query(command)

  #############################################################################
  def getLimitedFiles( self, configName, configVersion, conddescription = default, processing = default, evt = default, production = default, filetype = default, quality = default, runnb = default, startitem = 0, maxitems = 10 ):

    condition = ''
    if configName != default:
      condition += " and c.configname='%s' " % ( configName )

    if configVersion != default:
      condition += " and c.configversion='%s' " % ( configVersion )

    sim_dq_conditions = ''
    if conddescription != default:
      retVal = self._getConditionString( conddescription, 'prod' )
      if retVal['OK']:
        sim_dq_conditions = retVal['Value']
        condition += sim_dq_conditions
      else:
        return retVal

    tables = ''
    if evt != default:
      tables += ' ,prodview bview'
      condition += '  and j.production=bview.production and bview.production=prod.production and bview.eventtypeid=%s and f.eventtypeid=bview.eventtypeid '%(evt)

    if production != default:
      condition += ' and j.production=' + str( production )

    if runnb != default:
      condition += ' and j.runnumber=' + str( runnb )

    if filetype != default:
      condition += " and ftypes.name='%s' and bview.filetypeid=ftypes.filetypeid " % (str( filetype ))

    if quality != 'ALL':
      tables += ' ,dataquality d '
      condition += 'and d.qualityid=f.qualityid '
      if type(quality) == types.StringType:
        command = "select QualityId from dataquality where dataqualityflag='"+str(i)+"'"
        res = self.dbR_._query(command)
        if not res['OK']:
          gLogger.error( 'Data quality problem:', res['Message'] )
        elif len( res['Value'] ) == 0:
            return S_ERROR( 'Dataquality is missing!' )
        else:
          quality = res['Value'][0][0]
        condition += ' and f.qualityid=' + str( quality )
      else:
        if type( quality ) == types.ListType and len( quality ) > 0:
          conds = ' ('
          for i in quality:
            quality = None
            command = "select QualityId from dataquality where dataqualityflag='" + str( i ) + "'"
            res = self.dbR_._query( command )
            if not res['OK']:
              gLogger.error( 'Data quality problem:', res['Message'] )
            elif len( res['Value'] ) == 0:
                return S_ERROR( 'Dataquality is missing!' )
            else:
              quality = res['Value'][0][0]
            conds += ' f.qualityid=' + str( quality ) + ' or'
          condition += 'and' + conds[:-3] + ')'

    if processing != default:
      command = "select distinct prod.processingid from productionscontainer prod where \
           prod.processingid in (select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID FROM processing v \
         START WITH id in (select distinct id from processing where name='%s') \
         CONNECT BY NOCYCLE PRIOR  id=parentid) v \
        where v.path='%s') %s"%(processing.split('/')[1], processing, sim_dq_conditions)

      retVal = self.dbR_._query(command)
      if not retVal['OK']:
        return retVal
      pro = '('
      for i in retVal['Value']:
        pro += "%s," % ( str( i[0] ) )
      pro = pro[:-1]
      pro += ( ')' )

      condition += " and prod.processingid in %s" % ( pro )

    command = "select fname, fstat, fsize, fcreation, jstat, jend, jnode, ftypen, evttypeid, jrun, jfill, ffull, dflag,   jevent, jtotal, flum, finst, jtck from \
              (select rownum r, fname, fstat, fsize, fcreation, jstat, jend, jnode, ftypen, evttypeid, jrun, jfill, ffull, dflag,   jevent, jtotal, flum, finst, jtck from \
                  (select ROWNUM r, f.FileName fname, f.EventStat fstat, f.FileSize fsize, f.CreationDate fcreation, j.JobStart jstat, j.JobEnd jend, j.WorkerNode jnode, ftypes.Name ftypen, f.eventtypeid evttypeid,\
                          j.runnumber jrun, j.fillnumber jfill, f.fullstat ffull, d.dataqualityflag dflag,j.eventinputstat jevent, j.totalluminosity jtotal,\
                           f.luminosity flum, f.instLuminosity finst, j.tck jtck from files f, jobs j, productionscontainer prod, configurations c, filetypes ftypes %s where \
    j.jobid=f.jobid and \
    ftypes.filetypeid=f.filetypeid and \
    f.gotreplica='Yes' and \
    f.visibilityflag='Y' and \
    j.configurationid=c.configurationid  %s) where rownum <=%d ) where r >%d"%(tables, condition, int(maxitems),int(startitem))
    return self.dbR_._query(command)

  #############################################################################
  def getDataTakingCondId( self, condition ):
    command = 'select DaqPeriodId from data_taking_conditions where '
    for param in condition:
      if type( condition[param] ) == types.StringType and len( condition[param].strip() ) == 0:
        command += str( param ) + ' is NULL and '
      elif condition[param] != None:
        command += str( param ) + '=\'' + condition[param] + '\' and '
      else:
        command += str( param ) + ' is NULL and '

    command = command[:-4]
    res = self.dbR_._query( command )
    if res['OK']:
        if len( res['Value'] ) == 0:
          command = 'select DaqPeriodId from data_taking_conditions where '
          for param in condition:
            if param != 'Description':
              if type( condition[param] ) == types.StringType and len( condition[param].strip() ) == 0:
                command += str( param ) + ' is NULL and '
              elif condition[param] != None:
                command += str( param ) + '=\'' + condition[param] + '\' and '
              else:
                command += str( param ) + ' is NULL and '

          command = command[:-4]
          retVal = self.dbR_._query( command )
          if retVal['OK']:
            if len( retVal['Value'] ) != 0:
              return S_ERROR( 'Only the Description is different, the other attributes are the same and they are exists in the DB!' )
    return res

  #############################################################################
  def getDataTakingCondDesc( self, condition ):
    command = 'select description from data_taking_conditions where '
    for param in condition:
      if type( condition[param] ) == types.StringType and len( condition[param].strip() ) == 0:
        command += str( param ) + ' is NULL and '
      elif condition[param] != None:
        command += str( param ) + '=\'' + condition[param] + '\' and '
      else:
        command += str( param ) + ' is NULL and '

    command = command[:-4]
    res = self.dbR_._query( command )
    if res['OK']:
        if len( res['Value'] ) == 0:
          command = 'select DaqPeriodId from data_taking_conditions where '
          for param in condition:
            if param != 'Description':
              if type( condition[param] ) == types.StringType and len( condition[param].strip() ) == 0:
                command += str( param ) + ' is NULL and '
              elif condition[param] != None:
                command += str( param ) + '=\'' + condition[param] + '\' and '
              else:
                command += str( param ) + ' is NULL and '

          command = command[:-4]
          retVal = self.dbR_._query( command )
          if retVal['OK']:
            if len( retVal['Value'] ) != 0:
              return S_ERROR( 'Only the Description is different, the other attributes are the same and they are exists in the DB!' )
    return res

  #############################################################################
  def getStepIdandNameForRUN( self, programName, programVersion ):
    command = "select stepid, stepname from steps where applicationname='%s' and applicationversion='%s'" % ( programName, programVersion )
    retVal = self.dbR_._query( command )
    if retVal['OK']:
      stepName = 'Real Data'
      if len(retVal['Value']) == 0:
        retVal = self.insertStep({'Step':{'StepName':stepName,'ApplicationName':programName,'ApplicationVersion':programVersion,'ProcessingPass':stepName},'OutputFileTypes':[{'FileType':'RAW','Visible':'Y'}]})
        if retVal['OK']:
          return S_OK( [retVal['Value'], stepName] )
        else:
          return retVal
      else:
        return S_OK( [retVal['Value'][0][0], retVal['Value'][0][1]] )
    else:
      return retVal

  #############################################################################
  def __getPassIds( self, name ):
    command = "select id from processing where name='%s'" % ( name )
    retVal = self.dbR_._query( command )
    if retVal['OK']:
      result = []
      for i in retVal['Value']:
        result += [i[0]]
      return S_OK( result )
    else:
      return retVal

  #############################################################################
  def __getprocessingid( self, id ):
    command = 'SELECT name "Name", CONNECT_BY_ISCYCLE "Cycle", \
   LEVEL, SYS_CONNECT_BY_PATH(name, \'/\') "Path", id "ID" \
   FROM processing \
   START WITH id='+str(id)+'\
   CONNECT BY NOCYCLE PRIOR  parentid=id AND LEVEL <= 5 \
   ORDER BY  Level desc, "Name", "Cycle", "Path"'
    return self.dbR_._query( command )

  #############################################################################
  def __checkprocessingpass( self, opath, values ):
    if len( opath ) != len( values ):
      return False
    else:
      j = 0
      for i in values:
        if i[0] != opath[j]:
          return False
        j += 1
      return True

  #############################################################################
  def __insertprocessing(self, values, parentid=None, ids = []):

    for i in values:
      command = ''
      if parentid != None:
        command = "select id from processing where name='%s' and parentid=%s" % ( i, parentid )
      else:
        command = "select id from processing where name='%s' and parentid is null" % ( i )
      retVal = self.dbR_._query( command )
      if retVal['OK']:
        if len( retVal['Value'] ) == 0:
          if parentid != None:
            command = 'select max(id)+1 from processing'
            retVal = self.dbR_._query( command )
            if retVal['OK']:
              id = retVal['Value'][0][0]
              ids += [id]
              command = "insert into processing(id,parentid,name)values(%d,%d,'%s')"%(id,parentid,i)
              retVal = self.dbW_._query(command)
              if not retVal['OK']:
                gLogger.error(retVal['Message'])
              values.remove(i)
              self.__insertprocessing(values, id, ids)
          else:
            command = 'select max(id)+1 from processing'
            retVal = self.dbR_._query( command )
            if retVal['OK']:
              id = retVal['Value'][0][0]
              if id == None:
                id = 1
              ids += [id]
              command = "insert into processing(id,parentid,name)values(%d,null,'%s')"%(id,i)
              retVal = self.dbW_._query(command)
              if not retVal['OK']:
                gLogger.error(retVal['Message'])
              values.remove(i)
              self.__insertprocessing(values, id, ids)
        else:

          values.remove(i)
          parentid = retVal['Value'][0][0]
          ids += [parentid]
          self.__insertprocessing(values,parentid, ids)


  #############################################################################
  def addProcessing(self, path):

    lastindex = len(path)-1
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
          if len(procs)>0:
            if self.__checkprocessingpass(path, procs):
              return S_OK(ppp[len(ppp)-1][4])
        newpath = list(path)
        self.__insertprocessing(newpath, None, stepids)
        return S_OK(stepids[-1:])
    return S_ERROR()

  #############################################################################
  def insertStepsContainer( self, prod, stepid, step ):
    return self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.insertStepsContainer', [prod, stepid, step], False )

  #############################################################################
  def insertproductionscontainer( self, prod, processingid, simid, daqperiodid ):
    return self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.insertproductionscontainer', [ prod, processingid, simid, daqperiodid], False )

  #############################################################################
  def addProductionSteps( self, steps, prod ):
    level = 1
    for i in steps:
      retVal = self.insertStepsContainer( prod, i['StepId'], level )
      if not retVal['OK']:
        return retVal
      level += 1
    return S_OK()

  #############################################################################
  def checkProcessingPassAndSimCond( self, production ):
    command = ' select count(*) from productionscontainer where production=' + str( production )
    res = self.dbR_._query( command )
    return res

  #############################################################################
  def addProduction( self, production, simcond = None, daq = None, steps = default, inputproc = '' ):

    path = []
    if inputproc != '':
      if inputproc[0] != '/': inputproc = '/'+inputproc
      path = inputproc.split('/')[1:]

    for i in steps:
      if i['Visible']=='Y':
        res = self.getAvailableSteps({'StepId':i['StepId']})
        if not res['OK']:
          gLogger.error(res['Message'])
          return res
        if len(res['Value']) > 0:
          procpas = res['Value'][0][9]
          path += [procpas]
        else:
          return S_ERROR('This step is missing')

    if len(path) == 0:
      return S_ERROR('You have to define the input processing pass or you have to have a visible step!')
    processingid = None
    retVal = self.addProcessing( path )
    if not retVal['OK']:
      return retVal
    else:
      if len( retVal['Value'] ) > 0:
        processingid = retVal['Value'][0]
      else:
        return S_ERROR( 'The proccesing pass exist! You have to ask Zoltan!' )
    retVal = self.addProductionSteps( steps, production )
    if retVal['OK']:
      sim = None
      did = None
      if daq != None:
        retVal = self._getDataTakingConditionId( daq )
        if retVal['OK'] and retVal['Value'] > -1:
          did = retVal['Value']
        else:
          return S_ERROR( 'Data taking condition is missing!!' )
      if simcond != None:
        retVal = self._getSimulationConditioId( simcond )
        if retVal['OK'] and retVal['Value'] > -1:
          sim = retVal['Value']
        else:
          return S_ERROR( 'Data taking condition is missing!!' )
      return self.insertproductionscontainer( production, processingid, sim, did )
    else:
      return retVal
    return S_OK( 'The production processing pass is entered to the bkk' )

  #############################################################################
  def getEventTypes( self, configName, configVersion ):
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getEventTypes', [configName, configVersion] )

  #############################################################################
  def getStandardEventTypes( self, configName = default, configVersion = default, prod = default ):
    condition = ''
    if configName != default:
      condition += " prodview.configname='%s' " % ( configName )

    if configVersion != default:
      condition += " and prodview.configversion='%s' " % ( configVersion )

    if prod != default:
      if condition == '':
        condition += ' prodview.production=' + str( prod )
      else:
        condition += ' and prodview.production=' + str( prod )

    command = ' select prodview.eventtypeid, prodview.description from  prodview where ' + condition
    retVal = self.dbR_._query( command )
    records = []
    if retVal['OK']:
      parameters = ['EventTypeId', 'Description']
      for record in retVal['Value']:
        records += [[record[0], record[1]]]
    else:
      return retVal

    return S_OK( {'ParameterNames':parameters, 'Records':records, 'TotalRecords':len( records )} )

  #############################################################################
  def getProcessingPassSteps( self, procpass = default, cond = default, stepname = default ):

    processing = {}
    condition = ''

    if procpass != default:
      condition += " and prod.processingid in ( \
                    select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                        FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                        CONNECT BY NOCYCLE PRIOR  id=parentid) v where v.path='%s' \
                       )"%(procpass.split('/')[1], procpass)

    if cond!=default:
      retVal = self._getConditionString(cond,'prod')
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal


    if stepname != default:
      condition += " and s.processingpass='%s' " % ( stepname )

    command = "select distinct s.stepid,s.stepname,s.applicationname,s.applicationversion, s.optionfiles,s.dddb, s.conddb,s.extrapackages,s.visible, cont.step \
                from steps s, productionscontainer prod, stepscontainer cont \
               where \
              cont.stepid=s.stepid and \
              prod.production=cont.production %s order by cont.step"%(condition)

    retVal = self.dbR_._query(command)
    records = []
    #parametersNames = [ 'StepId', 'StepName','ApplicationName', 'ApplicationVersion','OptionFiles','DDDB','CONDDB','ExtraPackages','Visible']
    parametersNames = ['id', 'name']
    if retVal['OK']:
      nb = 0
      for i in retVal['Value']:
        #records = [[i[0],i[1],i[2],i[3],i[4],i[5],i[6], i[7], i[8]]]
        records = [ ['StepId', i[0]], ['StepName', i[1]], ['ApplicationName', i[2]], ['ApplicationVersion', i[3]], ['OptionFiles', i[4]], ['DDDB', i[5]], ['CONDDB', i[6]], ['ExtraPackages', i[7]], ['Visible', i[8]]]
        step = 'Step-%s' % ( i[0] )
        processing[step] = records
        nb += 1
    else:
      return retVal

    return S_OK( {'Parameters':parametersNames, 'Records':processing, 'TotalRecords':nb} )

  #############################################################################
  def getProductionProcessingPassSteps( self, prod ):
    processing = {}
    retVal = self.getProductionProcessingPass( prod )
    if retVal['OK']:
      procpass = retVal['Value']

    condition = ''


    if procpass != default:
      condition += " and prod.processingid in ( \
                    select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                        FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                        CONNECT BY NOCYCLE PRIOR  id=parentid) v where v.path='%s' \
                       )"%(procpass.split('/')[1], procpass)

    command = "select distinct s.stepid,s.stepname,s.applicationname,s.applicationversion, s.optionfiles,s.dddb, s.conddb,s.extrapackages,s.visible, cont.step \
                from steps s, productionscontainer prod, stepscontainer cont \
               where \
              cont.stepid=s.stepid and \
              prod.production=cont.production %s and prod.production=%dorder by cont.step"%(condition,prod)

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

    return S_OK({'Parameters':parametersNames,'Records':processing, 'TotalRecords':nb})

  #############################################################################
  def getRuns(self, cName, cVersion):
    return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getRuns', [cName, cVersion])

  #############################################################################
  def getRunProcPass(self, runnb):
    command = "select distinct runnumber, processingpass from table (BOOKKEEPINGORACLEDB.getRunProcPass(%d))"%(runnb)
    return self.dbR_._query(command)
    #return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getRunProcPass', [runnb])

  #############################################################################
  def getRunNbFiles(self, runid, eventtype):
    condition = ''
    if eventtype != default:
      condition = ' and f.eventtypeid=%s'%(str(eventtype))

    command = ' select count(*) from jobs j, files f where j.jobid=f.jobid and j.production<0 and j.runnumber=%s %s'%(str(runid),condition)
    return self.dbR_._query(command)