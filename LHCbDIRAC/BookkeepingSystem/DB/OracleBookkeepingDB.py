"""
Queries creation
"""

__RCSID__ = "$Id$"

import datetime
import types
import re

from DIRAC                                                           import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config                         import gConfig
from DIRAC.ConfigurationSystem.Client.PathFinder                     import getDatabaseSection
# from DIRAC.Core.Utilities.OracleDB                                   import OracleDB
from LHCbDIRAC.BookkeepingSystem.DB.OracleDB                         import OracleDB
from DIRAC.Core.Utilities.List                                       import breakListIntoChunks

global ALLOWED_ALL
ALLOWED_ALL = 2

global default
default = 'ALL'

class OracleBookkeepingDB:
  """This class provides all the methods which manipulate the database"""
  #############################################################################
  def __init__( self ):
    """
    """
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
  @staticmethod
  def __isSpecialFileType( flist ):
    """decide the type of a file"""
    result = False
    if ( 'RAW' in flist ) or ( 'MDF' in flist ):
      result = True
    else:
      for i in flist:
        if re.search( 'HIST', i ):
          result = True
          break
    return result

  #############################################################################
  def getAvailableSteps( self, in_dict ):
    """
    I never implemented this kind of method in my life. I have to start some time!
    """
    start = 0
    maximum = 10
    paging = False
    retVal = None
    fileTypefilter = None

    condition = ''
    tables = 'steps s, steps r, runtimeprojects rr '
    isMulticore = in_dict.get( 'isMulticore', default )
    if isMulticore.upper() != default:
      if isMulticore.upper() in ['Y', 'N']:
        condition += " and s.isMulticore='%s'" % ( isMulticore )
      else:
        return S_ERROR( 'isMulticore is not Y or N!' )
    result = S_ERROR()
    if len( in_dict ) > 0:
      infiletypes = in_dict.get( 'InputFileTypes', default )
      outfiletypes = in_dict.get( 'OutputFileTypes', default )
      matching = in_dict.get( 'Equal', 'YES' )

      if isinstance( matching, bool ):
        if matching:
          matching = "YES"
        else:
          matching = 'NO'
      elif matching.upper() not in ['YES', 'NO']:
        return S_ERROR ( 'Wrong Equal value!' )

      if infiletypes != default or outfiletypes != default:
        if isinstance( infiletypes, basestring ):
          infiletypes = []
        if isinstance( outfiletypes, basestring ):
          outfiletypes = []
        infiletypes.sort()
        outfiletypes.sort()
        values = 'lists( '
        for i in infiletypes:
          values += "'%s'," % ( i )
        inp = values[:-1] + ')'

        values = 'lists( '
        for i in outfiletypes:
          values += "'%s'," % ( i )
        out = values[:-1] + ')'

        fileTypefilter = " table(BOOKKEEPINGORACLEDB.getStepsForFiletypes(%s, %s, '%s')) s \
                                   " % ( inp, out, matching.upper() )

      startDate = in_dict.get( 'StartDate', default )
      if startDate != default:
        condition += " and s.inserttimestamps >= TO_TIMESTAMP (' %s ' ,'YYYY-MM-DD HH24:MI:SS')" % ( startDate )

      stepId = in_dict.get( 'StepId', default )
      if stepId != default:
        condition += ' and s.stepid= %s' % ( str( stepId ) )

      stepName = in_dict.get( 'StepName', default )
      if stepName != default:
        if isinstance( stepName, basestring ):
          condition += " and s.stepname='%s'" % ( stepName )
        elif isinstance( stepName, list ):
          values = ' and ('
          for i in stepName:
            values += " s.stepname='%s' or " % ( i )
          condition += values[:-3] + ')'

      appName = in_dict.get( 'ApplicationName', default )
      if appName != default:
        if isinstance( appName, basestring ):
          condition += " and s.applicationName='%s'" % ( appName )
        elif isinstance( appName, list ):
          values = ' and ('
          for i in appName:
            values += " s.applicationName='%s' or " % ( i )
          condition += values[:-3] + ')'

      appVersion = in_dict.get( 'ApplicationVersion', default )
      if appVersion != default:
        if isinstance( appVersion, basestring ):
          condition += " and s.applicationversion='%s'" % ( appVersion )
        elif isinstance( appVersion, list ):
          values = ' and ('
          for i in appVersion:
            values += " s.applicationversion='%s' or " % ( i )
          condition += values[:-3] + ')'

      optFile = in_dict.get( 'OptionFiles', default )
      if optFile != default:
        if isinstance( optFile, basestring ):
          condition += " and s.optionfiles='%s'" % ( optFile )
        elif isinstance( optFile, list ):
          values = ' and ('
          for i in optFile:
            values += " s.optionfiles='%s' or " % ( i )
          condition += values[:-3] + ')'

      dddb = in_dict.get( 'DDDB', default )
      if dddb != default:
        if isinstance( dddb, basestring ):
          condition += " and s.dddb='%s'" % ( dddb )
        elif isinstance( dddb, list ):
          values = ' and ('
          for i in dddb:
            values += " s.dddb='%s' or " % ( i )
          condition += values[:-3] + ')'

      conddb = in_dict.get( 'CONDDB', default )
      if conddb != default:
        if isinstance( conddb, basestring ):
          condition += " and s.conddb='%s'" % ( conddb )
        elif isinstance( conddb, list ):
          values = ' and ('
          for i in conddb:
            values += " s.conddb='%s' or " % ( i )
          condition += values[:-3] + ')'

      extraP = in_dict.get( 'ExtraPackages', default )
      if extraP != default:
        if isinstance( extraP, basestring ):
          condition += " and s.extrapackages='%s'" % ( extraP )
        elif isinstance( extraP, list ):
          values = ' and ('
          for i in extraP:
            values += " s.extrapackages='%s' or " % ( i )
          condition += values + ')'

      visible = in_dict.get( 'Visible', default )
      if visible != default:
        if isinstance( visible, basestring ):
          condition += " and s.visible='%s'" % ( visible )
        elif isinstance( visible, list ):
          values = ' and ('
          for i in visible:
            values += " s.visible='%s' or " % ( i )
          condition += values[:-3] + ')'

      procPass = in_dict.get( 'ProcessingPass', default )
      if procPass != default:
        if isinstance( procPass, basestring ):
          condition += " and s.processingpass like'%%%s%%'" % ( procPass )
        elif isinstance( procPass, list ):
          values = ' and ('
          for i in procPass:
            values += " s.processingpass like '%%%s%%' or " % ( i )
          condition += values[:-3] + ')'

      usable = in_dict.get( 'Usable', default )
      if usable != default:
        if isinstance( usable, basestring ):
          condition += " and s.usable='%s'" % ( usable )
        elif isinstance( usable, list ):
          values = ' and ('
          for i in usable:
            values += " s.usable='%s' or " % ( i )
          condition += values[:-3] + ')'

      runtimeProject = in_dict.get( 'RuntimeProjects', default )
      if runtimeProject != default:
        condition += " and s.runtimeProject=%d" % ( runtimeProject )

      dqtag = in_dict.get( 'DQTag', default )
      if dqtag != default:
        if isinstance( dqtag, basestring ):
          condition += " and s.dqtag='%s'" % ( dqtag )
        elif isinstance( dqtag, list ):
          values = ' and ('
          for i in dqtag:
            values += "  s.dqtag='%s' or " % ( i )
          condition += values[:-3] + ')'

      optsf = in_dict.get( 'OptionsFormat', default )
      if optsf != default:
        if isinstance( optsf, basestring ):
          condition += " and s.optionsFormat='%s'" % ( optsf )
        elif isinstance( optsf, list ):
          values = ' and ('
          for i in optsf:
            values += " s.optionsFormat='%s' or " % ( i )
          condition += values[:-3] + ')'

      sysconfig = in_dict.get( 'SystemConfig', default )
      if sysconfig != default:
        condition += " and s.systemconfig='%s'" % sysconfig

      mcTck = in_dict.get( 'mcTCK', default )
      if mcTck != default:
        condition += " and s.mcTCK='%s'" % mcTck

      start = in_dict.get( 'StartItem', default )
      maximum = in_dict.get( 'MaxItem', default )

      if start != default and maximum != default:
        paging = True

      sort = in_dict.get( 'Sort', default )
      if sort != default:
        condition += 'Order by '
        order = sort.get( 'Order', 'Asc' )
        if order.upper() not in ['ASC', 'DESC']:
          return S_ERROR( "wrong sorting order!" )
        items = sort.get( 'Items', default )
        if isinstance( items, list ):
          order = ''
          for item in items:
            order += 's.%s,' % ( item )
          condition += ' %s %s' % ( order[:-1], order )
        elif isinstance( items, basestring):
          condition += ' s.%s %s' % ( items, order )
        else:
          result = S_ERROR( 'SortItems is not properly defined!' )
      else:
        condition += ' order by s.inserttimestamps desc'
      if fileTypefilter:
        if paging:
          command = " select sstepid, sname, sapplicationname, sapplicationversion, soptionfiles, \
                    sdddb, sconddb, sextrapackages, svisible, sprocessingpass, susable, \
                    sdqtag, soptsf, smulti, ssysconfig, smcTck, \
                     rsstepid, rsname, rsapplicationname, rsapplicationversion, rsoptionfiles, rsdddb, \
                     rsconddb, rsextrapackages, rsvisible, rsprocessingpass, rsusable, \
                     rdqtag, roptsf, rmulti, rsysconfig, rmcTck from \
  ( select ROWNUM r , sstepid, sname, sapplicationname, sapplicationversion, soptionfiles, sdddb, sconddb,\
  sextrapackages, svisible, sprocessingpass, susable, sdqtag, soptsf, smulti, ssysconfig, smcTck,\
     rsstepid, rsname, rsapplicationname, rsapplicationversion, rsoptionfiles, rsdddb, rsconddb,\
     rsextrapackages, rsvisible, rsprocessingpass, rsusable , rdqtag, roptsf, rmulti, rsysconfig, rmcTck from \
    ( select ROWNUM r, s.stepid sstepid ,s.stepname sname, s.applicationname sapplicationname,\
    s.applicationversion sapplicationversion, s.optionfiles soptionfiles,\
    s.DDDB sdddb,s.CONDDB sconddb, s.extrapackages sextrapackages,s.Visible svisible ,\
    s.ProcessingPass sprocessingpass, s.Usable susable, s.dqtag sdqtag, s.optionsFormat soptsf,\
     s.isMulticore smulti, s.systemconfig ssysconfig, s.mcTCK smcTck, \
    s.rstepid rsstepid ,s.rstepname rsname, s.rapplicationname rsapplicationname,\
    s.rapplicationversion rsapplicationversion, s.roptionfiles rsoptionfiles,\
    s.rDDDB rsdddb,s.rCONDDB rsconddb, s.rextrapackages rsextrapackages,s.rVisible rsvisible , s.rProcessingPass rsprocessingpass,\
     s.rUsable rsusable, s.rdqtag rdqtag, s.roptionsFormat roptsf, s.risMulticore rmulti, s.rsystemconfig rsysconfig, s.mcTCK rmcTck \
    from %s where s.stepid=s.stepid %s \
     ) where rownum <=%d ) where r >%d" % ( fileTypefilter, condition, maximum, start )
        else:
          command = " select * from %s where s.stepid=s.stepid %s" % ( fileTypefilter, condition )
      elif paging:
        command = "select sstepid, sname, sapplicationname, sapplicationversion, soptionfiles, \
                    sdddb, sconddb, sextrapackages, svisible, sprocessingpass, susable, \
                    sdqtag, soptsf, smulti, ssysconfig, smcTck, \
                     rsstepid, rsname, rsapplicationname, rsapplicationversion, rsoptionfiles, rsdddb, \
                     rsconddb, rsextrapackages, rsvisible, rsprocessingpass, rsusable, \
                     rdqtag, roptsf, rmulti, rsysconfig, rmcTck from \
  ( select ROWNUM r , sstepid, sname, sapplicationname, sapplicationversion, soptionfiles, sdddb, sconddb,\
  sextrapackages, svisible, sprocessingpass, susable, sdqtag, soptsf, smulti, ssysconfig, smcTck,\
     rsstepid, rsname, rsapplicationname, rsapplicationversion, rsoptionfiles, rsdddb, rsconddb,\
     rsextrapackages, rsvisible, rsprocessingpass, rsusable , rdqtag, roptsf, rmulti, rsysconfig, rmcTck from \
    ( select ROWNUM r, s.stepid sstepid ,s.stepname sname, s.applicationname sapplicationname,\
    s.applicationversion sapplicationversion, s.optionfiles soptionfiles,\
    s.DDDB sdddb,s.CONDDB sconddb, s.extrapackages sextrapackages,s.Visible svisible ,\
    s.ProcessingPass sprocessingpass, s.Usable susable, s.dqtag sdqtag, s.optionsFormat soptsf,\
     s.isMulticore smulti, s.systemconfig ssysconfig, s.mcTCK smcTck, \
    r.stepid rsstepid ,r.stepname rsname, r.applicationname rsapplicationname,\
    r.applicationversion rsapplicationversion, r.optionfiles rsoptionfiles,\
    r.DDDB rsdddb,r.CONDDB rsconddb, r.extrapackages rsextrapackages,r.Visible rsvisible , r.ProcessingPass rsprocessingpass,\
     r.Usable rsusable, r.dqtag rdqtag, r.optionsFormat roptsf, r.isMulticore rmulti, r.systemconfig rsysconfig, r.mcTCK rmcTck \
    from %s where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid %s \
     ) where rownum <=%d ) where r >%d" % ( tables, condition, maximum, start )

      else:
        command = 'select s.stepid,s.stepname, s.applicationname,s.applicationversion,s.optionfiles,s.DDDB,s.CONDDB,\
         s.extrapackages,s.Visible, s.ProcessingPass, s.Usable, s.dqtag, s.optionsformat, s.ismulticore, s.systemconfig, s.mcTCK, \
        r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,\
        r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.ismulticore, r.systemconfig, r.mcTCK \
        from %s where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid  %s ' % ( tables, condition )
      retVal = self.dbR_.query( command )
    else:
      command = 'select s.stepid, s.stepname, s.applicationname,s.applicationversion,s.optionfiles,s.DDDB,s.CONDDB, \
      s.extrapackages,s.Visible, s.ProcessingPass, s.Usable, s.dqtag, s.optionsformat, s.isMulticore, s.systemconfig, s.mcTCK,\
      r.stepid, r.stepname, r.applicationname,r.applicationversion,r.optionfiles,r.DDDB,r.CONDDB, r.extrapackages,\
      r.Visible, r.ProcessingPass, r.Usable, r.dqtag, r.optionsformat, r.ismulticore, r.systemconfig, r.mcTCK \
      from %s where s.stepid=rr.stepid(+) and r.stepid(+)=rr.runtimeprojectid ' % ( tables )
      retVal = self.dbR_.query( command )

    if retVal['OK']:
      parameters = ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion', 'OptionFiles', 'DDDB',
                    'CONDDB', 'ExtraPackages', 'Visible', 'ProcessingPass', 'Usable', 'DQTag', 'OptionsFormat',
                    'isMulticore', 'SystemConfig', 'mcTCK', 'RuntimeProjects']
      rParameters = ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion', 'OptionFiles',
                     'DDDB', 'CONDDB', 'ExtraPackages', 'Visible', 'ProcessingPass', 'Usable', 'DQTag',
                     'OptionsFormat', 'isMulticore', 'SystemConfig', 'mcTCK']
      records = []
      for record in retVal['Value']:
        step = list( record[0:16] )
        runtimeProject = []
        runtimeProject = [ rec for rec in list( record[16:] ) if rec != None]
        if len( runtimeProject ) > 0:
          runtimeProject = [runtimeProject]
        step += [{'ParameterNames':rParameters, 'Records':runtimeProject, 'TotalRecords':len( runtimeProject ) + 1}]
        records += [step]
      if paging:
        if fileTypefilter:
           command = "select count(*) from %s where s.stepid>0 %s " % ( fileTypefilter, condition )
        else:
          command = "select count(*) from steps s where s.stepid>0 %s " % ( condition )
          
        retVal = self.dbR_.query( command )
        if retVal['OK']:
          totrec = retVal['Value'][0][0]
          result = S_OK( {'ParameterNames':parameters, 'Records':records, 'TotalRecords':totrec} )
        else:
          result = retVal
      else:
        result = S_OK( {'ParameterNames':parameters, 'Records':records, 'TotalRecords':len( records )} )
    else:
      result = S_ERROR( retVal['Message'] )
    return result

  #############################################################################
  def getRuntimeProjects( self, in_dict ):
    """get runtime projects"""
    result = S_ERROR()
    condition = ''
    selection = 's.stepid,stepname, s.applicationname,s.applicationversion,s.optionfiles,s.DDDB,CONDDB,\
     s.extrapackages,s.Visible, s.ProcessingPass, s.Usable, s.DQTag, s.optionsformat, s.ismulticore, s.systemconfig, s.mcTCK'
    tables = 'steps s, runtimeprojects rp'
    stepId = in_dict.get( 'StepId', default )
    if stepId != default:
      condition += " rp.stepid=%d" % ( stepId )
      command = " select %s from %s where s.stepid=rp.runtimeprojectid and %s" % ( selection, tables, condition )
      retVal = self.dbR_.query( command )
      if retVal['OK']:
        parameters = ['StepId', 'StepName', 'ApplicationName', 'ApplicationVersion', 'OptionFiles',
                      'DDDB', 'CONDDB', 'ExtraPackages', 'Visible', 'ProcessingPass', 'Usable', 'DQTag',
                      'OptionsFormat', 'isMulticore', 'SystemConfig', 'mcTCK']
        records = []
        for record in retVal['Value']:
          records += [list( record )]
        result = S_OK( {'ParameterNames':parameters, 'Records':records, 'TotalRecords':len( records )} )
      else:
        result = retVal
    else:
      result = S_ERROR( 'You must provide a StepId!' )
    return result


  #############################################################################
  def getStepInputFiles( self, stepId ):
    """input files of a given step"""
    command = 'select inputFiletypes.name,inputFiletypes.visible from steps, \
     table(steps.InputFileTypes) inputFiletypes where steps.stepid=' + str( stepId )
    return self.dbR_.query( command )

  #############################################################################
  def setStepInputFiles( self, stepid, files ):
    """set input files to a given step"""
    files = sorted( files, key = lambda k: k['FileType'] )
    if len( files ) == 0:
      values = 'null'
    else:
      values = 'filetypesARRAY('
      for i in files:
        fileType = i.get( 'FileType', default )
        visible = i.get( 'Visible', default )
        if fileType != default and visible != default:
          values += "ftype('%s','%s')," % ( fileType, visible )
      values = values[:-1]
      values += ')'
    command = "update steps set inputfiletypes=%s where stepid=%s" % ( values, str( stepid ) )
    return self.dbW_.query( command )

  #############################################################################
  def setStepOutputFiles( self, stepid, files ):
    """set output files to a given step"""
    files = sorted( files, key = lambda k: k['FileType'] )
    if len( files ) == 0:
      values = 'null'
    else:
      values = 'filetypesARRAY('
      for i in files:
        fileType = i.get( 'FileType', default )
        visible = i.get( 'Visible', default )
        if fileType != default and visible != default:
          values += "ftype('%s','%s')," % ( fileType, visible )
      values = values[:-1]
      values += ')'
    command = "update steps set Outputfiletypes=%s  where stepid=%s" % ( values, str( stepid ) )
    return self.dbW_.query( command )

  #############################################################################
  def getStepOutputFiles( self, stepId ):
    """returns the output file for a given step"""
    command = 'select outputfiletypes.name,outputfiletypes.visible from steps, \
    table(steps.outputfiletypes) outputfiletypes where  steps.stepid=' + str( stepId )
    return self.dbR_.query( command )

  #############################################################################
  def getProductionOutputFileTypes( self, prod ):
    """returns the production output file types"""
    command = "select o.name,o.visible from steps s, table(s.outputfiletypes) o, stepscontainer st \
            where st.stepid=s.stepid and st.production=%d order by step" % ( int( prod ) )
    retVal = self.dbR_.query( command )
    values = {}
    if retVal['OK']:
      for i in retVal['Value']:
        values[i[0]] = i[1]
      result = S_OK( values )
    else:
      result = retVal
    return result

  #############################################################################
  def getAvailableFileTypes( self ):
    """returns the available file types"""
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getAvailableFileTypes', [] )

  #############################################################################
  def insertFileTypes( self, ftype, desc, fileType ):
    """inserts a given file type"""
    return self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.insertFileTypes', types.LongType, [ftype, desc, fileType] )

  #############################################################################
  def insertStep( self, in_dict ):
    """inserts a given step
     Dictionary format: {'Step': {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': '',
    'ApplicationVersion': 'v29r1', 'ext-comp-1273': 'CHARM.MDST (Charm micro dst)', 'ExtraPackages': '',
    'StepName': 'davinci prb2', 'ProcessingPass': 'WG-Coool', 'ext-comp-1264': 'CHARM.DST (Charm stream)',
    'Visible': 'Y', 'DDDB': '', 'OptionFiles': '', 'CONDDB': ''},
    'OutputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.MDST'}],
    'InputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.DST'}],
    'RuntimeProjects':[{'StepId':13878}]}"""
    result = S_ERROR()
    values = ''
    command = "SELECT applications_index_seq.nextval from dual"
    sid = 0
    retVal = self.dbR_.query( command )
    if not retVal['OK']:
      result = retVal
    else:
      sid = retVal['Value'][0][0]

    selection = 'insert into steps(stepid,stepname,applicationname,applicationversion,OptionFiles,dddb,conddb,\
    extrapackages,visible, processingpass, usable, DQTag, optionsformat,isMulticore, SystemConfig, mcTCK'
    inFileTypes = in_dict.get( 'InputFileTypes', default )
    if inFileTypes != default:
      inFileTypes = sorted( inFileTypes, key = lambda k: k['FileType'] )
      values = ',filetypesARRAY('
      selection += ',InputFileTypes'
      for i in inFileTypes:
        values += "ftype('%s', '%s')," % ( ( i.get( 'FileType', None ).strip() if i.get( 'FileType', None )
                                             else i.get( 'FileType', None ) ),
                                          ( i.get( 'Visible', None ).strip() if i.get( 'Visible', None )
                                            else i.get( 'Visible', None ) ) )
      values = values[:-1]
      values += ')'

    outFileTypes = in_dict.get( 'OutputFileTypes', default )
    if outFileTypes != default:
      outFileTypes = sorted( outFileTypes, key = lambda k: k['FileType'] )
      values += ' , filetypesARRAY('
      selection += ',OutputFileTypes'
      for i in outFileTypes:
        values += "ftype('%s', '%s')," % ( ( i.get( 'FileType', None ).strip() if i.get( 'FileType', None )
                                           else i.get( 'FileType', None ) ),
                                          ( i.get( 'Visible', None ).strip() if i.get( 'Visible', None )
                                           else i.get( 'Visible', None ) ) )
      values = values[:-1]
      values += ')'

    step = in_dict.get( 'Step', default )
    if step != default:
      command = selection + ")values(%d" % ( sid )
      command += ",'%s'" % ( step.get( 'StepName', 'NULL' ) )
      command += ",'%s'" % ( step.get( 'ApplicationName', 'NULL' ) )
      command += ",'%s'" % ( step.get( 'ApplicationVersion', 'NULL' ) )
      command += ",'%s'" % ( step.get( 'OptionFiles', 'NULL' ) )
      command += ",'%s'" % ( step.get( 'DDDB', 'NULL' ) )
      command += ",'%s'" % ( step.get( 'CONDDB', 'NULL' ) )
      command += ",'%s'" % ( step.get( 'ExtraPackages', 'NULL' ) )
      command += ",'%s'" % ( step.get( 'Visible', 'NULL' ) )
      command += ",'%s'" % ( step.get( 'ProcessingPass', 'NULL' ) )
      command += ",'%s'" % ( step.get( 'Usable', 'Not ready' ) )
      command += ",'%s'" % ( step.get( 'DQTag', '' ) )
      command += ",'%s'" % ( step.get( 'OptionsFormat', '' ) )
      command += ",'%s'" % ( step.get( 'isMulticore', 'N' ) )
      command += ",'%s'" % ( step.get( 'SystemConfig', 'NULL' ) )
      command += ",'%s'" % ( step.get( 'mcTCK', 'NULL' ) )
      command += values + ")"
      retVal = self.dbW_.query( command )
      if retVal['OK']:
        r_project = in_dict.get( 'RuntimeProjects', step.get( 'RuntimeProjects', default ) )
        if r_project != default:
          for i in r_project:
            rid = i['StepId']
            retVal = self.insertRuntimeProject( sid, rid )
            if not retVal['OK']:
              result = retVal
            else:
              result = S_OK( sid )
        else:
          result = S_OK( sid )
      else:
        result = retVal
    else:
      result = S_ERROR( 'The Step is not provided!' )
    return result

  #############################################################################
  def deleteStep( self, stepid ):
    """deletes a step"""
    result = S_ERROR()
    command = " delete runtimeprojects where stepid=%d" % ( stepid )
    retVal = self.dbW_.query( command )
    if not retVal['OK']:
      result = retVal
    else:
      # now we can delete the step
      command = "delete steps where stepid=%d" % ( stepid )
      result = self.dbW_.query( command )
    return result

  #############################################################################
  def deleteSetpContiner( self, prod ):
    """delete a production from the step container"""
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.deleteSetpContiner', [prod], False )
    return result

  #############################################################################
  def deleteProductionsContiner( self, prod ):
    """delete a production from the productions container"""
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.deleteProductionsCont', [prod], False )
    return result

  #############################################################################
  def updateStep( self, in_dict ):
    """update an existing step.
    input data {'ApplicationName': 'DaVinci', 'Usable': 'Yes', 'StepId': '13860',
    'ApplicationVersion': 'v29r1', 'ExtraPackages': '', 'StepName': 'davinci prb3', 'ProcessingPass':
    'WG-Coool-new', 'InputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.DST'}], 'Visible': 'Y',
    'DDDB': '', 'OptionFiles': '', 'CONDDB': '',
    'OutputFileTypes': [{'Visible': 'Y', 'FileType': 'CHARM.MDST'}],
    'RuntimeProjects':[{'StepId':13879}]}"""
    result = S_ERROR()
    ok = True
    rProjects = in_dict.get( 'RuntimeProjects', default )
    if rProjects != default:
      if len( rProjects ) > 0:
        for i in rProjects:
          if 'StepId' not in in_dict:
            result = S_ERROR( 'The runtime project can not changed, because the StepId is missing!' )
            ok = False
          else:
            retVal = self.updateRuntimeProject( in_dict['StepId'], i['StepId'] )
            if not retVal['OK']:
              result = retVal
              ok = False
            else:
              in_dict.pop( 'RuntimeProjects' )
      else:
        retVal = self.removeRuntimeProject( in_dict['StepId'] )
        if not retVal['OK']:
          result = retVal
          ok = False
        else:
          in_dict.pop( 'RuntimeProjects' )

    if ok:
      stepid = in_dict.get( 'StepId', default )
      if stepid != default:
        condition = " where stepid=%s" % ( str( stepid ) )
        command = 'update steps set '
        for i in in_dict:
          if isinstance( in_dict[i], basestring):
            command += " %s='%s'," % ( i, str( in_dict[i] ) )
          else:
            if len( in_dict[i] ) > 0:
              values = 'filetypesARRAY('
              ftypes = in_dict[i]
              ftypes = sorted( ftypes, key = lambda k: k['FileType'] )
              for j in ftypes:
                filetype = j.get( 'FileType', default )
                visible = j.get( 'Visible', default )
                if filetype != default and visible != default:
                  values += "ftype('%s','%s')," % ( filetype.strip(), visible.strip() )
              values = values[:-1]
              values += ')'
              command += i + '=' + values + ','
            else:
              command += i + '=null,'
        command = command[:-1]
        command += condition
        result = self.dbW_.query( command )
      else:
        result = S_ERROR( 'Please provide a StepId!' )

    return result

  #############################################################################
  def getAvailableConfigNames( self ):
    """returns the available configuration names"""
    command = ' select distinct Configname from prodview'
    return self.dbR_.query( command )

  ##############################################################################
  def getAvailableConfigurations( self ):
    """returns the available configurations"""
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getAvailableConfigurations', [] )

  #############################################################################
  def getConfigVersions( self, configname ):
    """returns the configuration version for a givem configname"""
    result = S_ERROR()
    if configname != default:
      command = " select distinct configversion from prodview where\
       configname='%s'" % ( configname )
      result = self.dbR_.query( command )
    else:
      result = S_ERROR( 'You must provide a Configuration Name!' )
    return result

  #############################################################################
  def getConditions( self, configName, configVersion, evt ):
    """ returns the conditions for a given configuration name, version and event type"""
    condition = ''
    if configName != default:
      condition += " and prodview.configname='%s' " % ( configName )

    if configVersion != default:
      condition += " and prodview.configversion='%s' " % ( configVersion )


    if evt != default:
      condition += " and prodview.eventtypeid=%s" % ( str( evt ) )

    command = 'select distinct simulationConditions.SIMID,data_taking_conditions.DAQPERIODID,\
    simulationConditions.SIMDESCRIPTION, simulationConditions.BEAMCOND, \
    simulationConditions.BEAMENERGY, simulationConditions.GENERATOR,\
    simulationConditions.MAGNETICFIELD,simulationConditions.DETECTORCOND, \
    simulationConditions.LUMINOSITY, simulationconditions.G4settings, \
    data_taking_conditions.DESCRIPTION,data_taking_conditions.BEAMCOND, \
    data_taking_conditions.BEAMENERGY,data_taking_conditions.MAGNETICFIELD, \
    data_taking_conditions.VELO,data_taking_conditions.IT, \
    data_taking_conditions.TT,data_taking_conditions.OT,\
    data_taking_conditions.RICH1,data_taking_conditions.RICH2, \
    data_taking_conditions.SPD_PRS, data_taking_conditions.ECAL, \
    data_taking_conditions.HCAL, data_taking_conditions.MUON, data_taking_conditions.L0, data_taking_conditions.HLT,\
     data_taking_conditions.VeloPosition from simulationConditions,data_taking_conditions,prodview where \
      prodview.simid=simulationConditions.simid(+) and \
      prodview.DAQPERIODID=data_taking_conditions.DAQPERIODID(+)' + condition

    return self.dbR_.query( command )

  #############################################################################
  def getProcessingPass( self, configName, configVersion, conddescription,
                         runnumber, production, eventType = default, path = '/' ):
    """returns the processing pass for a given dataset"""
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
      retVal = self.__getConditionString( conddescription )
      if not retVal['OK']:
        return  retVal
      else:
        condition += retVal['Value']

    if production != default:
      condition += ' and prodview.production=' + str( production )
    tables = ''
    if runnumber != default:
      tables += ' , prodrunview '
      condition += ' and prodrunview.production=prodview.production and prodrunview.runnumber=%s' % ( str( runnumber ) )

    if eventType != default:
      condition += " and prodview.eventtypeid=%s" % ( str( eventType ) )

    proc = path.split( '/' )[len( path.split( '/' ) ) - 1]
    if proc != '':
      command = "select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='%s'" % ( path.split( '/' )[1], path )
      retVal = self.dbR_.query( command )
      if not retVal['OK']:
        return retVal
      pro = ''
      for i in retVal['Value']:
        pro += "%s," % ( str( i[0] ) )
      pro = pro[:-1]

      if pro == '':
        return S_ERROR( 'Empty Directory' )
      command = 'select distinct eventTypes.EventTypeId,\
       eventTypes.Description from eventtypes,prodview,productionscontainer,processing %s where \
        prodview.production=productionscontainer.production and \
        eventTypes.EventTypeId=prodview.eventtypeid and \
        productionscontainer.processingid=processing.id and \
        processing.id in (%s) %s' % ( tables, pro, condition )

      retVal = self.dbR_.query( command )
      if retVal['OK']:
        eparameters = ['EventType', 'Description']
        for record in retVal['Value']:
          erecords += [list( record )]
      else:
        return retVal

      command = "SELECT distinct name \
      FROM processing   where parentid in (%s) \
      START WITH id in (select distinct productionscontainer.processingid \
      from productionscontainer,prodview %s where \
      productionscontainer.production=prodview.production  %s )  CONNECT BY NOCYCLE PRIOR  parentid=id \
      order by name desc" % ( pro, tables, condition )
    else:
      command = 'SELECT distinct name \
      FROM processing  where parentid is null START WITH id in \
      (select distinct productionscontainer.processingid \
      from productionscontainer, prodview %s where \
      productionscontainer.production=prodview.production %s ) CONNECT BY NOCYCLE PRIOR  parentid=id \
      order by name desc' % ( tables, condition )
    retVal = self.dbR_.query( command )
    if retVal['OK']:
      precords = []
      pparameters = ['Name']
      for record in retVal['Value']:
        precords += [[record[0]]]
    else:
      return retVal

    return S_OK( [{'ParameterNames':pparameters,
                   'Records':precords,
                   'TotalRecords':len( precords )},
                  {'ParameterNames':eparameters,
                   'Records':erecords,
                   'TotalRecords':len( erecords )}] )

  #############################################################################
  def __getConditionString( self, conddescription, table = 'productionscontainer' ):
    """builds the condition for data taking/ simulation conditions"""
    condition = ''
    retVal = self.__getDataTakingConditionId( conddescription )
    if retVal['OK']:
      if retVal['Value'] != -1:
        condition += " and %s.DAQPERIODID=%s and %s.DAQPERIODID is not null " % ( table, str( retVal['Value'] ), table )
      else:
        retVal = self.__getSimulationConditioId( conddescription )
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
  def __getDataTakingConditionId( self, desc ):
    """returns the data taking conditions identifire"""
    command = 'select DAQPERIODID from data_taking_conditions where DESCRIPTION=\'' + str( desc ) + '\''
    retVal = self.dbR_.query( command )
    if retVal['OK']:
      if len( retVal['Value'] ) > 0:
        return S_OK( retVal['Value'][0][0] )
      else:
        return S_OK( -1 )
    else:
      return retVal

  #############################################################################
  def __getSimulationConditioId( self, desc ):
    """returns the simulation condition identifier"""
    command = "select simid from simulationconditions where simdescription='%s'" % ( desc )
    retVal = self.dbR_.query( command )
    if retVal['OK']:
      if len( retVal['Value'] ) > 0:
        return S_OK( retVal['Value'][0][0] )
      else:
        return S_OK( -1 )
    else:
      return retVal

  #############################################################################
  def getProductions( self, configName = default, configVersion = default,
                      conddescription = default, processing = default, evt = default,
                      visible = default, fileType = default, replicaFlag = default ):
    """return the production for a given dataset"""
    #### MUST BE REIMPLEMNETED!!!!!!
    ####
    ####
    
    condition = ''
    tables = ' files f,jobs j '
    
    retVal = self.__buildConfiguration( configName, configVersion, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildConditions( default, conddescription, condition, tables, visible )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    if evt != default and visible.upper().startswith( 'Y' ):
      tables += ' ,prodview bview'
      condition += '  and j.production=bview.production and bview.production=prod.production\
       and bview.eventtypeid=%s and f.eventtypeid=bview.eventtypeid ' % ( str( evt ) )
    elif evt != default:
      if tables.upper().find( 'FILES' ) < 0:
        tables += ' ,file f '
      if tables.upper().find( 'JOBS' ) < 0:
        tables += ' ,jobs j '
      condition += '  and f.eventtypeid=%s ' % ( str( evt ) )


    if fileType != default and visible.upper().startswith( 'Y' ):
      if tables.find( 'bview' ) > -1:
        condition += " and bview.filetypeid=ftypes.filetypeid "
      if isinstance( fileType, list ):
        values = ' and ftypes.name in ('
        for i in fileType:
          values += " '%s'," % ( i )
        condition += values[:-1] + ')'
      else:
        condition += " and ftypes.name='%s' " % ( str( fileType ) )
    else:
      if isinstance( fileType, list ):
        values = ' and ftypes.name in ('
        for i in fileType:
          values += " '%s'," % ( i )
        condition += values[:-1] + ')'
        condition += " and ftypes.filetypeid=f.filetypeid"
      elif fileType != default:
        condition += " and ftypes.name='%s' " % ( str( fileType ) )
        condition += " and ftypes.filetypeid=f.filetypeid"
    
    if fileType != default:
      if tables.upper().find( 'FILETYPES' ) < 0:
        tables += ',filetypes ftypes'

    retVal = self.__buildProcessingPass( processing, condition, tables, visible )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildReplicaflag( replicaFlag, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildVisibilityflag( visible, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    if tables.upper().find( 'FILES' ) > 0:
      condition += ' and f.jobid=j.jobid '  
            
    command = "select distinct prod.production from %s where 1=1  %s " % (tables, condition)
     
         
    return self.dbR_.query( command )

  #############################################################################
  def getFileTypes( self, configName, configVersion, conddescription = default,
                    processing = default, evt = default, runnb = default, production = default,
                    visible = default ):
    """returns the available file types"""
    condition = ''
    tables = ''
    if visible.upper().startswith( 'N' ):
      condition += " and  f.visibilityflag='N'"

    if visible.upper().startswith( 'Y' ):
      if configName != default:
        condition += " and bview.configname='%s' " % ( configName )

      if configVersion != default:
        condition += " and bview.configversion='%s' " % ( configVersion )
    else:
      if configName != default:
        condition += " and c.configname='%s'" % ( configName )
      if configVersion != default:
        condition += " and c.configversion='%s'" % ( configVersion )
      if condition.find( 'and c.' ) >= 0:
        tables += ',configurations c'
        condition += ' and j.configurationid=c.configurationid '

    if conddescription != default:
      retVal = self.__getConditionString( conddescription, 'pcont' )
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal

    if evt != default and visible.upper().startswith( 'Y' ):
      condition += ' and bview.eventtypeid=%d' % ( int( evt ) )
    elif evt != default:
      condition += '  and f.eventtypeid=%d ' % ( int( evt ) )

    defaultTable = 'bview'
    if not visible.upper().startswith( 'Y' ):
      defaultTable = 'j'

    if production != default and isinstance( production, ( basestring, long, int ) ):
      condition += ' and %s.production=%d' % ( defaultTable, int( production ) )
    elif production != default and isinstance( production, list ):
      cond = ' and ('
      for i in production:
        cond += ' %s.production=%d or ' % ( defaultTable, i )
      condition += cond[:-3] + ') '

    if runnb != default:
      if visible.upper().startswith( 'Y' ):
        tables = ' , prodrunview prview'
        condition += ' and prview.production=bview.production and prview.runnumber=' + str( runnb )
      else:
        condition += ' and j.runnumber=%d' % ( int( runnb ) )


    if processing != default:
      command = "select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='%s'" % ( processing.split( '/' )[1], processing )
      retVal = self.dbR_.query( command )
      if not retVal['OK']:
        return retVal
      pro = '('
      for i in retVal['Value']:
        pro += "%s," % ( str( i[0] ) )
      pro = pro[:-1]
      pro += ( ')' )
      if visible.upper().startswith( 'Y' ):
        command = "select distinct ftypes.name from \
                 productionscontainer pcont,prodview bview, filetypes ftypes  %s \
                 where pcont.processingid in %s \
                  and bview.production=pcont.production and\
                   bview.filetypeId=ftypes.filetypeid  %s" % ( tables, pro, condition )
      else:
        command = " select distinct ft.name from filetypes ft, files f, jobs j, productionscontainer pcont %s where \
                    j.jobid=f.jobid and f.filetypeid=ft.filetypeid and pcont.production=j.production and  \
                    pcont.processingid in %s %s " % ( tables, pro, condition )
    else:
      if visible.upper().startswith( 'Y' ):
        command = "select distinct ftypes.name  from \
      productionscontainer pcont, prodview bview,  filetypes ftypes %s where \
                 bview.production=pcont.production and bview.filetypeId=ftypes.filetypeid %s" % ( tables, condition )
      else:
        command = " select distinct ft.name from filetypes ft, files f, jobs j, productionscontainer pcont %s  where \
                    j.jobid=f.jobid and f.filetypeid=ft.filetypeid and \
                    pcont.production=j.production  %s " % ( tables, condition )
    return self.dbR_.query( command )


  #############################################################################
  def getFilesWithMetadata( self, configName, configVersion, conddescription = default,
                            processing = default, evt = default, production = default,
                            filetype = default, quality = default,
                            visible = default, replicaflag = default,
                            startDate = None, endDate = None, runnumbers = list(),
                            startRunID = None, endRunID = None ):
    """return a list of files with their metadata"""
    condition = ''

    tables = 'files f, jobs j, dataquality d '
    retVal = self.__buildStartenddate( startDate, endDate, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildRunnumbers( runnumbers, startRunID, endRunID, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildVisibilityflag( visible, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildConfiguration( configName, configVersion, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildConditions( default, conddescription, condition, tables, visible )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildProduction( production, condition, tables, visible )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildReplicaflag( replicaflag, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildDataquality( quality, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildProcessingPass( processing, condition, tables, visible )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildEventType(evt, condition, tables, visible)
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    
    tables += ',filetypes ft'
    retVal = self.__buildFileTypes(filetype, condition, tables, visible)
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    command = "select distinct f.FileName, f.EventStat, f.FileSize, f.CreationDate, j.JobStart, j.JobEnd, \
    j.WorkerNode, ft.Name, j.runnumber, j.fillnumber, f.fullstat, d.dataqualityflag, \
    j.eventinputstat, j.totalluminosity, f.luminosity, f.instLuminosity, j.tck, f.guid, f.adler32, f.eventTypeid, f.md5sum,f.visibilityflag, j.jobid, f.gotreplica, f.inserttimestamp from %s  where \
    j.jobid=f.jobid  and \
    ft.filetypeid=f.filetypeid and \
    f.qualityid=d.qualityid %s" % ( tables, condition )
    return self.dbR_.query( command )

  #############################################################################
  def getAvailableDataQuality( self ):
    """returns the available data quality flags"""
    result = S_ERROR()
    command = ' select dataqualityflag from dataquality'
    retVal = self.dbR_.query( command )
    if not retVal['OK']:
      result = retVal
    else:
      flags = retVal['Value']
      values = []
      for i in flags:
        values += [i[0]]
      result = S_OK( values )
    return result

  #############################################################################
  def getAvailableProductions( self ):
    """returns the available productions"""
    command = ' select distinct production from prodview where production > 0'
    res = self.dbR_.query( command )
    return res

  #############################################################################
  def getAvailableRuns( self ):
    """returns the available runs"""
    command = ' select distinct runnumber from prodrunview'
    res = self.dbR_.query( command )
    return res

  #############################################################################
  def getAvailableEventTypes( self ):
    """returns the availabel event types"""
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getAvailableEventTypes', [] )

  #############################################################################
  def getProductionProcessingPass( self, prodid ):
    """returns the processing pass of a given production"""
    return self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getProductionProcessingPass', types.StringType, [prodid] )

  #############################################################################
  def getRunProcessingPass( self, runnumber ):
    """returns the processing pass for a given run"""
    return self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getProductionProcessingPass',
                                             types.StringType, [-1 * runnumber] )

  #############################################################################
  def getProductionProcessingPassID( self, prodid ):
    """returns the processing pass identifier of a production"""
    return self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getProductionProcessingPassId', types.LongType, [prodid] )

  #############################################################################
  def getMoreProductionInformations( self, prodid ):
    """returns the statistics of a production"""
    command = 'select prodview.configname, prodview.configversion, prodview.ProgramName, prodview.programversion from \
    prodview  where prodview.production=' + str( prodid )
    res = self.dbR_.query( command )
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

    command = "select distinct sim.simdescription, daq.description from simulationconditions sim, \
    data_taking_conditions daq,productionscontainer prod \
    where sim.simid(+)=prod.simid and daq.daqperiodid(+)=prod.daqperiodid and prod.production=" + str( prodid )
    retVal = self.dbR_.query( command )
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
      return S_OK( {'ConfigName':cname,
                   'ConfigVersion':cversion,
                   'ProgramName':pname,
                   'ProgramVersion':pversion,
                   'Processing pass':procdescription,
                   'Simulation conditions':simdesc} )
    else:
      return S_OK( {'ConfigName':cname,
                   'ConfigVersion':cversion,
                   'ProgramName':pname,
                   'ProgramVersion':pversion,
                   'Processing pass':procdescription,
                   'Data taking conditions':daqdesc} )


  #############################################################################
  def getJobInfo( self, lfn ):
    """returns the job information which created a file"""
    return self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getJobInfo', [lfn] )

  #############################################################################
  def bulkJobInfo( self, in_dict ):
    """returns the job information for a list of files"""
    
    data = []
    if 'lfn' in in_dict:
      data = in_dict['lfn']
      retVal = self.dbR_.executeStoredProcedure( packageName = 'BOOKKEEPINGORACLEDB.bulkJobInfo',
                                                parameters = [],
                                                output = True,
                                                array = data )
    elif 'jobId' in in_dict:
      data = in_dict['jobId']
      retVal = self.dbR_.executeStoredProcedure( packageName = 'BOOKKEEPINGORACLEDB.bulkJobInfoForJobId',
                                                parameters = [],
                                                output = True,
                                                array = data )
      
    elif 'jobName' in in_dict:
      data = in_dict['jobName']
      retVal = self.dbR_.executeStoredProcedure( packageName = 'BOOKKEEPINGORACLEDB.bulkJobInfoForJobName',
                                                parameters = [],
                                                output = True,
                                                array = data )
    else:
      return S_ERROR( "Wrong input parameters. You can use a dictionary with the following keys: lfn,jobId, jobName" )
     
    records = {}
    if retVal['OK']:
      for i in retVal['Value']:
        record = dict( zip( ( 'DIRACJobId',
                              'DIRACVersion',
                              'EventInputStat',
                              'ExecTime',
                              'FirstEventNumber',
                              'Location',
                              'Name',
                              'NumberOfEvents',
                              'StatisticsRequested',
                              'WNCPUPOWER',
                              'CPUTIME',
                              'WNCACHE',
                              'WNMEMORY',
                              'WNMODEL',
                              'WORKERNODE',
                              'WNCPUHS06',
                              'JobId',
                              'TotalLumonosity',
                              'Production',
                              'ApplicationName',
                              'ApplicationVersion',
                              'WNMJFHS06' ), i[1:] ) )
        j = 0
        if i[0] not in records:
          records[i[0]] = [record]
        else:
          records[i[0]] += [record]
        
        j += 1

      failed = [ i for i in data if i not in records]
      result = S_OK( {'Successful':records, 'Failed':failed} )
    else:
      result = retVal

    return result

  #############################################################################
  def getJobInformation( self, params ):
    """it returns only  the job information which given  by params for a given lfn"""
    production = params.get( 'Production', default )
    lfn = params.get( 'LFN', default )
    condition = ''
    tables = ' jobs j, files f'
    result = None
    if production != default:
      if isinstance( production, ( basestring, long, int ) ) :
        condition += " and j.production=%d " % ( int( production ) )
      elif isinstance( production, list ):
        condition += ' and j.production in ( ' + ','.join( [str( p ) for p in production] ) + ')'
      else:
        result = S_ERROR( "The production type is invalid. It can be a list, integer or string!" )
    elif lfn != default:
      if isinstance( lfn, basestring ):
        condition += " and f.filename='%s' " % ( lfn )
      elif isinstance( lfn, list ):
        condition += ' and (' + ' or '.join( ["f.filename='%s'" % l for l in lfn] ) + ')'
      else:
        result = S_ERROR( "You must provide an LFN or a list of LFNs!" )

    if not result:
      command = " select  distinct j.DIRACJOBID, j.DIRACVERSION, j.EVENTINPUTSTAT, j.EXECTIME,\
      j.FIRSTEVENTNUMBER,j.LOCATION,  j.NAME, j.NUMBEROFEVENTS, \
                 j.STATISTICSREQUESTED, j.WNCPUPOWER, j.CPUTIME, j.WNCACHE, j.WNMEMORY, j.WNMODEL, \
                 j.WORKERNODE, j.WNCPUHS06, j.jobid, j.totalluminosity, j.production, j.WNMJFHS06\
                 from %s where f.jobid=j.jobid %s" % ( tables, condition )
      retVal = self.dbR_.query( command )
      if retVal['OK']:
        records = []
        parameters = ['DiracJobID', 'DiracVersion', 'EventInputStat', 'Exectime', 'FirstEventNumber',
                      'Location', 'JobName', 'NumberOfEvents', 'StatisticsRequested', 'WNCPUPower',
                      'CPUTime', 'WNCache', 'WNMemory', 'WNModel', 'WorkerNode', 'WNCPUHS06',
                      'JobId', 'TotalLuminosity', 'Production','WNMJFHS06']
        for i in retVal['Value']:
          records += [dict( zip( parameters, i ) )]
        result = S_OK( records )
      else:
        result = retVal

    return result

  #############################################################################
  def getRunNumber( self, lfn ):
    """returns the run number of a given file"""
    return self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getRunNumber', types.LongType, [lfn] )

  #############################################################################
  def getRunNbAndTck( self, lfn ):
    """returns the run number and tck for a given file"""
    return self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getRunNbAndTck', [lfn] )

  #############################################################################
  def getProductionFiles( self, prod, ftype, gotreplica = default ):
    """returns the files which are belongs to a given production"""
    result = S_ERROR()
    value = {}
    condition = ''
    if gotreplica != default:
      condition += " and files.gotreplica='%s'" % ( str( gotreplica ) )

    if ftype != default:
      condition += " and filetypes.name='%s'" % ( ftype )

    command = "select files.filename, files.gotreplica, files.filesize,files.guid, \
    filetypes.name, files.inserttimestamp, files.visibilityflag from jobs,files,filetypes where\
    jobs.jobid=files.jobid and files.filetypeid=filetypes.filetypeid and jobs.production=%d %s" % ( prod, condition )

    res = self.dbR_.query( command )
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        value[record[0]] = {'GotReplica':record[1],
                            'FileSize':record[2],
                            'GUID':record[3],
                            'FileType':record[4],
                            'Visible':record[6]}
      result = S_OK( value )
    else:
      result = S_ERROR( res['Message'] )
    return result

  #############################################################################
  def getRunFiles( self, runid ):
    """returns a list of files with metadata for a given run"""
    result = S_ERROR()
    value = {}
    res = self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getRunFiles', [runid] )
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        value[record[0]] = {'GotReplica':record[1],
                            'FileSize':record[2],
                            'GUID':record[3],
                            'Luminosity':record[4],
                            'InstLuminosity':record[5],
                            'EventStat':record[6],
                            'FullStat':record[7]
                            }
      result = S_OK( value )
    else:
      result = res
    return result

  #############################################################################
  def updateFileMetaData( self, filename, fileAttr ):
    """updates the file metadata"""
    utctime = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
    command = "update files Set inserttimestamp=TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS') ," % ( str( utctime ) )
    command += ','.join( ["%s=%s" % ( str( attribute ), str( fileAttr[attribute] ) ) for attribute in fileAttr] )
    command += " where fileName='%s'" % ( filename )
    res = self.dbW_.query( command )
    return res

  #############################################################################
  def bulkupdateFileMetaData( self, lfnswithmeta ):
    """it updates the metadata a list of files"""
    
    utctime = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
    sqls = []
    for filename in lfnswithmeta:
      command = "update files Set inserttimestamp=TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS') ," % ( str( utctime ) )
      command += ','.join( ["%s=%s" % ( str( attribute ), str( lfnswithmeta[filename][attribute] ) ) for attribute in lfnswithmeta[filename]] )
      command += " where fileName='%s'" % ( filename )
      sqls += [command]
      
    retVal = self.dbR_.executeStoredProcedure( packageName = 'BOOKKEEPINGORACLEDB.bulkupdateFileMetaData',
                                                parameters = [],
                                                output = False,
                                                array = sqls )
    return retVal
  
  #############################################################################
  def renameFile( self, oldLFN, newLFN ):
    """renames a file"""
    utctime = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
    command = " update files set inserttimestamp=TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS'),\
     filename ='%s' where filename='%s'" % ( str( utctime ), newLFN, oldLFN )
    res = self.dbW_.query( command )
    return res

  #############################################################################
  def getInputFiles( self, jobid ):
    """returns the input files for a given jobid"""
    command = ' select files.filename from inputfiles,files where \
    files.fileid=inputfiles.fileid and inputfiles.jobid=' + str( jobid )
    res = self.dbR_.query( command )
    return res

  #############################################################################
  def getOutputFiles( self, jobid ):
    """returns the outputfiles for a given jobid"""
    command = ' select files.filename from files where files.jobid =' + str( jobid )
    res = self.dbR_.query( command )
    return res

  #############################################################################
  def insertTag( self, name, tag ):
    """inserts the CONDD,DDDB tags to the database"""
    return self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.insertTag', [name, tag], False )

  #############################################################################
  def existsTag( self, name, value ):  #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    """checks the tag existsance in the database"""
    result = False
    command = "select count(*) from tags where name='%s' and tag='%s'" % ( str( name ), str( value ) )
    retVal = self.dbR_.query( command )
    if not retVal['OK']:
      result = retVal
    elif retVal['Value'][0][0] > 0:
      result = True
    return S_OK( result )

  #############################################################################
  def setFileDataQuality( self, lfns, flag ):
    """sets the dataquality for a list of lfns"""
    result = S_ERROR()
    values = {}
    retVal = self.__getDataQualityId( flag )
    if not retVal['OK']:
      result = retVal
    else:
      qid = retVal['Value']
      failed = []
      succ = []
      retVal = self.dbR_.executeStoredProcedure( packageName = 'BOOKKEEPINGORACLEDB.updateDataQualityFlag',
                                                parameters = [qid],
                                                output = False,
                                                array = lfns )
      if not retVal['OK']:
        failed = lfns
        gLogger.error( retVal['Message'] )
      else:
        succ = lfns
      values['Successful'] = succ
      values['Failed'] = failed
      result = S_OK( values )
    return result

  #############################################################################
  def  __getProcessingPassId( self, root, fullpath ):
    """returns the processing pass identifier for a given root and fullpath.
    for example root is 'Real Data' fullpath is '/Real Data/Reco11'"""
    return  self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getProcessingPassId', types.LongType, [root, fullpath] )

  #############################################################################
  def getProcessingPassId( self, fullpath ):
    """returns the processing pass identifier for a given path"""
    return self.__getProcessingPassId( fullpath.split( '/' )[1:][0], fullpath )

  #############################################################################
  def __getDataQualityId( self, name ):
    """returns the quality identifire for a given data quality"""
    return  self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getDataQualityId', types.LongType, [name] )

  #############################################################################
  def setRunAndProcessingPassDataQuality( self, runNB, procpass, flag ):
    """set the data quality of a run which belongs to a given processing pass"""
    result = S_ERROR()
    retVal = self.__getProcessingPassId( procpass.split( '/' )[1:][0], procpass )
    if retVal['OK']:
      processingid = retVal['Value']
      retVal = self.__getDataQualityId( flag )
      if retVal['OK']:
        flag = retVal['Value']
        result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.insertRunquality',
                                                  [runNB, flag, processingid], False )
      else:
        result = retVal
    else:
      result = retVal
    return result

  #############################################################################
  def setRunDataQuality( self, runNb, flag ):
    """sets the data quality flag for a given run"""
    result = S_ERROR()
    command = 'select distinct j.runnumber from  jobs j, productionscontainer prod where \
    j.production=prod.production and \
    j.production<0 and \
    j.runnumber=%s' % ( str( runNb ) )
    retVal = self.dbR_.query( command )
    if not retVal['OK']:
      result = retVal
    else:

      if len( retVal['Value'] ) == 0:
        result = S_ERROR( 'This ' + str( runNb ) + ' run is missing in the BKK DB!' )
      else:
        retVal = self.__getDataQualityId( flag )

        if not retVal['OK']:
          result = retVal
        else:
          qid = retVal['Value']
          utctime = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
          command = " update files set inserttimestamp=TO_TIMESTAMP('%s','YYYY-MM-DD HH24:MI:SS'), \
          qualityId=%d where fileid in ( select files.fileid from jobs, files where jobs.jobid=files.jobid and \
            jobs.runnumber=%d)" % ( str( utctime ), qid, runNb )
          retVal = self.dbW_.query( command )

          if not retVal['OK']:
            result = retVal
          else:
            command = 'select files.filename from jobs, files where jobs.jobid=files.jobid and \
              jobs.runnumber=%s' % ( runNb )

            retVal = self.dbR_.query( command )

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
              result = S_OK( values )
    return result

  #############################################################################
  def setProductionDataQuality( self, prod, flag ):
    """sets the data quality to a production"""
    result = S_ERROR()
    command = "select distinct jobs.production  from jobs where jobs.production=%d" % ( prod )
    retVal = self.dbR_.query( command )

    if not retVal['OK']:
      result = retVal
    else:

      if len( retVal['Value'] ) == 0:
        result = S_ERROR( 'This ' + str( prod ) + ' production is missing in the BKK DB!' )
      else:
        retVal = self.__getDataQualityId( flag )

        if not retVal['OK']:
          result = retVal
        else:
          qid = retVal['Value']
          utctime = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
          command = " update files set inserttimestamp=TO_TIMESTAMP('%s' + str(utctime),'YYYY-MM-DD HH24:MI:SS'), \
          qualityId=%d where fileid in ( select files.fileid from jobs, files where jobs.jobid=files.jobid and \
            jobs.production=%d)" % ( str( utctime ), qid, prod )
          retVal = self.dbW_.query( command )

          if not retVal['OK']:
            result = retVal
          else:
            command = "select files.filename from jobs, files where jobs.jobid=files.jobid and \
              jobs.production=%d" % ( prod )
            retVal = self.dbR_.query( command )

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
              result = S_OK( values )
    return result

  #############################################################################
  def getFileAncestorHelper( self, fileName, files, depth, checkreplica ):
    """returns the ancestor of a file"""
    failed = []

    if depth:
      depth -= 1
      result = self.dbR_.executeStoredFunctions( 'BKK_MONITORING.getJobIdWithoutReplicaCheck', types.LongType, [fileName] )

      if not result["OK"]:
        gLogger.error( 'Error getting jobID', result['Message'] )
      jobID = int( result.get( 'Value', 0 ) )
      if jobID:
        command = "select files.fileName,files.jobid, files.gotreplica, files.eventstat,\
         files.eventtypeid, files.luminosity, files.instLuminosity, filetypes.name \
        from inputfiles,files, filetypes where files.filetypeid=filetypes.filetypeid \
         and inputfiles.fileid=files.fileid and inputfiles.jobid=%d" % ( jobID )
        res = self.dbR_.query( command )
        if not res['OK']:
          gLogger.error( 'Error getting job input files', result["Message"] )
        else:
          dbResult = res['Value']
          for record in dbResult:
            if ( not checkreplica or ( record[2] != 'No' ) ):
              files.append( {'FileName':record[0],
                            'GotReplica':record[2],
                            'EventStat':record[3],
                            'EventType':record[4],
                            'Luminosity':record[5],
                            'InstLuminosity':record[6],
                            'FileType':record[7]} )
            if depth:
              failed += self.getFileAncestorHelper( record[0], files, depth, checkreplica )
      else:
        failed.append( fileName )
    return failed

  #############################################################################
  def getFileAncestors( self, lfn, depth = 0, replica = True ):
    """"iterates on the list of lfns and prepare the ancestor list using a recusive helper function"""
    depth = min( 10, max( 1, depth ) )

    logicalFileNames = {'Failed':[]}
    ancestorList = {}
    filesWithMetadata = {}
    gLogger.debug( 'original', lfn )
    failed = []
    for fileName in lfn:
      files = []
      failed += self.getFileAncestorHelper( fileName, files, depth, replica )
      logicalFileNames['Failed'] = failed
      if len( files ) > 0:
        ancestorList[fileName] = files
        tmpfiles = {}
        for i in  files:
          tmpattr = dict( i )
          tmpfiles[tmpattr.pop( 'FileName' )] = tmpattr
        filesWithMetadata[fileName] = tmpfiles
    logicalFileNames['Successful'] = ancestorList
    logicalFileNames['WithMetadata'] = filesWithMetadata
    return S_OK( logicalFileNames )

  #############################################################################
  def getFileDescendentsHelper( self, fileName, files, depth, production, checkreplica, productionFound = False ):
    """returns the descendents of a file"""
    failed = set()
    notprocessed = set()

    if depth:
      depth -= 1
      
      res = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getFileDesJobId', [fileName] )
      if not res["OK"]:
        gLogger.error( 'Error getting fileId', res['Message'] )
        failed.add( fileName )
      elif not res['Value']:
        notprocessed.add( fileName )
      else:
        for jobID in [item[0] for item in res['Value']]:
          getProd = bool( production )

          res = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getFileAndJobMetadata', [jobID, getProd] )
          if not res["OK"]:
            gLogger.error( 'Error getting job output files', res['Message'] )
            failed.add( fileName )
          elif not res['Value']:
            notprocessed.add( fileName )
          else:
            for record in res['Value']:
              inRequestedProd = getProd and int( record[3] ) == int( production )
              if not productionFound or inRequestedProd:
                # If we have already found the production but we no longer are in it, break the recursive loop
                if ( not checkreplica or ( record[2] != 'No' ) ) and ( not getProd or inRequestedProd ):
                  files[record[0]] = {'GotReplica':record[2],
                                      'EventStat':record[4],
                                      'EventType':record[5],
                                      'Luminosity':record[6],
                                      'InstLuminosity':record[7],
                                      'FileType':record[8]}

                if depth:
                  # Only call if we are not at the correct depth
                  newFailed, _newNotprocessed = self.getFileDescendentsHelper( record[0], files, depth, production, checkreplica, productionFound = inRequestedProd )
                  failed.update( newFailed )

    return sorted( failed ), sorted( notprocessed )

  #############################################################################
  def getFileDescendents( self, lfn, depth = 0, production = 0, checkreplica = True ):
    """iterates over a list of lfns and collects their descendents"""
    logicalFileNames = {'Failed':[], 'NotProcessed':[]}
    ancestorList = {}
    filesWithMetadata = {}

    depth = min( 10, max( 1, depth ) )

    for fileName in lfn:
      files = {}
      failed, notprocessed = self.getFileDescendentsHelper( fileName, files, depth, production, checkreplica )
      logicalFileNames['Failed'] += failed
      logicalFileNames['NotProcessed'] += notprocessed
      if files:
        ancestorList[fileName] = files.keys()
        filesWithMetadata[fileName] = files
    logicalFileNames['Successful'] = ancestorList
    logicalFileNames['WithMetadata'] = filesWithMetadata
    return S_OK( logicalFileNames )


  #############################################################################
  def checkfile( self, fileName ):  # file
    """checks the status of a file"""
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
  def checkFileTypeAndVersion( self, filetype, version ):  # fileTypeAndFileTypeVersion(self, type, version):
    """checks the the format and the version"""
    result = self.dbR_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.checkFileTypeAndVersion',
                                               types.LongType, [filetype, version] )
    return result

  #############################################################################
  def checkEventType( self, eventTypeId ):  # eventType(self, eventTypeId):
    """checks the event type"""
    result = S_ERROR()

    retVal = self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.checkEventType', [eventTypeId] )
    if retVal['OK']:
      value = retVal['Value']
      if len( value ) != 0:
        result = S_OK( value )
      else:
        gLogger.error( "Event type not found! ", str( eventTypeId ) )
        result = S_ERROR( "Event type not found!" + str( eventTypeId ) )
    else:
      result = retVal
    return result

  #############################################################################
  def insertJob( self, job ):
    """inserts a job to the database"""
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
                 'Tck':'None',\
                 'StepID': None,\
                 'WNMJFHS06' : 0}

    for param in job:
      if not attrList.__contains__( param ):
        gLogger.error( "insert job error: ", " the job table not contains " + param + " this attributte!!" )
        return S_ERROR( " The job table not contains " + param + " this attributte!!" )

      if param == 'JobStart' or param == 'JobEnd':  # We have to convert data format
        dateAndTime = job[param].split( ' ' )
        date = dateAndTime[0].split( '-' )
        time = dateAndTime[1].split( ':' )
        if len( time ) > 2:
          timestamp = datetime.datetime( int( date[0] ), int( date[1] ),
                                        int( date[2] ), int( time[0] ),
                                        int( time[1] ), int( time[2] ), 0 )
        else:
          timestamp = datetime.datetime( int( date[0] ), int( date[1] ),
                                        int( date[2] ), int( time[0] ),
                                        int( time[1] ), 0, 0 )
        attrList[param] = timestamp
      else:
        attrList[param] = job[param]

    try:
      conv = int( attrList['Tck'] )
      attrList['Tck'] = str( hex( conv ) )
    except ValueError:
      pass  # it is already defined


    result = self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.insertJobsRow',
                                              types.LongType, [ attrList['ConfigName'],
                                                         attrList['ConfigVersion'],
                                                         attrList['DiracJobId'],
                                                         attrList['DiracVersion'],
                                                         attrList['EventInputStat'],
                                                         attrList['ExecTime'],
                                                         attrList['FirstEventNumber'],
                                                         attrList['JobEnd'],
                                                         attrList['JobStart'],
                                                         attrList['Location'],
                                                         attrList['Name'],
                                                         attrList['NumberOfEvents'],
                                                         attrList['Production'],
                                                         attrList['ProgramName'],
                                                         attrList['ProgramVersion'],
                                                         attrList['StatisticsRequested'],
                                                         attrList['WNCPUPOWER'],
                                                         attrList['CPUTIME'],
                                                         attrList['WNCACHE'],
                                                         attrList['WNMEMORY'],
                                                         attrList['WNMODEL'],
                                                         attrList['WorkerNode'],
                                                         attrList['RunNumber'],
                                                         attrList['FillNumber'],
                                                         attrList['WNCPUHS06'],
                                                         attrList['TotalLuminosity'],
                                                         attrList['Tck'],
                                                         attrList['StepID'],
                                                         attrList['WNMJFHS06'] ] )
    return result

  #############################################################################
  def insertInputFile( self, jobID, fileId ):
    """inserts the input file of a job"""
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.insertInputFilesRow', [fileId, jobID], False )
    return result

  #############################################################################
  def insertOutputFile( self, fileobject ):
    """inserst an output file"""
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

    for param in fileobject:
      if param not in  attrList:
        gLogger.error( "insert file error: ", " the files table not contains " + param + " this attributte!!" )
        return S_ERROR( " The files table not contains " + param + " this attributte!!" )

      if param == 'CreationDate':  # We have to convert data format
        dateAndTime = fileobject[param].split( ' ' )
        date = dateAndTime[0].split( '-' )
        time = dateAndTime[1].split( ':' )
        timestamp = datetime.datetime( int( date[0] ), int( date[1] ), int( date[2] ), int( time[0] ), int( time[1] ), 0, 0 )
        attrList[param] = timestamp
      else:
        attrList[param] = fileobject[param]
    utctime = datetime.datetime.utcnow()

    result = self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.insertFilesRow', types.LongType, [  attrList['Adler32'], \
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
  def updateReplicaRow( self, fileID, replica ):  # , name, location):
    """adds the replica flag"""
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.updateReplicaRow', [fileID, replica], False )
    return result

  #############################################################################
  def deleteJob( self, jobID ):
    """deletes a job"""
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.deleteJob', [jobID], False )
    return result

  #############################################################################
  def deleteInputFiles( self, jobid ):
    """deletes the input files of a job"""
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.deleteInputFiles', [jobid], False )
    return result

  #############################################################################
  def deleteFile( self, fileid ):
    """deletes a file"""
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.deletefile', [fileid], False )
    return result

  #############################################################################
  @staticmethod
  def deleteFiles( lfns ):
    """deletes a file"""
    return S_ERROR( 'Not Implemented !!' + lfns )

  #############################################################################
  def insertSimConditions( self, in_dict ):
    """inserst a simulation conditions"""

    simdesc = in_dict.get( 'SimDescription', None )
    beamCond = in_dict.get( 'BeamCond', None )
    beamEnergy = in_dict.get( 'BeamEnergy', None )
    generator = in_dict.get( 'Generator', None )
    magneticField = in_dict.get( 'MagneticField', None )
    detectorCond = in_dict.get( 'DetectorCond', None )
    luminosity = in_dict.get( 'Luminosity', None )
    g4settings = in_dict.get( 'G4settings', None )
    visible = in_dict.get( 'Visible', 'Y' )
    return self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.insertSimConditions',
                                            types.LongType, [simdesc, beamCond, beamEnergy,
                                                       generator, magneticField,
                                                       detectorCond, luminosity, g4settings, visible] )

  #############################################################################
  def getSimConditions( self ):
    """rerturns the available simulation conditions"""
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getSimConditions', [] )

  #############################################################################
  def insertDataTakingCond( self, conditions ):
    """inserts a data taking condition"""
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

    res = self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.insertDataTakingCond',
                                            types.LongType, [datataking['Description'],
                                                       datataking['BeamCond'],
                                                       datataking['BeamEnergy'],
                                                       datataking['MagneticField'],
                                                       datataking['VELO'],
                                                       datataking['IT'],
                                                       datataking['TT'],
                                                       datataking['OT'],
                                                       datataking['RICH1'],
                                                       datataking['RICH2'],
                                                       datataking['SPD_PRS'],
                                                       datataking['ECAL'],
                                                       datataking['HCAL'],
                                                       datataking['MUON'],
                                                       datataking['L0'],
                                                       datataking['HLT'],
                                                       datataking['VeloPosition'] ] )
    return res


  #############################################################################
  def removeReplica( self, fileNames ):
    """removes the replica flag of a file"""
    result = S_ERROR()
    retVal = self.dbR_.executeStoredProcedure( packageName = 'BOOKKEEPINGORACLEDB.bulkcheckfiles',
                                                parameters = [],
                                                output = True,
                                                array = fileNames )
    failed = {}

    if not retVal['OK']:
      result = retVal
    else:
      for i in retVal['Value']:
        failed[i[0]] = 'The file %s does not exist in the BKK database!!!' % ( i[0] )
        fileNames.remove( i[0] )
      if len( fileNames ) > 0:
        retVal = self.dbW_.executeStoredProcedure( packageName = 'BOOKKEEPINGORACLEDB.bulkupdateReplicaRow',
                                                  parameters = ['No'],
                                                  output = False,
                                                  array = fileNames )
        if not retVal['OK']:
          result = retVal
        else:
          failed['Failed'] = failed.keys()
          failed['Successful'] = fileNames
          result = S_OK( failed )
      else:  # when no files are exists
        files = {'Failed':[ i[0] for i in retVal['Value']], 'Successful':[]}
        result = S_OK( files )
    return result

  #############################################################################
  def getFileMetadata( self, lfns ):
    """returns the metadata of a list of files"""
    result = {}

#    for lfnList in breakListIntoChunks( lfns, 999 ):
#      inputlfns = ''
#      if len(lfnList) == 1:
#        inputlfns = "lists(%s)" % str(tuple(lfnList))[1:-2]
#      else:
#        inputlfns = "lists(%s)" % str(tuple(lfnList))[1:-1]
#
#      command = " select * from table(BOOKKEEPINGORACLEDB.getFileMetaData2(%s))" % (inputlfns)
#
#      retVal = self.dbR_.query(command)
#      if not retVal['OK']:
#        result = retVal
#      else:
#        for record in retVal['Value']:
#          row = {'ADLER32':record[1],
#                 'CreationDate':record[2],
#                 'EventStat':record[3],
#                 'FullStat':record[10],
#                 'EventType':record[4],
#                 'FileType':record[5],
#                 'GotReplica':record[6],
#                 'GUID':record[7],
#                 'MD5SUM':record[8],
#                 'FileSize':record[9],
#                 'DQFlag':record[11],
#                 'JobId':record[12],
#                 'RunNumber':record[13],
#                 'InsertTimeStamp':record[14],
#                 'Luminosity':record[15],
#                 'InstLuminosity':record[16]}
#          result[record[0]] = row

#    #2 type
#    for lfnList in breakListIntoChunks( lfns, 999 ):
#      inputlfns = ''
#      if len(lfnList) == 1:
#        inputlfns = "(%s)" % str(tuple(lfnList))[1:-2]
#      else:
#        inputlfns = "(%s)" % str(tuple(lfnList))[1:-1]
#
#      command = " select files.FILENAME,files.ADLER32,files.CREATIONDATE,files.EVENTSTAT, \
#      files.EVENTTYPEID,filetypes.Name,files.GOTREPLICA,files.GUID,files.MD5SUM,files.FILESIZE, \
#      files.FullStat, dataquality.DATAQUALITYFLAG, files.jobid,\
#      jobs.runnumber, files.inserttimestamp,files.luminosity,files.instluminosity \
#      from files,filetypes,dataquality,jobs where\
#         filename in %s and \
#         jobs.jobid=files.jobid and\
#         files.filetypeid=filetypes.filetypeid and\
#         files.QUALITYID=DataQuality.qualityID " % (inputlfns)
#
#      retVal = self.dbR_.query(command)
#      if not retVal['OK']:
#        result = retVal
#      else:
#        for record in retVal['Value']:
#          row = {'ADLER32':record[1],
#                 'CreationDate':record[2],
#                 'EventStat':record[3],
#                 'FullStat':record[10],
#                 'EventType':record[4],
#                 'FileType':record[5],
#                 'GotReplica':record[6],
#                 'GUID':record[7],
#                 'MD5SUM':record[8],
#                 'FileSize':record[9],
#                 'DQFlag':record[11],
#                 'JobId':record[12],
#                 'RunNumber':record[13],
#                 'InsertTimeStamp':record[14],
#                 'Luminosity':record[15],
#                 'InstLuminosity':record[16]}
#          result[record[0]] = row

    # 3 type
    for lfnList in breakListIntoChunks( lfns, 5000 ):
      retVal = self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getFileMetaData3', [], True, lfnList )
      if not retVal['OK']:
        result = retVal
      else:
        for record in retVal['Value']:
          row = {'ADLER32':record[1],
                 'CreationDate':record[2],
                 'EventStat':record[3],
                 'FullStat':record[10],
                 'EventType':record[4],
                 'FileType':record[5],
                 'GotReplica':record[6],
                 'GUID':record[7],
                 'MD5SUM':record[8],
                 'FileSize':record[9],
                 'DataqualityFlag':record[11],
                 'JobId':record[12],
                 'RunNumber':record[13],
                 'InsertTimeStamp':record[14],
                 'Luminosity':record[15],
                 'InstLuminosity':record[16],
                 'VisibilityFlag': record[17]}
          result[record[0]] = row
#    4 type
#    for lfn in lfns:
#      res = self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getFileMetaData', [lfn])
#      if not res['OK']:
#        result[lfn] = res['Message']
#      else:
#        records = res['Value']
#        for record in records:
#          row = {'ADLER32':record[1],
#                 'CreationDate':record[2],
#                 'EventStat':record[3],
#                 'FullStat':record[10],
#                 'EventType':record[4],
#                 'FileType':record[5],
#                 'GotReplica':record[6],
#                 'GUID':record[7],
#                 'MD5SUM':record[8],
#                 'FileSize':record[9],
#                 'DQFlag':record[11],
#                 'JobId':record[12],
#                 'RunNumber':record[13],
#                 'InsertTimeStamp':record[14],
#                 'Luminosity':record[15],
#                 'InstLuminosity':record[16]}
#          result[lfn] = row

    retVal = result
    retVal['Successful'] = dict( result )
    retVal['Failed'] = [ i for i in lfns if i not in result]
    return S_OK( retVal )

  #############################################################################
  def getFileMetaDataForWeb( self, lfns ):
    """returns the metadata information used by Web"""
    totalrecords = len( lfns )
    parametersNames = ['Name', 'FileSize', 'FileType', 'CreationDate', 'EventType', 'EventStat', 'GotReplica']
    records = []
    for lfn in lfns:
      res = self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getFileMetaData', [lfn] )
      if not res['OK']:
        records = [str( res['Message'] )]
      else:
        values = res['Value']
        for record in values:
          row = [lfn, record[9], record[5], record[2], record[4], record[3], record[6]]
          records += [row]
    return S_OK( {'TotalRecords':totalrecords, 'ParameterNames':parametersNames, 'Records':records} )

  #############################################################################
  def __getProductionStatisticsForUsers( self, prod ):
    """returns the statistics of a production"""
    command = "select count(*), SUM(files.EventStat), SUM(files.FILESIZE), sum(files.Luminosity), \
    sum(files.instLuminosity) from files ,jobs where jobs.jobid=files.jobid and jobs.production=%d" % ( prod )
    res = self.dbR_.query( command )
    return res

  #############################################################################
  def getProductionFilesForWeb( self, prod, ftypeDict, sortDict, startItem, maxitems ):
    """returns the production files to the WEB interface"""
    command = ''
    parametersNames = ['Name', 'FileSize', 'FileType', 'CreationDate', 'EventType', 'EventStat',
                       'GotReplica', 'InsertTimeStamp', 'Luminosity', 'InstLuminosity']
    records = []
    result = S_ERROR()

    totalrecords = 0
    nbOfEvents = 0
    filesSize = 0
    ftype = ftypeDict['type']
    if len( sortDict ) > 0:
      res = self.__getProductionStatisticsForUsers( prod )
      if not res['OK']:
        gLogger.error( res['Message'] )
      else:
        totalrecords = res['Value'][0][0]
        nbOfEvents = res['Value'][0][1]
        filesSize = res['Value'][0][2]


    if ftype != 'ALL':

      command = "select rnum, filename, filesize, name , creationdate, eventtypeId, \
      eventstat,gotreplica, inserttimestamp , luminosity ,instLuminosity from \
                ( select rownum rnum, filename, filesize, name , creationdate, \
                eventtypeId, eventstat, gotreplica, inserttimestamp, luminosity,instLuminosity \
                from ( select files.filename, files.filesize, filetypes.name , files.creationdate, \
                files.eventtypeId, files.eventstat,files.gotreplica, \
                files.inserttimestamp, files.luminosity, files.instLuminosity \
                           from jobs,files, filetypes where \
                           jobs.jobid=files.jobid and \
                           jobs.production=%s and filetypes.filetypeid=files.filetypeid and filetypes.name='%s' \
                           Order by files.filename) where rownum <= %d )\
                            where rnum > %d" % ( prod, ftype, maxitems, startItem )
    else:

      command = "select rnum, fname, fsize, name, fcreation, feventtypeid,\
       feventstat, fgotreplica, finst, flumi, finstlumy from \
      (select rownum rnum, fname, fsize, ftypeid, fcreation, feventtypeid, \
      feventstat, fgotreplica, finst, flumi, finstlumy\
      from ( select files.filename fname, files.filesize fsize, filetypeid \
      ftypeid, files.creationdate fcreation, files.eventtypeId feventtypeid, \
          files.eventstat feventstat, files.gotreplica fgotreplica, \
          files.inserttimestamp finst, files.luminosity flumi, files.instLuminosity finstlumy\
            from jobs,files where\
            jobs.jobid=files.jobid and\
            jobs.production=%d\
            Order by files.filename) where rownum <=%d)f , filetypes ft where rnum > %d \
            and ft.filetypeid=f.ftypeid" % ( prod, maxitems, startItem )

    res = self.dbR_.query( command )
    if res['OK']:
      dbResult = res['Value']
      for record in dbResult:
        row = [record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8]]
        records += [row]
      result = S_OK( {'TotalRecords':totalrecords,
                     'ParameterNames':parametersNames,
                     'Records':records,
                     'Extras': {'GlobalStatistics':
                                {'Number of Events':nbOfEvents,
                                 'Files Size':filesSize }}} )
    else:
      result = res
    return result

  #############################################################################
  def exists( self, lfns ):
    """checks the files in the databse"""
    result = {}
    for lfn in lfns:
      res = self.dbR_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.fileExists', types.LongType, [lfn] )
      if not res['OK']:
        return S_ERROR( res['Message'] )
      if res['Value'] == 0:
        result[lfn] = False
      else:
        result[lfn] = True
    return S_OK( result )

  #############################################################################
  def addReplica( self, fileNames ):
    """adds the replica flag to a file"""
    result = S_ERROR()
    retVal = self.dbR_.executeStoredProcedure( packageName = 'BOOKKEEPINGORACLEDB.bulkcheckfiles',
                                                parameters = [],
                                                output = True,
                                                array = fileNames )
    failed = {}
    if not retVal['OK']:
      result = retVal
    else:
      for i in retVal['Value']:
        failed[i[0]] = 'The file %s does not exist in the BKK database!!!' % ( i[0] )
        fileNames.remove( i[0] )
      if len( fileNames ) > 0:
        retVal = self.dbW_.executeStoredProcedure( packageName = 'BOOKKEEPINGORACLEDB.bulkupdateReplicaRow',
                                                  parameters = ['Yes'],
                                                  output = False,
                                                  array = fileNames )
        if not retVal['OK']:
          result = retVal
        else:
          failed['Failed'] = failed.keys()
          failed['Successful'] = fileNames
          result = S_OK( failed )
      else:  # when no files are exists
        files = {'Failed':[ i[0] for i in retVal['Value']], 'Successful':[]}
        result = S_OK( files )

    return result


  #############################################################################
  def getRunInformations( self, runnb ):
    """returns the run statistics"""
    result = S_ERROR()
    command = "select distinct j.fillnumber, conf.configname, conf.configversion, \
    daq.description, j.jobstart, j.jobend, j.tck, j.TOTALLUMINOSITY \
        from jobs j, configurations conf,data_taking_conditions \
        daq, productionscontainer prod where \
        j.configurationid=conf.configurationid and \
        j.production<0 and prod.daqperiodid=daq.daqperiodid and\
         j.production=prod.production and j.runnumber=%d" % ( runnb )
    retVal = self.dbR_.query( command )

    if not retVal['OK']:
      result = retVal
    else:
      value = retVal['Value']
      if len( value ) == 0:
        result = S_ERROR( 'This run is missing in the BKK DB!' )
      else:
        values = {'Configuration Name':value[0][1], 'Configuration Version':value[0][2], 'FillNumber':value[0][0]}
        values['DataTakingDescription'] = value[0][3]
        values['RunStart'] = value[0][4]
        values['RunEnd'] = value[0][5]
        values['Tck'] = value[0][6]
        values['TotalLuminosity'] = value[0][7]

        retVal = self.getRunProcessingPass( runnb )
        if not retVal['OK']:
          result = retVal
        else:
          values['ProcessingPass'] = retVal['Value']
          command = ' select count(*), SUM(files.EventStat), SUM(files.FILESIZE), sum(files.fullstat), \
          files.eventtypeid , sum(files.luminosity), sum(files.instLuminosity)  from files,jobs \
               where files.JobId=jobs.JobId and  \
               files.gotReplica=\'Yes\' and \
               jobs.production<0 and \
               jobs.runnumber=' + str( runnb ) + ' Group by files.eventtypeid'
          retVal = self.dbR_.query( command )
          if not retVal['OK']:
            result = retVal
          else:
            value = retVal['Value']
            if len( value ) == 0:
              result = S_ERROR( 'Replica flag is not set!' )
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
              result = S_OK( values )

    return result

  #############################################################################
  def getRunInformation( self, inputParams ):
    """returns only the requested information for a given run"""
    result = S_ERROR()
    runnb = inputParams.get( 'RunNumber', default )
    if runnb == default:
      result = S_ERROR ( 'The RunNumber must be given!' )
    else:
      if isinstance( runnb, ( basestring, int, long ) ):
        runnb = [runnb]
      runs = ''
      for i in runnb:
        runs += '%d,' % ( int( i ) )
      runs = runs[:-1]
      fields = inputParams.get( 'Fields', ['CONFIGNAME', 'CONFIGVERSION',
                                          'JOBSTART', 'JOBEND',
                                          'TCK', 'FILLNUMBER',
                                          'PROCESSINGPASS', 'CONDITIONDESCRIPTION',
                                          'CONDDB', 'DDDB'] )
      statistics = inputParams.get( 'Statistics', [] )
      configurationsFields = ['CONFIGNAME', 'CONFIGVERSION']
      jobsFields = [ 'JOBSTART', 'JOBEND', 'TCK', 'FILLNUMBER', 'PROCESSINGPASS']
      conditionsFields = ['CONDITIONDESCRIPTION']
      stepsFields = ['CONDDB', 'DDDB']
      selection = ''
      tables = 'jobs j,'
      conditions = ' j.runnumber in (%s) and j.production <0 ' % ( runs )

      for i in fields:
        if i.upper() in configurationsFields:
          if tables.find( 'configurations' ) < 0:
            tables += ' configurations c,'
            conditions += " and j.configurationid=c.configurationid "
          selection += 'c.%s,' % ( i )
        elif i.upper() in jobsFields:
          if i.upper() == 'PROCESSINGPASS':
            selection += 'BOOKKEEPINGORACLEDB.getProductionProcessingPass(-1 * j.runnumber),'
          else:
            selection += 'j.%s,' % ( i )
        elif i.upper() in conditionsFields:
          if tables.find( 'productionscontainer' ) < 0:
            tables += ' productionscontainer prod, data_taking_conditions daq,'
            conditions += ' and j.production=prod.production and prod.daqperiodid=daq.daqperiodid '
          selection += 'daq.description,'
        elif i.upper() in stepsFields:
          if tables.find( 'stepscontainer' ) < 0:
            tables += ' stepscontainer st, steps s,'
            conditions += ' and j.production=st.production and st.stepid=s.stepid '
          selection += ' s.%s,' % ( i )

      selection = selection[:-1]
      tables = tables[:-1]

      command = "select j.runnumber, %s from %s where %s" % ( selection, tables, conditions )
      retVal = self.dbR_.query( command )
      if not retVal['OK']:
        result = retVal
      else:
        values = {}
        for i in retVal['Value']:
          rnb = i[0]
          i = i[1:]
          record = dict( zip( fields, i ) )
          values[rnb] = record

        if len( statistics ) > 0:
          filesFields = ['NBOFFILES', 'EVENTSTAT',
                         'FILESIZE', 'FULLSTAT',
                         'LUMINOSITY', 'INSTLUMINOSITY',
                         'EVENTTYPEID']
          tables = 'jobs j, files f'
          conditions = " j.jobid=f.jobid and j.runnumber in (%s) and \
          j.production <0 and f.gotreplica='Yes' \
          Group by j.runnumber,f.eventtypeid" % ( runs )
          selection = 'j.runnumber,'
          for i in statistics:
            if i.upper() == 'NBOFFILES':
              selection += "count(*),"
            elif i.upper() == 'EVENTTYPEID':
              selection += 'f.%s,' % ( i )
            elif i.upper() in filesFields:
              selection += 'sum(f.%s),' % ( i )
          selection = selection[:-1]
          command = "select %s  from %s where %s" % ( selection, tables, conditions )
          retVal = self.dbR_.query( command )
          if not retVal['OK']:
            result = retVal
          else:
            for i in retVal['Value']:
              rnb = i[0]
              if 'Statistics' not in values[rnb]:
                values[rnb]['Statistics'] = []
              i = i[1:]
              record = dict( zip( statistics, i ) )
              values[rnb]['Statistics'] += [record]
        result = S_OK( values )
    return result

  #############################################################################
  def getProductionFilesStatus( self, productionid = None, lfns = list() ):
    """returns the status of the files produced by a production"""
    result = {}
    missing = []
    replicas = []
    noreplicas = []
    if productionid != None:
      command = "select files.filename, files.gotreplica from files,jobs where \
                 files.jobid=jobs.jobid and \
                 jobs.production=%d" % ( productionid )
      retVal = self.dbR_.query( command )
      if not retVal['OK']:
        return S_ERROR( retVal['Message'] )
      files = retVal['Value']
      for lfn in files:
        if lfn[1] == 'Yes':
          replicas += [lfn[0]]
        else:
          noreplicas += [lfn[0]]
      result['replica'] = replicas
      result['noreplica'] = noreplicas
    elif len( lfns ) != 0:
      for lfn in lfns:
        command = " select files.filename, files.gotreplica from files where filename='%s'" % ( lfn )
        retVal = self.dbR_.query( command )
        if not retVal['OK']:
          return S_ERROR( retVal['Message'] )
        value = retVal['Value']
        if len( value ) == 0:
          missing += [lfn]
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
  def getFileCreationLog( self, lfn ):
    """returns the logs of a file"""
    result = S_ERROR( 'getLogfile error!' )
    command = "select files.jobid from files where files.filename='%s'" % ( lfn )
    retVal = self.dbR_.query( command )
    if not retVal['OK']:
      result = retVal
    elif len( retVal['Value'] ) == 0:
      result = S_ERROR( 'Job not in the DB' )
    else:
      jobid = retVal['Value'][0][0]
      command = "select filename from files where \
      (files.filetypeid=17 or files.filetypeid=9) and files.jobid=%d" % ( jobid )
      retVal = self.dbR_.query( command )
      if not retVal['OK']:
        result = retVal
      elif len( retVal['Value'] ) == 0:
        result = S_ERROR( 'Log file is not exist!' )
      else:
        result = S_OK( retVal['Value'][0][0] )
    return result

  #############################################################################
  def insertEventTypes( self, evid, desc, primary ):
    """inserts an event type"""
    return self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.insertEventTypes', [desc, evid, primary], False )

  #############################################################################
  def updateEventType( self, evid, desc, primary ):
    """updates and existing event type"""
    return self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.updateEventTypes', [desc, evid, primary], False )


  #############################################################################
  def getProductionSummary( self, cName, cVersion,
                           conddesc = default, processing = default,
                           production = default, ftype = default,
                           evttype = default ):
    """returns a statistics for a given dataset"""
    conditions = ''

    if cName != default:
      conditions += " and bview.configname='%s'" % ( cName )

    if cVersion != default:
      conditions += " and bview.configversion='%s'" % ( cVersion )

    if ftype != default:
      conditions += " and f.filetypeid=ftypes.filetypeid and ftypes.name='%s'" % ( ftype )
    else:
      conditions += ' and bview.filetypeid= f.filetypeid  \
                      and bview.filetypeid= ftypes.filetypeid '
    if evttype != default:
      conditions += " and bview.eventtypeid=%d" % ( int( evttype ) )
    else:
      conditions += ' and bview.eventtypeid=f.eventtypeid '

    if production != default:
      conditions += " and j.production=%d" % ( int( production ) )
      conditions += ' and j.production= bview.production '
    else:
      conditions += ' and j.production= bview.production '

    if processing != default:
      command = "select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='%s'" % ( processing.split( '/' )[1], processing )
      retVal = self.dbR_.query( command )
      if not retVal['OK']:
        return retVal
      else:
        pro = '('
        for i in retVal['Value']:
          pro += "%s," % ( str( i[0] ) )
        pro = pro[:-1]
        pro += ( ')' )
        if len( pro ) > 1:
          conditions += " and prod.processingid in %s" % ( pro )

    if conddesc != default:
      retVal = self.__getConditionString( conddesc, 'prod' )
      if not retVal['OK']:
        return retVal
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

    command += "group by bview.configname, bview.configversion, sim.simdescription, \
    daq.description, prod.processingid, bview.eventtypeid, bview.description, bview.production, ftypes.name"
    retVal = self.dbR_.query( command )
    if not retVal['OK']:
      return S_ERROR( retVal['Message'] )

    parameters = ['ConfigurationName', 'ConfigurationVersion',
                  'ConditionDescription', 'Processing pass ',
                  'EventType', 'EventType description',
                  'Production', 'FileType', 'Number of events']
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
    result = S_OK( {'TotalRecords':nbRecords, 'ParameterNames':parameters, 'Records':records, 'Extras': {}} )

    return result

  #############################################################################
  def getProductionSimulationCond( self, prod ):
    """returns the simulation or data taking description of a production"""
    simdesc = None
    daqdesc = None

    command = "select distinct sim.simdescription, daq.description from \
    simulationconditions sim, data_taking_conditions daq,productionscontainer prod \
              where sim.simid(+)=prod.simid and daq.daqperiodid(+)=prod.daqperiodid and prod.production=" + str( prod )
    retVal = self.dbR_.query( command )
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
    """retuns the history of a file"""
    command = 'select  files.fileid, files.filename,files.adler32,\
    files.creationdate,files.eventstat,files.eventtypeid,files.gotreplica, \
files.guid,files.jobid,files.md5sum, files.filesize,files.fullstat, dataquality.\
dataqualityflag, files.inserttimestamp, files.luminosity, files.instLuminosity from files, dataquality \
where files.fileid in ( select inputfiles.fileid from files,inputfiles where \
files.jobid= inputfiles.jobid and files.filename=\'' + str( lfn ) + '\')\
and files.qualityid= dataquality.qualityid'

    res = self.dbR_.query( command )
    return res

  #############################################################################
  def getProductionInformationsFromView( self, prodid ):
    """production statistics from the view"""
    command = 'select * from productioninformations where production=' + str( prodid )
    res = self.dbR_.query( command )
    return res

  #############################################################################
  #
  #          MONITORING
  #############################################################################
  def getProductionNbOfJobs( self, prodid ):
    """returns the number of jobs"""
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getJobsNb', [prodid] )

  #############################################################################
  def getProductionNbOfEvents( self, prodid ):
    """returns the number of events"""
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getNumberOfEvents', [prodid] )

  #############################################################################
  def getProductionSizeOfFiles( self, prodid ):
    """returns the size of files"""
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getSizeOfFiles', [prodid] )

  #############################################################################
  def getProductionNbOfFiles( self, prodid ):
    """returns the number of files"""
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getNbOfFiles', [prodid] )

  #############################################################################
  def getProductionInformation( self, prodid ):
    """returns the statistics of a production"""
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getProductionInformation', [prodid] )

  #############################################################################
  def getSteps( self, prodid ):
    """returns the step used by a production"""
    result = None
    retVal = self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getSteps', [prodid] )
    if not retVal['OK']:
      result = retVal
    else:  
      data = retVal['Value']
      found = False
      ddb = ''
      conddb = ''
      for i in data:
        if i[4] != "fromPreviousStep" or i[5] != "fromPreviousStep":
          gLogger.warn( "DB tags are not specified try to retrieve from the parent production" )
          ddb = i[4]
          conddb = i[5]
          found = True
          break
      if found:
        correctedValues = []
        for i in data:
          tmp = list(i)
          tmp[4] = ddb
          tmp[5] = conddb
          correctedValues += [tuple(tmp)]
        result = S_OK( correctedValues )
      else:
        retVal = self.__resolveFromPreviousStep( prodid )
        if retVal['OK']:
          correctedValues = []
          for i in data:
            tmp = list(i)
            tmp[4] = retVal['Value'][0]
            tmp[5] = retVal['Value'][1]
            correctedValues += [tuple(tmp)]
          result = S_OK( correctedValues )
        else:
          result = S_OK(data)
      return result
  
  #############################################################################
  def __resolveFromPreviousStep( self, production ):
    """It returns the database tags from the ancestor"""
    command = "select distinct production from jobs j, files f where j.jobid=f.jobid and j.production!=%d and f.fileid in (select i.fileid from inputfiles i where i.JOBID in (select jobid from jobs j where j.production=%d))" % ( production, production )
    retVal = self.dbR_.query( command )
    if not retVal['OK']:
      return retVal
    prod = retVal['Value'][0][0]
    retVal = self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getSteps', [prod] )
    if not retVal['OK']:
      return retVal 
    data = retVal['Value']
    found = False
    for i in data:
      if i[4] != "fromPreviousStep" or i[5] != "fromPreviousStep":
        ddb = i[4]
        conddb = i[5]
        found = True
        break
    if found:
      return S_OK( [ddb, conddb] )
    else:
      return self.__resolveFromPreviousStep( prod )
      
  #############################################################################
  def getNbOfJobsBySites( self, prodid ):
    """returns the number of sucessfully
    finished jobs at different Grid sites for a given production"""
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getJobsbySites', [prodid] )

  #############################################################################
  def getConfigsAndEvtType( self, prodid ):
    """returns the configurations and event type of a production"""
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getConfigsAndEvtType', [prodid] )

  #############################################################################
  def getAvailableTags( self ):
    """returns the tags"""
    result = S_ERROR()
    command = 'select name, tag from tags order by inserttimestamp desc'
    retVal = self.dbR_.query( command )
    if retVal['OK']:
      parameters = ['TagName', 'TagValue']
      dbResult = retVal['Value']
      records = []
      nbRecords = 0
      for record in dbResult:
        row = [record[0], record[1]]
        records += [row]
        nbRecords += 1
      result = S_OK( {'TotalRecords':nbRecords, 'ParameterNames':parameters, 'Records':records, 'Extras': {}} )
    else:
      result = retVal
    return result

  #############################################################################
  def getProductionProcessedEvents( self, prodid ):
    """returns the processed event by a production"""
    return self.dbR_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getProcessedEvents', types.LongType, [prodid] )

  #############################################################################
  def getRunsForAGivenPeriod( self, in_dict ):
    """returns the runs for a given conditions"""
    condition = ''
    startDate = in_dict.get( 'StartDate', default )
    endDate = in_dict.get( 'EndDate', default )
    allowOutside = in_dict.get( 'AllowOutsideRuns', default )

    if allowOutside != default:
      if startDate == default and endDate == default:
        return S_ERROR( 'The Start and End date must be given!' )
      else:
        condition += " and jobs.jobstart >= TO_TIMESTAMP ('%s','YYYY-MM-DD HH24:MI:SS')" % ( startDate )
        condition += " and jobs.jobstart <= TO_TIMESTAMP ('%s','YYYY-MM-DD HH24:MI:SS')" % ( endDate )
    else:
      if startDate != default:
        condition += " and jobs.jobstart >= TO_TIMESTAMP ('%s','YYYY-MM-DD HH24:MI:SS')" % ( startDate )
      if endDate != default:
        condition += " and jobs.jobend <= TO_TIMESTAMP ('%s','YYYY-MM-DD HH24:MI:SS')" % ( endDate )
      elif startDate != default and endDate == default:
        currentTimeStamp = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
        condition += ' and jobs.jobend <= TO_TIMESTAMP (\'' + str( currentTimeStamp ) + '\',\'YYYY-MM-DD HH24:MI:SS\')'
        condition += " and jobs.jobend <= TO_TIMESTAMP ('%s','YYYY-MM-DD HH24:MI:SS')" % ( str( currentTimeStamp ) )

    command = ' select jobs.runnumber from jobs where jobs.production < 0' + condition
    retVal = self.dbR_.query( command )
    runIds = []
    if retVal['OK']:
      records = retVal['Value']
      for record in records:
        if record[0] != None:
          runIds += [record[0]]
    else:
      return S_ERROR( retVal['Message'] )

    check = in_dict.get( 'CheckRunStatus', False )
    if check:
      processedRuns = []
      notProcessedRuns = []
      for i in runIds:
        command = "select files.filename from files,jobs where jobs.jobid=files.jobid\
         and files.gotreplica='Yes' and jobs.production<0 and jobs.runnumber=%d" % ( i )
        retVal = self.dbR_.query( command )
        if retVal['OK']:
          files = retVal['Value']
          ok = True
          for lfn in files:
            name = lfn[0]
            retVal = self.getFileDescendents( [name], 1, 0, True )
            successful = retVal['Value']['Successful']
            if len( successful ) == 0:
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
  def getProductionsFromView( self, in_dict ):
    """returns the productions using the bookkeeping view"""
    run = in_dict.get( 'RunNumber', in_dict.get( 'Runnumber', default ) )
    proc = in_dict.get( 'ProcessingPass', in_dict.get( 'ProcPass', default ) )
    result = S_ERROR()
    if 'Runnumber' in in_dict:
      gLogger.verbose( 'The Runnumber has changed to RunNumber!' )

    if run != default:
      if proc != default:
        retVal = self.__getProcessingPassId( proc.split( '/' )[1:][0], proc )
        if retVal['OK']:
          processingid = retVal['Value']
          command = "select distinct bview.production  from prodview bview,\
           prodrunview prview, productionscontainer prod where \
      bview.production=prview.production and prview.runnumber=%d and \
      bview.production>0 and bview.production=prod.production and prod.processingid=%d" % ( run, processingid )
          result = self.dbR_.query( command )
      else:
        result = S_ERROR( 'The processing pass is missing!' )
    else:
      result = S_ERROR( 'The run number is missing!' )

    return result

  #############################################################################
  def getRunFilesDataQuality( self, runs ):
    """retuns the files with data quality"""
    retVal = self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getRunQuality', [], True, runs )
    return retVal

  #############################################################################
  def getRunAndProcessingPassDataQuality( self, runnb, processing ):
    """returns the data qaulity for a given run and processing pass"""
    return self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getQFlagByRunAndProcId',
                                            types.StringType, [runnb, processing] )

  #############################################################################
  def getRunWithProcessingPassAndDataQuality( self, procpass, flag = default ):
    """returns the runs for a given flag and processing pass"""
    retVal = self.__getProcessingPassId( procpass.split( '/' )[1:][0], procpass )
    if retVal['OK']:
      processingid = retVal['Value']
      qualityid = None
      if flag != default:
        retVal = self.__getDataQualityId( flag )
        if retVal['OK']:
          qualityid = retVal['Value']
        else:
          return retVal
    retVal = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getRunByQflagAndProcId',
                                              [processingid, qualityid] )
    if not retVal['OK']:
      return retVal
    else:
      result = [ i[0] for i in retVal['Value']]
    return S_OK( result )

  #############################################################################
  def setFilesInvisible( self, lfns ):
    """sets a given list of lfn invisible"""

    for i in lfns:
      retVal = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.setFileInvisible', [i], False )
      if not retVal['OK']:
        return retVal
    return S_OK( 'The files are invisible!' )

  #############################################################################
  def setFilesVisible( self, lfns ):
    """sets a given list of lfn visible"""
    for i in lfns:
      res = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.setFileVisible', [i], False )
      if not res['OK']:
        return res
    return S_OK( 'The files are visible!' )

  #############################################################################
  def getFiles( self, simdesc, datataking, procPass, ftype, evt,
               configName = default, configVersion = default,
               production = default, flag = default,
               startDate = None, endDate = None,
               nbofEvents = False, startRunID = None,
               endRunID = None, runnumbers = list(),
               replicaFlag = default, visible = default, filesize = False, tcks = list() ):
    """returns a list of lfns"""
    condition = ''
    tables = ' files f,jobs j '

    retVal = self.__buildConfiguration( configName, configVersion, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildProduction( production, condition, tables, visible )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildTCKS( tcks, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildProcessingPass( procPass, condition, tables, visible )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildFileTypes( ftype, condition, tables, visible )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildRunnumbers( runnumbers, startRunID, endRunID, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildEventType( evt, condition, tables, visible )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildStartenddate( startDate, endDate, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildDataquality( flag, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildReplicaflag( replicaFlag, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildVisibilityflag( visible, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildConditions( simdesc, datataking, condition, tables, visible )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    if nbofEvents:
      command = " select sum(f.eventstat) \
      from %s where f.jobid= j.jobid %s " % ( tables, condition )
    elif filesize:
      command = " select sum(f.filesize) \
      from %s where f.jobid= j.jobid %s " % ( tables, condition )
    else:
      command = " select distinct f.filename \
      from %s where f.jobid= j.jobid %s " % ( tables, condition )

    res = self.dbR_.query( command )

    return res

  #############################################################################
  @staticmethod
  def __buildConfiguration( configName, configVersion, condition, tables ):
    """ it make the condition string for a given configName and configVersion"""
    if configName not in [default, None, '']  and configVersion not in [default, None, '']:
      if tables.upper().find( 'CONFIGURATIONS' ) < 0:
        tables += ' ,configurations c'
      condition += "  and c.ConfigName='%s' and c.ConfigVersion='%s' and \
      j.configurationid=c.configurationid " % ( configName, configVersion )
    return S_OK( ( condition, tables ) )
  
  #############################################################################
  @staticmethod
  def __buildProduction( production, condition, tables, visible = default ):
    """it adds the production which can be a list or string to the jobs table
    :param list,int long the production number(s)
    :param str condition It contains the where conditions
    :param str tables it containes the tables. 
    :param str visible the default value is 'ALL'. [Y,N]
    """
    if production not in [default, None]:
      if isinstance( production, list ) and len( production ) > 0:
        condition += ' and '
        cond = ' ( '
        for i in production:
          cond += ' j.production=%s or ' % str( i )
        cond = cond[:-3] + ')'
        condition += cond
      elif isinstance( production, ( basestring, int, long ) ):
        condition += ' and j.production=%s' % str( production )
    
    if production not in [default, None] and visible.upper().startswith( 'Y' ) and 'BVIEW' in tables.upper():
      condition += ' and j.production=bview.production '
    
    return S_OK( ( condition, tables ) )

  #############################################################################
  @staticmethod
  def __buildTCKS( tcks, condition, tables ):
    """ it adds the tck to the jobs table"""

    if tcks not in [None, default]:
      if isinstance( tcks, list ):
        if len( tcks ) > 0:
          cond = '('
          for i in tcks:
            cond += "j.tck='%s' or " % ( i )
          cond = cond[:-3] + ')'
          condition = " and %s " % ( cond )
      elif isinstance( tcks, basestring ):
        condition += " and j.tck='%s'" % ( tcks )
      else:
        return S_ERROR( 'The TCK should be a list or a string' )

    return S_OK( ( condition, tables ) )

  #############################################################################
  def __buildProcessingPass( self, procPass, condition, tables, visible = default ):
    """It adds the processing pass condition to the query
    :param str procPass it is a processing pass for example: /Real Data/Reco20
    :param str condition It contains the where conditions
    :param str tables it containes the tables. 
    :param str visible the default value is 'ALL'. [Y,N]
    """
    if procPass not in [default, None]:
      if not re.search( '^/', procPass ):
        procPass = procPass.replace( procPass, '/%s' % procPass )
      command = "select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                           FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                     where v.path='%s'" % ( procPass.split( '/' )[1], procPass )
      retVal = self.dbR_.query( command )
      if not retVal['OK']:
        return retVal

      if len( retVal['Value'] ) < 1:
        return S_ERROR( 'No file found! Processing pass is missing!' )

      pro = '('
      for i in retVal['Value']:
        pro += "%s," % ( str( i[0] ) )
      pro = pro[:-1]
      pro += ')'

      if visible.upper().startswith( 'Y' ): 
        if 'BVIEW' not in tables.upper():
          tables += ',prodview bview'
        condition += " and bview.production=prod.production and bview.production=j.production"
        
      condition += " and j.production=prod.production \
                     and prod.processingid in %s" % ( pro )
      if tables.upper().find( 'PRODUCTIONSCONTAINER' ) < 0:
        tables += ',productionscontainer prod'
    return S_OK( ( condition, tables ) )

  #############################################################################
  @staticmethod
  def __buildFileTypes( ftype, condition, tables, visible = default ):
    """it adds the file type to the files list"""
    
    if ftype != default and visible.upper().startswith( 'Y' ):
      if tables.lower().find( 'filetypes' ) < 0:
        tables += ' ,filetypes ft'
      if tables.find( 'bview' ) < 0:
        tables += ' ,prodview bview'
      condition += " and f.filetypeid=ft.filetypeid and bview.filetypeid=ft.filetypeid and bview.filetypeid=f.filetypeid "
      if isinstance( ftype, list ):
        values = ' and ft.name in ('
        for i in ftype:
          values += " '%s'," % ( i )
        condition += values[:-1] + ')'
      else:
        condition += " and ft.name='%s' " % ( str( ftype ) )
        
    elif ftype not in [default, None]:
      if tables.lower().find( 'filetypes' ) < 0:
        tables += ' ,filetypes ft'
      if isinstance( ftype, list ) and len( ftype ) > 0:
        condition += ' and '
        cond = ' ( '
        for i in ftype:
          cond += " ft.name='%s' or " % ( i )
        cond = cond[:-3] + ')'
        condition += cond
      elif isinstance( ftype, basestring ):
        condition += " and ft.name='%s'" % ( ftype )
      else:
        return S_ERROR( 'File type problem!' )
      condition += ' and f.filetypeid=ft.filetypeid'
    return S_OK( ( condition, tables ) )

  #############################################################################
  @staticmethod
  def __buildRunnumbers( runnumbers, startRunID, endRunID, condition, tables ):
    """it adds the run numbers or start end run to the jobs table"""
    cond = None
    if isinstance( runnumbers, ( int, long ) ):
      condition = ' and j.runnumber=%s' % ( str( runnumbers ) )
    elif isinstance( runnumbers, basestring ) and runnumbers.upper() != default:
      condition = ' and j.runnumber=%s' % ( str( runnumbers ) )
    elif isinstance( runnumbers, list ) and len( runnumbers ) > 0:
      cond = ' ( '
      for i in runnumbers:
        cond += ' j.runnumber=' + str( i ) + ' or '
      cond = cond[:-3] + ')'
      if startRunID != None and endRunID != None:
        condition += ' and (j.runnumber>=%s and j.runnumber<=%s or %s)' % ( str( startRunID ), str( endRunID ), cond )
      elif startRunID != None or endRunID != None:
        condition += " and %s " % ( cond )
      elif startRunID == None or endRunID == None:
        condition += " and %s " % ( cond )
    else:
      if ( isinstance( startRunID, basestring ) and startRunID.upper() != default ) or\
         ( isinstance( startRunID, ( int, long ) ) and startRunID != None ):
        condition += ' and j.runnumber>=' + str( startRunID )
      if ( isinstance( endRunID, basestring ) and endRunID.upper() != default ) or\
         ( isinstance( endRunID, ( int, long ) ) and endRunID != None ) :
        condition += ' and j.runnumber<=' + str( endRunID )
    return S_OK( ( condition, tables ) )

  #############################################################################
  @staticmethod
  def __buildEventType( evt, condition, tables, visible = default):
    """adds the event type to the files table"""

    if evt not in [0, None, default] and visible.upper().startswith( 'Y' ):
      if tables.find( 'bview' ) < 0:   
        tables += ' ,prodview bview'
      
      if tables.upper().find( 'PRODUCTIONSCONTAINER' ) > 0:
        condition += '  and j.production=bview.production and bview.production=prod.production and f.eventtypeid=bview.eventtypeid and'
      else:
        condition += '  and j.production=bview.production and f.eventtypeid=bview.eventtypeid and'
      
      if isinstance( evt, ( list, tuple ) ) and len( evt ) > 0:
        cond = ' ( '
        for i in evt:
          cond += " bview.eventtypeid=%s or " % ( str( i ) )
        cond = cond[:-3] + ')'
        condition += cond
      elif isinstance( evt, ( basestring, int, long ) ):
        condition += ' bview.eventtypeid=' + str( evt )
      
    elif evt not in [0, None, default]:
      if isinstance( evt, ( list, tuple ) ) and len( evt ) > 0:
        condition += ' and '
        cond = ' ( '
        for i in evt:
          cond += " f.eventtypeid=%s or " % ( str( i ) )
        cond = cond[:-3] + ')'
        condition += cond
      elif isinstance( evt, (basestring, int, long ) ):
        condition += ' and f.eventtypeid=' + str( evt )
    return S_OK( ( condition, tables ) )

  #############################################################################
  @staticmethod
  def __buildStartenddate( startDate, endDate, condition, tables ):
    """it adds the start and end date to the files table"""
    if startDate not in [None, default, []]:
      condition += " and f.inserttimestamp >= TO_TIMESTAMP ('%s','YYYY-MM-DD HH24:MI:SS')" % ( str( startDate ) )

    if endDate not in [None, default, []]:
      condition += " and f.inserttimestamp <= TO_TIMESTAMP ('%s','YYYY-MM-DD HH24:MI:SS')" % ( str( endDate ) )
    elif startDate not in [None, default, []] and endDate in [None, default, []]:
      currentTimeStamp = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
      condition += " and f.inserttimestamp <= TO_TIMESTAMP ('%s','YYYY-MM-DD HH24:MI:SS')" % ( str( currentTimeStamp ) )
    return S_OK( ( condition, tables ) )

  #############################################################################
  def __buildDataquality( self, flag, condition, tables ):
    """it adds the data quality to the files table"""
    if flag not in [default, None]:
      if isinstance( flag, ( list, tuple ) ):
        conds = ' ('
        for i in flag:
          quality = None
          command = "select QualityId from dataquality where dataqualityflag='%s'" % ( str( i ) )
          res = self.dbR_.query( command )
          if not res['OK']:
            gLogger.error( 'Data quality problem:', res['Message'] )
          elif len( res['Value'] ) == 0:
            return S_ERROR( 'No file found! Dataquality is missing!' )
          else:
            quality = res['Value'][0][0]
          conds += ' f.qualityid=' + str( quality ) + ' or'
        condition += ' and' + conds[:-3] + ')'
      else:
        quality = None
        command = 'select QualityId from dataquality where dataqualityflag=\'' + str( flag ) + '\''
        res = self.dbR_.query( command )
        if not res['OK']:
          gLogger.error( 'Data quality problem:', res['Message'] )
        elif len( res['Value'] ) == 0:
          return S_ERROR( 'No file found! Dataquality is missing!' )
        else:
          quality = res['Value'][0][0]

        condition += ' and f.qualityid=' + str( quality )
    return S_OK( ( condition, tables ) )

  #############################################################################
  @staticmethod
  def __buildReplicaflag( replicaFlag, condition, tables ):
    """it adds the replica flag tp the files atble"""
    if replicaFlag in ['Yes', 'No']:
      condition += " and f.gotreplica='%s' " % replicaFlag

    return S_OK( ( condition, tables ) )

  #############################################################################
  @staticmethod
  def __buildVisibilityflag( visible, condition, tables ):
    """it adds the visibility flag to the files table"""
    if not visible.upper().startswith( 'A' ):
      if visible.upper().startswith( 'Y' ):
        condition += " and f.visibilityflag='Y'"
      elif visible.upper().startswith( 'N' ):
        condition += " and f.visibilityflag='N'"
    if tables.upper().find( 'FILES' ) < 0:
        tables += ' ,file f '
    if tables.upper().find( 'JOBS' ) < 0:
        tables += ' ,jobs j '
    return S_OK( ( condition, tables ) )

  #############################################################################
  def __buildConditions( self, simdesc, datataking, condition, tables, visible=default ):
    """adds the data taking or simulation conditions to the query"""
    if simdesc != default or datataking != default:
      conddesc = simdesc if simdesc != default else datataking
      retVal = self.__getConditionString( conddesc, 'prod' )
      if not retVal['OK']:
        return retVal
      condition += retVal['Value']
      condition += ' and prod.production=j.production '
      if tables.upper().find( 'PRODUCTIONSCONTAINER' ) < 0:
        tables += ' ,productionscontainer prod '
      
      if ( simdesc != default or datataking != default ) and visible.upper().startswith( 'Y' ):
        condition += ' and bview.production=prod.production'
        
    return S_OK( ( condition, tables ) )

  #############################################################################
  def getVisibleFilesWithMetadata( self, simdesc, datataking,
                                  procPass, ftype, evt,
                                  configName = default, configVersion = default,
                                  production = default, flag = default,
                                  startDate = None, endDate = None,
                                  nbofEvents = False, startRunID = None,
                                  endRunID = None, runnumbers = list(), replicaFlag = 'Yes', tcks = list() ):
    """returns the visible files"""
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

    if simdesc != default:
      conddescription = simdesc
    else:
      conddescription = datataking

    sim_dq_conditions = ''
    if conddescription != default:
      retVal = self.__getConditionString( conddescription, 'prod' )
      if retVal['OK']:
        sim_dq_conditions = retVal['Value']
      else:
        return retVal

    if production != default:
      if isinstance( production, list ):
        condition += ' and '
        cond = ' ( '
        for i in production:
          cond += 'j.production=' + str( i ) + ' or '
        cond = cond[:-3] + ')'
        condition += cond
      else:
        condition += ' and j.production=' + str( production )

    if len( runnumbers ) > 0:
      if isinstance( runnumbers, list ):
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
      if isinstance( ftype, list ):
        for i in ftype:
          condition += " and ftypes.Name='%s'" % ( str( i ) )
          fcond += " and ftypes.Name='%s'" % ( str( i ) )

      elif isinstance( ftype, basestring ):
        condition += " and ftypes.Name='%s'" % ( str( ftype ) )
        fcond += " and ftypes.Name='%s'" % ( str( ftype ) )
      fcond += 'and bview.filetypeid=ftypes.filetypeid '
      condition += ' and f.filetypeid=ftypes.filetypeid '
    econd = ''
    if evt != 0:
      if isinstance( evt, (list, tuple)):
        econd += " and bview.eventtypeid=%s" % ( str( i ) )
        condition += " and f.eventtypeid=%s" % ( str( i ) )

      elif isinstance( evt, ( basestring, int, long ) ):
        econd += " and bview.eventtypeid='%s'" % ( str( evt ) )
        condition += " and f.eventtypeid=%s" % ( str( evt ) )

    if len( tcks ) > 0:
      if isinstance( tcks, list ):
        cond = '('
        for i in tcks:
          cond += "j.tck='%s' or " % ( i )
        cond = cond[:-3] + ')'
        condition = " and %s " % ( cond )
      elif isinstance( tcks, basestring ):
        condition += " and j.tck='%s'" % ( tcks )
      else:
        return S_ERROR( 'The TCK should be a list or a string' )

    if procPass != default:
      if not re.search( '^/', procPass ):
        procPass = procPass.replace( procPass, '/%s' % procPass )
      condition += " and j.production in (select  bview.production from productionscontainer prod,\
       ( select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID FROM processing v \
                     START WITH id in (select distinct id from processing where name='%s') \
                                           CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                         where v.path='%s') proc, prodview bview %s \
                             where \
                               prod.processingid=proc.id and \
                               bview.production=prod.production \
                               %s %s %s \
                )" % ( procPass.split( '/' )[1], procPass, tables2, fcond, econd, sim_dq_conditions )

    if startDate != None:
      condition += ' and f.inserttimestamp >= TO_TIMESTAMP (\'' + str( startDate ) + '\',\'YYYY-MM-DD HH24:MI:SS\')'

    if endDate != None:
      condition += ' and f.inserttimestamp <= TO_TIMESTAMP (\'' + str( endDate ) + '\',\'YYYY-MM-DD HH24:MI:SS\')'
    elif startDate != None and endDate == None:
      currentTimestamp = datetime.datetime.utcnow().strftime( '%Y-%m-%d %H:%M:%S' )
      condition += " and f.inserttimestamp <= TO_TIMESTAMP ('%s','YYYY-MM-DD HH24:MI:SS')" % ( str( currentTimestamp ) )

    if flag != default:
      if isinstance( flag, ( list, tuple ) ):
        conds = ' ('
        for i in flag:
          quality = None
          command = 'select QualityId from dataquality where dataqualityflag=\'' + str( i ) + '\''
          res = self.dbR_.query( command )
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
        res = self.dbR_.query( command )
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

    command = " select distinct f.filename, f.eventstat, j.eventinputstat, \
     j.runnumber, j.fillnumber, f.filesize, j.totalluminosity, f.luminosity, f.instLuminosity, j.tck from %s where\
     f.jobid= j.jobid and f.visibilityflag='Y'  and f.gotreplica='Yes' %s " % ( tables, condition )

    res = self.dbR_.query( command )

    return res

  #############################################################################
  def getFilesSummary( self, configName, configVersion,
                      conddescription = default, processing = default,
                      evt = default, production = default,
                      filetype = default, quality = default,
                      runnb = default, startrun = default, endrun = default,
                      visible = default, startDate = None, endDate = None,
                      runnumbers = list(), replicaflag = default ):

    """retuns the number of event, files, etc for a given dataset"""
    condition = ''
    tables = 'files f, jobs j '
    
    retVal = self.__buildStartenddate( startDate, endDate, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']
    
    retVal = self.__buildRunnumbers( runnumbers, startrun, endrun, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildConditions( default, conddescription, condition, tables, visible )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']
    
    retVal = self.__buildVisibilityflag( visible, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']
    
    retVal = self.__buildConfiguration( configName, configVersion, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildProduction( production, condition, tables, visible )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']
    
    retVal = self.__buildEventType( evt, condition, tables, visible )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']
    
    if production != default:
      condition += ' and j.production=' + str( production )

    retVal = self.__buildFileTypes( filetype, condition, tables, visible )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']
    
    retVal = self.__buildReplicaflag( replicaflag, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']

    retVal = self.__buildProcessingPass( processing, condition, tables, visible )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']
    
    retVal = self.__buildDataquality( quality, condition, tables )
    if not retVal['OK']:
      return retVal
    condition, tables = retVal['Value']   

    command = "select count(*),\
    SUM(f.EventStat), SUM(f.FILESIZE), \
    SUM(f.luminosity),SUM(f.instLuminosity) from  %s  where \
    j.jobid=f.jobid and \
    ft.filetypeid=f.filetypeid and \
    ft.filetypeid=f.filetypeid  %s" % ( tables, condition )
    return self.dbR_.query( command )

  #############################################################################
  def getLimitedFiles( self, configName, configVersion,
                      conddescription = default, processing = default,
                      evt = default, production = default,
                      filetype = default, quality = default,
                      runnb = default, startitem = 0, maxitems = 10 ):
    """returns a list of limited number of files"""
    condition = ''
    if configName != default:
      condition += " and c.configname='%s' " % ( configName )

    if configVersion != default:
      condition += " and c.configversion='%s' " % ( configVersion )

    sim_dq_conditions = ''
    if conddescription != default:
      retVal = self.__getConditionString( conddescription, 'prod' )
      if retVal['OK']:
        sim_dq_conditions = retVal['Value']
        condition += sim_dq_conditions
      else:
        return retVal

    tables = ''
    if evt != default:
      tables += ' ,prodview bview'
      condition += '  and j.production=bview.production and bview.production=prod.production\
       and bview.eventtypeid=%s and f.eventtypeid=bview.eventtypeid ' % ( evt )

    if production != default:
      condition += ' and j.production=%d' % ( int( production ) )

    if runnb != default:
      condition += ' and j.runnumber=%d' % ( int( runnb ) )

    if filetype != default:
      condition += " and ftypes.name='%s' and bview.filetypeid=ftypes.filetypeid " % ( filetype )

    if quality != 'ALL':
      tables += ' ,dataquality d '
      condition += 'and d.qualityid=f.qualityid '
      if isinstance( quality, basestring ):
        command = "select QualityId from dataquality where dataqualityflag='%s'" % ( quality )
        res = self.dbR_.query( command )
        if not res['OK']:
          gLogger.error( 'Data quality problem:', res['Message'] )
        elif len( res['Value'] ) == 0:
          return S_ERROR( 'Dataquality is missing!' )
        else:
          quality = res['Value'][0][0]
        condition += ' and f.qualityid=' + str( quality )
      else:
        if isinstance( quality, list ) and len( quality ) > 0:
          conds = ' ('
          for i in quality:
            quality = None
	    command = "select QualityId from dataquality where dataqualityflag='%s'" % ( i )
	    res = self.dbR_.query( command )
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
        where v.path='%s') %s" % ( processing.split( '/' )[1], processing, sim_dq_conditions )

      retVal = self.dbR_.query( command )
      if not retVal['OK']:
        return retVal
      pro = '('
      for i in retVal['Value']:
        pro += "%s," % ( str( i[0] ) )
      pro = pro[:-1]
      pro += ( ')' )

      condition += " and prod.processingid in %s" % ( pro )

    command = "select fname, fstat, fsize, fcreation, jstat, jend, jnode, ftypen, evttypeid, \
    jrun, jfill, ffull, dflag,   jevent, jtotal, flum, finst, jtck from \
              (select rownum r, fname, fstat, fsize, fcreation, jstat, jend, jnode, ftypen,\
               evttypeid, jrun, jfill, ffull, dflag,   jevent, jtotal, flum, finst, jtck from \
                  (select ROWNUM r, f.FileName fname, f.EventStat fstat, f.FileSize fsize, \
                  f.CreationDate fcreation, j.JobStart jstat, j.JobEnd jend, j.WorkerNode jnode, \
                  ftypes.Name ftypen, f.eventtypeid evttypeid, j.runnumber jrun, j.fillnumber jfill,\
                   f.fullstat ffull, d.dataqualityflag dflag,j.eventinputstat jevent, j.totalluminosity jtotal,\
                           f.luminosity flum, f.instLuminosity finst, j.tck jtck from files f, jobs j,\
                            productionscontainer prod, configurations c, filetypes ftypes %s where \
    j.jobid=f.jobid and \
    ftypes.filetypeid=f.filetypeid and \
    f.gotreplica='Yes' and \
    f.visibilityflag='Y' and \
    j.configurationid=c.configurationid  %s) where\
     rownum <=%d ) where r >%d" % ( tables, condition, int( maxitems ), int( startitem ) )
    return self.dbR_.query( command )

  #############################################################################
  def getDataTakingCondId( self, condition ):
    """returns the data taking conditions identifier"""
    command = 'select DaqPeriodId from data_taking_conditions where '
    for param in condition:
      if isinstance( condition[param], basestring ) and len( condition[param].strip() ) == 0:
        command += str( param ) + ' is NULL and '
      elif condition[param] != None:
        command += str( param ) + '=\'' + condition[param] + '\' and '
      else:
        command += str( param ) + ' is NULL and '

    command = command[:-4]
    res = self.dbR_.query( command )
    if res['OK']:
      if len( res['Value'] ) == 0:
        command = 'select DaqPeriodId from data_taking_conditions where '
        for param in condition:
          if param != 'Description':
            if isinstance( condition[param], basestring ) and len( condition[param].strip() ) == 0:
              command += str( param ) + ' is NULL and '
            elif condition[param] != None:
              command += str( param ) + '=\'' + condition[param] + '\' and '
            else:
              command += str( param ) + ' is NULL and '

        command = command[:-4]
        retVal = self.dbR_.query( command )
        if retVal['OK']:
          if len( retVal['Value'] ) != 0:
            return S_ERROR( 'Only the Description is different, \
            the other attributes are the same and they are exists in the DB!' )
    return res

  #############################################################################
  def getDataTakingCondDesc( self, condition ):
    """return the data taking description which adequate a given conditions."""
    command = 'select description from data_taking_conditions where '
    for param in condition:
      if isinstance( condition[param], basestring ) and len( condition[param].strip() ) == 0:
        command += str( param ) + ' is NULL and '
      elif condition[param] != None:
        command += str( param ) + '=\'' + condition[param] + '\' and '
      else:
        command += str( param ) + ' is NULL and '

    command = command[:-4]
    res = self.dbR_.query( command )
    if res['OK']:
      if len( res['Value'] ) == 0:
        command = 'select DaqPeriodId from data_taking_conditions where '
        for param in condition:
          if param != 'Description':
            if isinstance( condition[param], basestring ) and len( condition[param].strip() ) == 0:
              command += str( param ) + ' is NULL and '
            elif condition[param] != None:
              command += str( param ) + '=\'' + condition[param] + '\' and '
            else:
              command += str( param ) + ' is NULL and '

        command = command[:-4]
        retVal = self.dbR_.query( command )
        if retVal['OK']:
          if len( retVal['Value'] ) != 0:
            return S_ERROR( 'Only the Description is different,\
             the other attributes are the same and they are exists in the DB!' )
    return res

  #############################################################################
  def getStepIdandNameForRUN( self, programName, programVersion, conddb, dddb ):
    """returns the step used to process data"""
    dataset = {'Step':{'StepName':'Real Data',
                       'ApplicationName':programName,
                       'ApplicationVersion':programVersion,
                       'ProcessingPass':'Real Data',
                       'Visible':'Y',
                       'CONDDB':None,
                       'DDDB':None},
               'OutputFileTypes':[{'FileType':'RAW',
                                   'Visible':'Y'}]}
    condition = ''
    if conddb == None or conddb == '':
      condition += " and CondDB is NULL "
      dataset['Step'].pop( 'CONDDB' )
    else:
      condition += " and CondDB='%s' " % ( conddb )
      dataset['Step']['CONDDB'] = conddb

    if dddb == None or dddb == '':
      condition += " and DDDB is NULL "
      dataset['Step'].pop( 'DDDB' )
    else:
      condition += " and DDDB='%s'" % ( dddb )
      dataset['Step']['DDDB'] = dddb

    command = "select stepid, stepname from steps where applicationname='%s' \
    and applicationversion='%s' %s " % ( programName, programVersion, condition )
    retVal = self.dbR_.query( command )
    if retVal['OK']:
      if len( retVal['Value'] ) == 0:
        retVal = self.insertStep( dataset )
        if retVal['OK']:
          return S_OK( [retVal['Value'], 'Real Data'] )
        else:
          return retVal
      else:
        return S_OK( [retVal['Value'][0][0], retVal['Value'][0][1]] )
    else:
      return retVal

  #############################################################################
  def __getPassIds( self, name ):
    """returns the processing pass ids for a given processing pass name"""
    command = "select id from processing where name='%s'" % ( name )
    retVal = self.dbR_.query( command )
    if retVal['OK']:
      result = []
      for i in retVal['Value']:
        result += [i[0]]
      return S_OK( result )
    else:
      return retVal

  #############################################################################
  def __getprocessingid( self, processingpassid ):
    """returns the processing pass name for a given processing pass identifier"""
    command = 'SELECT name "Name", CONNECT_BY_ISCYCLE "Cycle", \
   LEVEL, SYS_CONNECT_BY_PATH(name, \'/\') "Path", id "ID" \
   FROM processing \
   START WITH id=' + str( processingpassid ) + '\
   CONNECT BY NOCYCLE PRIOR  parentid=id AND LEVEL <= 5 \
   ORDER BY  Level desc, "Name", "Cycle", "Path"'
    return self.dbR_.query( command )

  #############################################################################
  @staticmethod
  def __checkprocessingpass( opath, values ):
    """checks the processing pass"""
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
  def __insertprocessing( self, values, parentid = None, ids = list() ):
    """inserts a processing pass"""
    for i in values:
      command = ''
      if parentid != None:
        command = "select id from processing where name='%s' and parentid=%s" % ( i, parentid )
      else:
        command = "select id from processing where name='%s' and parentid is null" % ( i )
      retVal = self.dbR_.query( command )
      if retVal['OK']:
        if len( retVal['Value'] ) == 0:
          if parentid != None:
            command = 'select max(id)+1 from processing'
            retVal = self.dbR_.query( command )
            if retVal['OK']:
              processingpassid = retVal['Value'][0][0]
              ids += [processingpassid]
              command = "insert into processing(id,parentid,name)values(%d,%d,'%s')" % ( processingpassid, parentid, i )
              retVal = self.dbW_.query( command )
              if not retVal['OK']:
                gLogger.error( retVal['Message'] )
              values.remove( i )
              self.__insertprocessing( values, processingpassid, ids )
          else:
            command = 'select max(id)+1 from processing'
            retVal = self.dbR_.query( command )
            if retVal['OK']:
              processingpassid = retVal['Value'][0][0]
              if processingpassid == None:
                processingpassid = 1
              ids += [processingpassid]
              command = "insert into processing(id,parentid,name)values(%d,null,'%s')" % ( processingpassid, i )
              retVal = self.dbW_.query( command )
              if not retVal['OK']:
                gLogger.error( retVal['Message'] )
              values.remove( i )
              self.__insertprocessing( values, processingpassid, ids )
        else:

          values.remove( i )
          parentid = retVal['Value'][0][0]
          ids += [parentid]
          self.__insertprocessing( values, parentid, ids )


  #############################################################################
  def addProcessing( self, path ):
    """adds a new processing pass"""
    lastindex = len( path ) - 1
    retVal = self.__getPassIds( path[lastindex] )
    stepids = []
    if not retVal['OK']:
      return retVal
    else:
      ids = retVal['Value']
      if len( ids ) == 0:
        newpath = list( path )
        self.__insertprocessing( newpath, None, stepids )
        return S_OK( stepids[-1:] )
      else:
        for i in ids:
          procs = self.__getprocessingid( i )
          if len( procs ) > 0:
            if self.__checkprocessingpass( path, procs ):
              return S_OK()
        newpath = list( path )
        self.__insertprocessing( newpath, None, stepids )
        return S_OK( stepids[-1:] )
    return S_ERROR()

  #############################################################################
  def insertStepsContainer( self, prod, stepid, step ):
    """inserts a step to the stepcontainer"""
    return self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.insertStepsContainer', [prod, stepid, step], False )

  #############################################################################
  def insertproductionscontainer( self, prod, processingid, simid, daqperiodid ):
    """inserts a production to the productions container"""
    return self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.insertproductionscontainer',
                                            [ prod, processingid, simid, daqperiodid], False )

  #############################################################################
  def addProductionSteps( self, steps, prod ):
    """adds a step to a production"""
    level = 1
    for i in steps:
      retVal = self.insertStepsContainer( prod, i['StepId'], level )
      if not retVal['OK']:
        return retVal
      level += 1
    return S_OK()

  #############################################################################
  def checkProcessingPassAndSimCond( self, production ):
    """checks the processing pass and simulation condition"""
    command = ' select count(*) from productionscontainer where production=' + str( production )
    res = self.dbR_.query( command )
    return res

  #############################################################################
  def addProduction( self, production, simcond = None, daq = None, steps = default, inputproc = '' ):
    """adds a production"""
    path = []
    if inputproc != '':
      if inputproc[0] != '/':
        inputproc = '/' + inputproc
      path = inputproc.split( '/' )[1:]

    for i in steps:
      if i['Visible'] == 'Y':
        res = self.getAvailableSteps( {'StepId':i['StepId']} )
        if not res['OK']:
          gLogger.error( res['Message'] )
          return res
        if res['Value']['TotalRecords'] > 0:
          procpas = res['Value']['Records'][0][9]
          path += [procpas]
        else:
          return S_ERROR( 'This step is missing' )

    if len( path ) == 0:
      return S_ERROR( 'You have to define the input processing pass or you have to have a visible step!' )
    processingid = None
    retVal = self.addProcessing( path )
    if not retVal['OK']:
      return retVal
    else:
      if len( retVal['Value'] ) > 0:
        processingid = retVal['Value'][0]
      else:
        return S_ERROR( 'The proccesing pass exists! You have to write lhcb-bookkeeping@cern.ch!' )
    retVal = self.addProductionSteps( steps, production )
    if retVal['OK']:
      sim = None
      did = None
      if daq != None:
        retVal = self.__getDataTakingConditionId( daq )
        if retVal['OK'] and retVal['Value'] > -1:
          did = retVal['Value']
        else:
          return S_ERROR( 'Data taking condition is missing!!' )
      if simcond != None:
        retVal = self.__getSimulationConditioId( simcond )
        if retVal['OK'] and retVal['Value'] > -1:
          sim = retVal['Value']
        else:
          return S_ERROR( 'Data taking condition is missing!!' )
      return self.insertproductionscontainer( production, processingid, sim, did )
    else:
      return retVal
    return S_OK( 'The production processing pass is entered to the bkk' )


  #############################################################################
  def getEventTypes( self, configName = default, configVersion = default, prod = default ):
    """returns the events types for a given dataset"""
    condition = ''
    result = S_ERROR()
    if configName != default:
      condition += " prodview.configname='%s' " % ( configName )

    if configVersion != default:
      condition += " and prodview.configversion='%s' " % ( configVersion )

    if prod != default:
      if condition == '':
        condition += " prodview.production=%d" % ( int( prod ) )
      else:
        condition += " and prodview.production=%d" % ( int( prod ) )

    command = ' select distinct prodview.eventtypeid, prodview.description from  prodview where ' + condition
    retVal = self.dbR_.query( command )
    records = []
    if retVal['OK']:
      parameters = ['EventType', 'Description']
      for record in retVal['Value']:
        records += [list( record )]
      result = S_OK( {'ParameterNames':parameters, 'Records':records, 'TotalRecords':len( records )} )
    else:
      result = retVal
    return result

  #############################################################################
  def getProcessingPassSteps( self, procpass = default, cond = default, stepname = default ):
    """returns the steps with metadata"""
    processing = {}
    condition = ''

    if procpass != default:
      condition += " and prod.processingid in ( \
                    select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                        FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                        CONNECT BY NOCYCLE PRIOR  id=parentid) v where v.path='%s' \
                       )" % ( procpass.split( '/' )[1], procpass )

    if cond != default:
      retVal = self.__getConditionString( cond, 'prod' )
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal


    if stepname != default:
      condition += " and s.processingpass='%s' " % ( stepname )

    command = "select distinct s.stepid,s.stepname,s.applicationname,s.applicationversion, \
    s.optionfiles,s.dddb, s.conddb,s.extrapackages,s.visible, cont.step \
                from steps s, productionscontainer prod, stepscontainer cont \
               where \
              cont.stepid=s.stepid and \
              prod.production=cont.production %s order by cont.step" % ( condition )

    retVal = self.dbR_.query( command )
    records = []
    # parametersNames = [ 'StepId', 'StepName','ApplicationName', 'ApplicationVersion',
    # 'OptionFiles','DDDB','CONDDB','ExtraPackages','Visible']
    parametersNames = ['id', 'name']
    if retVal['OK']:
      nb = 0
      for i in retVal['Value']:
        # records = [[i[0],i[1],i[2],i[3],i[4],i[5],i[6], i[7], i[8]]]
        records = [ ['StepId', i[0]],
                   ['StepName', i[1]],
                   ['ApplicationName', i[2]],
                   ['ApplicationVersion', i[3]],
                   ['OptionFiles', i[4]],
                   ['DDDB', i[5]],
                   ['CONDDB', i[6]],
                   ['ExtraPackages', i[7]],
                   ['Visible', i[8]]]
        step = 'Step-%s' % ( i[0] )
        processing[step] = records
        nb += 1
    else:
      return retVal

    return S_OK( {'Parameters':parametersNames, 'Records':processing, 'TotalRecords':nb} )

  #############################################################################
  def getProductionProcessingPassSteps( self, prod ):
    """returns the production processing pass"""
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
                       )" % ( procpass.split( '/' )[1], procpass )

    command = "select distinct s.stepid,s.stepname,s.applicationname,s.applicationversion, \
    s.optionfiles,s.dddb, s.conddb,s.extrapackages,s.visible, cont.step \
                from steps s, productionscontainer prod, stepscontainer cont \
               where \
              cont.stepid=s.stepid and \
              prod.production=cont.production %s and prod.production=%dorder by cont.step" % ( condition, prod )

    retVal = self.dbR_.query( command )
    records = []
    # parametersNames = [ 'StepId', 'StepName','ApplicationName',
    # 'ApplicationVersion','OptionFiles','DDDB','CONDDB','ExtraPackages','Visible']
    parametersNames = ['id', 'name']
    if retVal['OK']:
      nb = 0
      for i in retVal['Value']:
        # records = [[i[0],i[1],i[2],i[3],i[4],i[5],i[6], i[7], i[8]]]
        records = [ ['StepId', i[0]],
                   ['ProcessingPass', procpass],
                   ['ApplicationName', i[2]],
                   ['ApplicationVersion', i[3]],
                   ['OptionFiles', i[4]],
                   ['DDDB', i[5]],
                   ['CONDDB', i[6]],
                   ['ExtraPackages', i[7]],
                   ['Visible', i[8]]]
        step = i[1]
        processing[step] = records
        nb += 1
    else:
      return retVal

    return S_OK( {'Parameters':parametersNames, 'Records':processing, 'TotalRecords':nb} )

  #############################################################################
  def getRuns( self, cName, cVersion ):
    """returns the runs"""
    return self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getRuns', [cName, cVersion] )

  #############################################################################
  def getRunAndProcessingPass( self, runnb ):
    """returns the processing pass of a run"""
    command = "select distinct runnumber, processingpass from table (BOOKKEEPINGORACLEDB.getRunProcPass(%d))" % ( runnb )
    return self.dbR_.query( command )
    # return self.dbR_.executeStoredProcedure('BOOKKEEPINGORACLEDB.getRunProcPass', [runnb])

  #############################################################################
  def getNbOfRawFiles( self, runid, eventtype, replicaFlag = 'Yes', visible = 'Y', isFinished = default):
    """retuns the number of raw files"""
    condition = ''
    tables = 'jobs j, files f'
    if eventtype != default:
      condition = ' and f.eventtypeid=%d' % ( eventtype )
    
    if visible != default:
      condition += " and f.visibilityFlag='%s'" % ( visible )
    
    if replicaFlag != default:
      condition += " and f.gotreplica='%s'" % ( replicaFlag )
    
    if isFinished != default:
      tables += ' ,runstatus r'
      condition += " and j.runnumber=r.runnumber and r.finished='%s' " % isFinished
    
    command = " select count(*) from %s  where \
    j.jobid=f.jobid and j.production<0 and j.runnumber=%d %s " % ( tables, runid, condition )
    return self.dbR_.query( command )

  #############################################################################
  def getFileTypeVersion( self, lfns ):
    """returns the format of an lfn"""
    result = None
    retVal = self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.bulkgetTypeVesrsion', [], True, lfns )
    if retVal['OK']:
      values = {}
      for i in retVal['Value']:
        values[i[0]] = i[1]
      result = S_OK( values )
    else:
      result = retVal
    return result

  #############################################################################
  def insertRuntimeProject( self, projectid, runtimeprojectid ):
    """inserts a runtime project"""
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.insertRuntimeProject',
                                              [projectid, runtimeprojectid], False )
    return result

  #############################################################################
  def updateRuntimeProject( self, projectid, runtimeprojectid ):
    """changes the runtime project"""
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.updateRuntimeProject',
                                              [projectid, runtimeprojectid], False )
    return result

  def removeRuntimeProject( self, stepid ):
    """removes the runtime project"""
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.removeRuntimeProject', [stepid], False )
    return result

  #############################################################################
  def getTCKs( self, configName, configVersion,
              conddescription = default, processing = default,
              evt = default, production = default,
              filetype = default, quality = default, runnb = default ):
    """returns the TCKs for a given dataset"""
    condition = ''
    if configName != default:
      condition += " and c.configname='%s' " % ( configName )

    if configVersion != default:
      condition += " and c.configversion='%s' " % ( configVersion )

    if conddescription != default:
      retVal = self.__getConditionString( conddescription, 'prod' )
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal

    tables = ''
    if evt != default:
      tables += ' ,prodview bview'
      condition += '  and j.production=bview.production and bview.production=prod.production and\
       bview.eventtypeid=%s and f.eventtypeid=bview.eventtypeid ' % ( evt )

    if production != default:
      condition += ' and j.production=' + str( production )

    if runnb != default:
      condition += ' and j.runnumber=' + str( runnb )

    if filetype != default:
      condition += " and ftypes.name='%s' and bview.filetypeid=ftypes.filetypeid " % ( str( filetype ) )

    if quality != default:
      if isinstance( quality, basestring ):
        command = "select QualityId from dataquality where dataqualityflag='" + str( quality ) + "'"
        res = self.dbR_.query( command )
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
            res = self.dbR_.query( command )
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
                     where v.path='%s'" % ( processing.split( '/' )[1], processing )
      retVal = self.dbR_.query( command )
      if not retVal['OK']:
        return retVal
      pro = '('
      for i in retVal['Value']:
        pro += "%s," % ( str( i[0] ) )
      pro = pro[:-1]
      pro += ( ')' )

      condition += " and prod.processingid in %s" % ( pro )

    command = "select distinct j.tck from files f, jobs j, productionscontainer prod,\
     configurations c, dataquality d, filetypes ftypes %s  where \
    j.jobid=f.jobid and \
    d.qualityid=f.qualityid and \
    f.gotreplica='Yes' and \
    f.visibilityFlag='Y' and \
    ftypes.filetypeid=f.filetypeid and \
    j.configurationid=c.configurationid %s" % ( tables, condition )
    return self.dbR_.query( command )

  #############################################################################
  def __prepareStepMetadata( self, configName, configVersion,
                       cond = default, procpass = default,
                       evt = default, production = default,
                       filetype = default, runnb = default, selection = '' ):
    """
    it generates the sql command depending on the selection
    """
    condition = ''
    tables = 'steps s, productionscontainer prod, stepscontainer cont, prodview bview'
    if configName != default:
      condition += " and bview.configname='%s' " % ( configName )

    if configVersion != default:
      condition += " and bview.configversion='%s' " % ( configVersion )

    if procpass != default:
      condition += " and prod.processingid in ( \
                    select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                        FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                        CONNECT BY NOCYCLE PRIOR  id=parentid) v where v.path='%s' \
                       )" % ( procpass.split( '/' )[1], procpass )

    if cond != default:
      retVal = self.__getConditionString( cond, 'prod' )
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal

    if evt != default:
      condition += '  and bview.eventtypeid=%s ' % ( str( evt ) )

    if production != default:
      condition += ' and bview.production=' + str( production )

    if runnb != default:
      tables += ' ,prodrunview rview'
      condition += ' and rview.production=bview.production and rview.runnumber=%d and bview.production<0' % ( runnb )

    if filetype != default:
      tables += ', filetypes ftypes'
      condition += " and ftypes.name='%s' and bview.filetypeid=ftypes.filetypeid " % ( filetype )

    command = "select %s  from  %s \
               where \
              cont.stepid=s.stepid and \
              prod.production=bview.production and \
              prod.production=cont.production %s order by cont.step" % ( selection, tables, condition )
    return command

  #############################################################################
  def getStepsMetadata( self, configName, configVersion,
                       cond = default, procpass = default,
                       evt = default, production = default,
                       filetype = default, runnb = default ):
    """returns the steps with metadata"""
    command = None
    processing = {}
    result = None
    if configName.upper().find( 'MC' ) >= 0:
      command = self.__prepareStepMetadata( configName,
                                          configVersion,
                                          cond,
                                          procpass,
                                          evt,
                                          production,
                                          filetype,
                                          runnb,
                                          selection = "prod.production" )
      retVal = self.dbR_.query( command )
      if not retVal['OK']:
        result = retVal
      else:
        production = tuple( [ i[0] for i in retVal['Value']] )
        gLogger.debug( 'Production' + str( production ) )
        parentprod = ()
        for i in production:
          command = "select j.production from jobs j, files f where \
        j.jobid=f.jobid and\
        f.fileid in (select i.fileid from jobs j, inputfiles i where \
        j.jobid=i.jobid and j.production = %d and ROWNUM <2)" % ( int( i ) )
          retVal = self.dbR_.query( command )
          if not retVal['OK']:
            result = retVal
            break
          else:
            prods = [ i[0] for i in retVal['Value']]
            parentprod = tuple( prods )
          gLogger.debug( 'Parent production:' + str( parentprod ) )
          condition = ''
          tables = 'steps s, productionscontainer prod, stepscontainer cont'
          if procpass != default:
            condition += " and prod.processingid in ( \
                          select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                              FROM processing v   START WITH id in (select distinct id from processing where name='%s') \
                                              CONNECT BY NOCYCLE PRIOR  id=parentid) v where v.path='%s' \
                             )" % ( procpass.split( '/' )[1], procpass )

          if cond != default:
            retVal = self.__getConditionString( cond, 'prod' )
            if retVal['OK']:
              condition += retVal['Value']
            else:
              return retVal
        
        parentprod += production
        command = "select distinct s.stepid,s.stepname,s.applicationname,s.applicationversion,\
       s.optionfiles,s.dddb, s.conddb,s.extrapackages,s.visible, cont.step  from  %s \
                     where cont.stepid=s.stepid and \
                      prod.production=cont.production and\
                      prod.production in %s %s order by cont.step" % ( tables, str( parentprod ), condition )


    else:
      command = self.__prepareStepMetadata( configName,
                                        configVersion,
                                        cond,
                                        procpass,
                                        evt,
                                        production,
                                        filetype,
                                        runnb,
                                        selection = 'distinct s.stepid,s.stepname,s.applicationname,s.applicationversion,\
       s.optionfiles,s.dddb, s.conddb,s.extrapackages,s.visible, cont.step' )

    if not result:
      retVal = self.dbR_.query( command )
      records = []

      parametersNames = ['id', 'name']
      if retVal['OK']:
        nb = 0
        for i in retVal['Value']:
          # records = [[i[0],i[1],i[2],i[3],i[4],i[5],i[6], i[7], i[8]]]
          records = [ ['StepId', i[0]],
                     ['StepName', i[1]],
                     ['ApplicationName', i[2]],
                     ['ApplicationVersion', i[3]],
                     ['OptionFiles', i[4]],
                     ['DDDB', i[5]],
                     ['CONDDB', i[6]],
                     ['ExtraPackages', i[7]],
                     ['Visible', i[8]]]
          step = 'Step-%s' % ( i[0] )
          processing[step] = records
          nb += 1
        result = S_OK( {'Parameters':parametersNames, 'Records':processing, 'TotalRecords':nb} )
      else:
        result = retVal

    return result

  #############################################################################
  def getDirectoryMetadata( self, lfn ):
    """returns a directory meradata"""
    result = S_ERROR()
    lfn += '%'
    retVal = self.dbR_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.getDirectoryMetadata', [lfn] )
    records = []
    if retVal['OK']:
      for i in retVal['Value']:
        records += [dict( zip( ( 'Production',
                              'ConfigName',
                              'ConfigVersion',
                              'EventType',
                              'FileType',
                              'ProcessingPass',
                              'ConditionDescription',
                              'VisibilityFlag' ), i ) )]
      result = S_OK( records )
    else:
      result = retVal
    return result

  #############################################################################
  def getDirectoryMetadata_new( self, lfn ):
    """returns a directory meradata"""
    result = S_ERROR()
    lfns = [ i + '%' for i in lfn]
    retVal = self.dbR_.executeStoredProcedure( packageName = 'BOOKKEEPINGORACLEDB.getDirectoryMetadata_new',
                                                parameters = [],
                                                output = True,
                                                array = lfns )

    records = {}
    failed = []
    if retVal['OK']:
      for i in retVal['Value']:
        fileName = i[0][:-1]
        if fileName in records:
          records[fileName] += [dict( zip( ( 'Production',
                              'ConfigName',
                              'ConfigVersion',
                              'EventType',
                              'FileType',
                              'ProcessingPass',
                              'ConditionDescription',
                              'VisibilityFlag' ), i[1:] ) )]
        else:
          records[fileName] = [dict( zip( ( 'Production',
                              'ConfigName',
                              'ConfigVersion',
                              'EventType',
                              'FileType',
                              'ProcessingPass',
                              'ConditionDescription',
                              'VisibilityFlag' ), i[1:] ) )]
      failed = [ i[:-1] for i in lfns if i[:-1] not in records]
      result = S_OK( {'Successful':records, 'Failed':failed} )
    else:
      result = retVal
    return result

  #############################################################################
  def getFilesForGUID( self, guid ):
    """returns the file for a given GUID"""
    result = S_ERROR()
    retVal = self.dbW_.executeStoredFunctions( 'BOOKKEEPINGORACLEDB.getFilesForGUID', types.StringType, [guid] )
    if retVal['OK']:
      result = S_OK( retVal['Value'] )
    else:
      result = retVal
    return result

  #############################################################################
  def getRunsGroupedByDataTaking( self ):
    """returns the runs data taking description and production"""
    result = S_ERROR()
    command = " select d.description, r.runnumber, r.production from \
    prodrunview r, prodview p, data_taking_conditions d where \
    d.daqperiodid=p.daqperiodid and p.production=r.production \
     group by d.description,  r.runnumber, r.production order by r.runnumber"
    retVal = self.dbR_.query( command )
    values = {}
    if retVal['OK']:
      for i in retVal['Value']:
        rnb = i[1]
        desc = i[0]
        prod = i[2]
        if desc in values:
          if rnb in values[desc]:
            if prod > 0:
              values[desc][rnb] += [prod]
          else:
            if prod > 0:
              values[desc].update( {rnb:[prod]} )
            else:
              values[desc].update( {rnb:[]} )
        else:
          if prod > 0:
            values[desc] = {rnb:[prod]}
          else:
            values[desc] = {rnb:[]}
      result = S_OK( values )
    else:
      result = retVal
    return result

  #############################################################################
  def getListOfFills( self, configName = default,
                     configVersion = default,
                     conddescription = default ):
    """
    It returns a list of fills for a given condition.
    """
    result = None
    condition = ''
    if configName != default:
      condition += " and c.configname='%s' " % ( configName )

    if configVersion != default:
      condition += " and c.configversion='%s' " % ( configVersion )

    if conddescription != default:
      retVal = self.__getConditionString( conddescription, 'prod' )
      if retVal['OK']:
        condition += retVal['Value']
      else:
        return retVal

    command = "select distinct j.FillNumber from jobs j, productionscontainer prod,\
     configurations c where \
    j.configurationid=c.configurationid %s and \
    prod.production=j.production and j.production<0" % ( condition )
    retVal = self.dbR_.query( command )
    if not retVal['OK']:
      result = retVal
    else:
      result = S_OK( [i[0] for i in retVal['Value']] )
    return result

  #############################################################################
  def getRunsForFill( self, fillid ):
    """
    It returns a list of runs for a given FILL
    """

    result = None
    command = "select distinct j.runnumber from jobs j where j.production<0 and j.fillnumber=%d" % ( fillid )
    retVal = self.dbR_.query( command )
    if not retVal['OK']:
      result = retVal
    else:
      result = S_OK( [i[0] for i in retVal['Value']] )
    return result

  #############################################################################
  def getListOfRuns( self, configName = default, configVersion = default,
                     conddescription = default, processing = default,
                     evt = default, quality = default ):
    """return the runnumbers for a given dataset"""
    #### MUST BE REIMPLEMNETED!!!!!!
    ####
    ####
    command = ''
    if quality.upper().startswith( 'A' ):
      condition = ''
      if configName != default:
        condition += " and bview.configname='%s'" % ( configName )
      if configVersion != default:
        condition += " and bview.configversion='%s'" % ( configVersion )

      if conddescription != default:
        retVal = self.__getConditionString( conddescription, 'pcont' )
        if retVal['OK']:
          condition += retVal['Value']
        else:
          return retVal

      if evt != default:
        condition += ' and bview.eventtypeid=%d' % ( int( evt ) )

      if processing != default:
        command = "select distinct rview.runnumber from \
                   productionscontainer pcont,prodview bview, prodrunview rview \
                   where pcont.processingid in \
                      (select v.id from (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                             FROM processing v   START WITH id in \
                                             (select distinct id from processing where \
                                             name='" + str( processing.split( '/' )[1] ) + "') \
                                                CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                       where v.path='" + processing + "') \
                    and bview.production=pcont.production  and bview.production=rview.production" + condition
      else:
        command = "select distinct rview.runnumber from \
        productionscontainer pcont,prodview bview, prodrunview rview where \
                   bview.production=pcont.production and bview.production=rview.production " + condition
    else:
      condition = ''
      tables = ',files f'
      if configName != default:
        condition += " and c.configname='%s'" % ( configName )
      if configVersion != default:
        condition += " and c.configversion='%s'" % ( configVersion )
      if condition.find( 'and c.' ) >= 0:
        tables += ',configurations c, jobs j'
        condition += ' and j.configurationid=c.configurationid '

      if conddescription != default:
        retVal = self.__getConditionString( conddescription, 'pcont' )
        if retVal['OK']:
          condition += retVal['Value']
        else:
          return retVal

      if isinstance( quality, basestring ):
        command = "select QualityId from dataquality where dataqualityflag='%s'" % ( quality )
        res = self.dbR_.query( command )
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
            res = self.dbR_.query( command )
            if not res['OK']:
              gLogger.error( 'Data quality problem:', res['Message'] )
            elif len( res['Value'] ) == 0:
              return S_ERROR( 'Dataquality is missing!' )
            else:
              quality = res['Value'][0][0]
            conds += ' f.qualityid=' + str( quality ) + ' or'
          condition += 'and' + conds[:-3] + ')'

      if evt != default:
        condition += ' and f.eventtypeid=%d and j.jobid=f.jobid' % ( int( evt ) )
        if tables.find( 'jobs' ) < 0:
          tables += ',jobs j'

      if tables.find( 'files' ) > 0:
        condition += " and f.gotreplica='Yes'"

      if processing != default:
        command = "select distinct j.runnumber from \
                   productionscontainer pcont %s \
                   where pcont.processingid in (select v.id from \
                   (SELECT distinct SYS_CONNECT_BY_PATH(name, '/') Path, id ID \
                                             FROM processing v   START WITH id in \
                                             (select distinct id from processing where name='%s') \
                                                CONNECT BY NOCYCLE PRIOR  id=parentid) v \
                       where v.path='%s') and j.production=pcont.production\
                        %s" % ( tables, str( processing.split( '/' )[1] ), processing, condition )
      else:
        command = "select distinct j.runnumber from productionscontainer pcont %s where \
         pcont.production=j.production %s " % ( condition )
    return self.dbR_.query( command )

  #############################################################################
  def getSimulationConditions( self, in_dict ):
    """it returns the simulation conditions for a given condition"""
    condition = ''
    tables = " simulationconditions sim"
    paging = False
    start = in_dict.get( 'StartItem', default )
    maximum = in_dict.get( 'MaxItem', default )

    simid = in_dict.get( 'SimId', default )
    if simid != default:
      condition += ' and sim.simid=%d ' % int( simid )

    simdesc = in_dict.get( 'SimDescription', default )
    if simdesc != default:
      condition += " and sim.simdescription like '%" + simdesc + "%'"

    visible = in_dict.get( 'Visible', default )
    if visible != default:
      condition += " and sim.visible='%s'" % visible

    if start != default and maximum != default:
      paging = True

    sort = in_dict.get( 'Sort', default )
    if sort != default:
      condition += 'Order by '
      order = sort.get( 'Order', 'Asc' )
      if order.upper() not in ['ASC', 'DESC']:
        return S_ERROR( "wrong sorting order!" )
      items = sort.get( 'Items', default )
      if isinstance( items, list ):
        order = ''
        for item in items:
          order += 'sim.%s,' % ( item )
        condition += ' %s' % order[:-1]
      elif isinstance( items, basestring ):
        condition += ' sim.%s %s' % ( items, order )
      else:
        result = S_ERROR( 'SortItems is not properly defined!' )
    else:
      condition += ' order by sim.inserttimestamps desc'

    if paging:
      command = " select sim_simid, sim_simdescription, sim_beamcond, sim_beamenergy, sim_generator,\
      sim_magneticfield, sim_detectorcond, sim_luminosity, sim_g4settings, sim_visible from \
      ( select ROWNUM r , sim_simid, sim_simdescription, sim_beamcond, sim_beamenergy, sim_generator, \
      sim_magneticfield, sim_detectorcond, sim_luminosity, sim_g4settings, sim_visible from \
      ( select ROWNUM r, sim.simid sim_simid, sim.simdescription sim_simdescription, sim.beamcond\
      sim_beamcond, sim.beamenergy sim_beamenergy, sim.generator sim_generator, \
      sim.magneticfield sim_magneticfield, sim.detectorcond sim_detectorcond, sim.luminosity\
      sim_luminosity, sim.g4settings sim_g4settings, sim.visible sim_visible \
      from %s where sim.simid=sim.simid %s ) where rownum <=%d ) where r >%d" % ( tables, condition, maximum, start )
      retVal = self.dbR_.query( command )
    else:
      command = "select sim.simid sim_simid, sim.simdescription sim_simdescription, sim.beamcond sim_beamcond,\
      sim.beamenergy sim_beamenergy, sim.generator sim_generator, \
      sim.magneticfield sim_magneticfield, sim.detectorcond sim_detectorcond, sim.luminosity sim_luminosity,\
      sim.g4settings sim_g4settings, sim.visible sim_visible from %s where sim.simid=sim.simid %s" % ( tables, condition )
      retVal = self.dbR_.query( command )

    if not retVal['OK']:
      return retVal
    else:
      command = "select count(*) from simulationconditions"

      parameterNames = ['SimId',
                'SimDescription',
                'BeamCond',
                'BeamEnergy',
                'Generator',
                'MagneticField',
                'DetectorCond',
                'Luminosity',
                'G4settings',
                'Visible']
      records = [ list( record ) for record in retVal['Value']]
      retVal = self.dbR_.query( command )
      if not retVal['OK']:
        return retVal
      totalRecords = retVal['Value'][0][0]
      result = S_OK( {'ParameterNames':parameterNames, 'Records':records, 'TotalRecords':totalRecords} )

    return result

  #############################################################################
  def updateSimulationConditions( self, in_dict ):
    """it updates a given simulation condition"""
    result = None
    simid = in_dict.get( 'SimId', default )
    if simid != default:
      condition = ''
      for cond in in_dict:
        if cond != 'SimId':
          condition += "%s='%s'," % ( cond, in_dict[cond] )
      condition = condition[:-1]
      command = "update simulationconditions set %s where simid=%d" % ( condition, int( simid ) )
      result = self.dbW_.query( command )
    else:
      result = S_ERROR( 'SimId is missing!' )


    return result

  #############################################################################
  def deleteSimulationConditions( self, simid ):
    """it delete a given simulation condition"""
    command = "delete simulationconditions where simid=%d" % simid
    return self.dbW_.query( command )

  #############################################################################
  def getProductionSummaryFromView( self, in_dict ):
    """
    it returns a summary for a given condition.
    """
    evt = in_dict.get( 'EventType', default )
    prod = in_dict.get( 'Production', default )
    configName = in_dict.get( 'ConfigName', default )
    configVersion = in_dict.get( 'ConfigVersion', default )

    condition = ""
    if evt != default:
      condition += "and bview.eventtypeid=%d" % ( int( evt ) )

    if prod != default:
      condition += " and bview.production=%d" % ( int( prod ) )

    if configName != default:
      condition += " and bview.configname='%s'" % ( configName )

    if configVersion != default:
      condition += " and bview.configversion='%s'" % ( configVersion )

    command = "select bview.production, bview.eventtypeid, bview.configname, bview.configversion, \
                      BOOKKEEPINGORACLEDB.getProductionProcessingPass(prod.production),\
                      sim.simdescription, daq.description\
                 from prodview bview, simulationconditions sim, data_taking_conditions daq, productionscontainer prod \
                 where sim.simid(+)=bview.simid and \
                daq.daqperiodid(+)=bview.daqperiodid and \
                bview.production=prod.production %s" % ( condition )
    parameterNames = ['Production',
                      'EventType',
                      'ConfigName',
                      'ConfigVersion',
                      'ProcessingPass',
                      'ConditionDescription']
    retVal = self.dbR_.query( command )
    if not retVal['OK']:
      return retVal
    rows = []
    for record in retVal['Value']:
      i = dict( zip( parameterNames[:-1], record[:-2] ) )
      i['ConditionDescription'] = record[len( parameterNames )] if record[len( parameterNames )] != None \
                                                              else record[len( parameterNames ) - 1]
      rows += [i]
    return S_OK( rows )

  #############################################################################
  def getJobInputOutputFiles( self, diracjobids ):
    """it returns the input and output files for jobs by a given list of DIRAC jobid"""
    result = {'Failed':{}, 'Successful': {}}
    for diracJobid in diracjobids:
      command = "select j.jobid, f.filename from inputfiles i, files f, jobs j where f.fileid=i.fileid and \
      i.jobid=j.jobid and j.diracjobid=%d order by j.jobid, f.filename" % int( diracJobid )
      retVal = self.dbR_.query( command )
      if not retVal['OK']:
        result['Failed'][diracJobid] = retVal["Message"]
      result['Successful'][diracJobid] = {}
      result['Successful'][diracJobid]['InputFiles'] = []
      for i in retVal['Value']:
        result['Successful'][diracJobid]['InputFiles'] += [i[1]]

      command = "select j.jobid, f.filename  from jobs j, files f where j.jobid=f.jobid and \
      diracjobid=%d order by j.jobid, f.filename" % int( diracJobid )
      retVal = self.dbR_.query( command )
      if not retVal['OK']:
        result['Failed'][diracJobid] = retVal["Message"]
      result['Successful'][diracJobid]['OutputFiles'] = []
      for i in retVal['Value']:
        result['Successful'][diracJobid]['OutputFiles'] += [i[1]]
    return S_OK( result )

  #############################################################################
  def insertRunStatus( self, runnumber, jobId, isFinished = 'N' ):
    """inserts the run status of a give run"""
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.insertRunStatus', [runnumber, jobId, isFinished], False )
    return result
  
  #############################################################################
  def setRunStatusFinished( self, runnumber, isFinished ):
    result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.setRunFinished', [runnumber, isFinished], False )
    if not result['OK']:
      return result
    else:
      return S_OK( '%s is finished' % ( str( runnumber ) ) )

  #############################################################################
  def getRunStatus( self, runnumbers ):
    status = {}
    params = ['Finished']
    status['Successful'] = {} 
    status['Failed'] = []
    for i in runnumbers:
      command = "select Finished from runstatus where runnumber=%d" % i
      retVal = self.dbR_.query( command )
      if not retVal['OK']:
        gLogger.error( i, retVal['Message'] )
        status['Failed'] += [i]
      else:
        if len( retVal['Value'] ) > 0:
          status['Successful'][i] = dict( zip( params, retVal['Value'][0] ) )
        else:
          status['Failed'] += [i]
    return S_OK( status )
  
  #############################################################################
  def fixRunLuminosity( self, runnumbers ):
    status = { 'Failed' : [], 'Successful' : []}
    for run in runnumbers:
      result = self.dbW_.executeStoredProcedure( 'BOOKKEEPINGORACLEDB.updateLuminosity', [run], False )
      if result['OK']:
        status['Successful'] += [run]
      else:
        status['Failed'] += [run]
    return S_OK( status )
