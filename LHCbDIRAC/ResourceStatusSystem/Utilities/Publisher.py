# $HeadURL:  $
''' Publisher

  Module not used. Will be back to life whenever the portal is ready.

'''

import copy
import threading

from DIRAC                                                  import gLogger
from DIRAC.Core.DISET.RPCClient                             import RPCClient
from DIRAC.Core.Utilities.ThreadPool                        import ThreadPool
from DIRAC.Core.Utilities.SitesDIRACGOCDBmapping            import getGOCSiteName
from DIRAC.ResourceStatusSystem.Client.ResourceStatusClient import ResourceStatusClient
from DIRAC.ResourceStatusSystem.Command.CommandCaller       import CommandCaller
from DIRAC.ResourceStatusSystem.Utilities                   import RssConfiguration, Utils 
from DIRAC.ResourceStatusSystem.Utilities.InfoGetter        import InfoGetter

from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

__RCSID__ = '$Id: $'

class Publisher:
  """
  Class Publisher is in charge of getting dispersed information, to be published on the web.
  """

  def __init__( self ):
    """
    Standard constructor

    :params:
      :attr:`VOExtension`: string, VO Extension (e.g. 'LHCb')

      :attr:`rsDBIn`: optional ResourceStatusDB object
      (see :class: `DIRAC.ResourceStatusSystem.DB.ResourceStatusDB.ResourceStatusDB`)

      :attr:`commandCallerIn`: optional CommandCaller object
      (see :class: `DIRAC.ResourceStatusSystem.Command.CommandCaller.CommandCaller`)

      :attr:`infoGetterIn`: optional InfoGetter object
      (see :class: `DIRAC.ResourceStatusSystem.Utilities.InfoGetter.InfoGetter`)

      :attr:`WMSAdminIn`: optional RPCClient object for WMSAdmin
      (see :class: `DIRAC.Core.DISET.RPCClient.RPCClient`)
    """

    # Clients
    self.rsClient = ResourceStatusClient() 
    self.rmClient = ResourceManagementClient()

    # RPC servers
    self.wmsAdmin = RPCClient( 'WorkloadManagement/WMSAdministrator' )

    # Others
    self.configModule  = Utils.voimport( "DIRAC.ResourceStatusSystem.Policy.Configurations" )
    self.infoGetter    = InfoGetter()    
    self.commandCaller = CommandCaller()

    # Thread pool
    self.threadPool       = ThreadPool( 2, 5 )
    self.lockObj          = threading.RLock()
    self.infoForPanel_res = {}

################################################################################

  def getInfo( self, granularity, name, useNewRes = False ):
    """
    Standard method to get all the info to be published

    This method uses a ThreadPool (:class:`DIRAC.Core.Utilities.ThreadPool.ThreadPool`)
    with 2-5 threads. The threaded method is
    :meth:`DIRAC.ResourceStatusSystem.Utilities.Publisher.Publisher.getInfoForPanel`

    :params:
      :attr:`granularity`: string - a ValidRes

      :attr:`name`: string - name of the Validres

      :attr:`useNewRes`: boolean. When set to true, will get new results,
      otherwise it will get cached results (where available).
    """

    if granularity not in RssConfiguration.getValidElements():
      return {}

    self.infoForPanel_res = {}

    status       = None
    formerStatus = None
    siteType     = None
    serviceType  = None
    resourceType = None

    if granularity in ( 'Resource', 'Resources' ):
      try:
      
        resourceType = self.getMonitoredsList( 'Resource', ['ResourceType'],
                                                        resourceName = name)[0][0]
      
      except IndexError:
        return "%s does not exist!" %name

    if granularity in ('StorageElement', 'StorageElements'):
      try:
        siteType = self.getMonitoredsList( 'StorageElement', ['SiteType'],
                                                    storageElementName = name)[0][0]
      except IndexError:
        return "%s does not exist!" %name

    paramNames = ['Type', 'Group', 'Name', 'Policy', 'DIRAC Status',
                  'RSS Status', 'Reason', 'Description']

    infoToGet = self.infoGetter.getInfoToApply(('view_info', ), granularity, status = status,
                                               formerStatus = formerStatus, siteType = siteType,
                                               serviceType = serviceType, resourceType = resourceType,
                                               useNewRes = useNewRes)[0]['Panels']
    infoToGet_res = {}

    recordsList = []

    infosForPolicy = {}

    for panel in infoToGet.keys():

      (granularityForPanel, nameForPanel) = self.__getNameForPanel(granularity, name, panel)

      if not self._resExist(granularityForPanel, nameForPanel):
#        completeInfoForPanel_res = None
        continue

      #take composite RSS result for name
      nameStatus_res = self._getStatus(nameForPanel, panel)

      recordBase = [None, None, None, None, None, None, None, None]

      recordBase[1] = panel.replace('_Panel', '')
      recordBase[2] = nameForPanel #nameForPanel
      try:
        recordBase[4] = nameStatus_res[nameForPanel]['DIRACStatus'] #DIRAC Status
      except:
        pass
      recordBase[5] = nameStatus_res[nameForPanel]['RSSStatus'] #RSS Status

      record = copy.deepcopy(recordBase)
      record[0] = 'ResultsForResource'

      recordsList.append(record)

      #take info that goes into the panel
      infoForPanel = infoToGet[panel]

      for info in infoForPanel:

        self.threadPool.generateJobAndQueueIt(self.getInfoForPanel,
                                              args = (info, granularityForPanel, nameForPanel) )

      self.threadPool.processAllResults()

      for policy in [x.keys()[0] for x in infoForPanel]:
        record = copy.deepcopy(recordBase)
        record[0] = 'SpecificInformation'
        record[3] = policy #policyName
        record[4] = None #DIRAC Status
        record[5] = self.infoForPanel_res[policy]['Status'] #RSS status for the policy
        record[6] = self.infoForPanel_res[policy]['Reason'] #Reason
        record[7] = self.infoForPanel_res[policy]['desc'] #Description
        recordsList.append(record)

        infosForPolicy[policy] = self.infoForPanel_res[policy]['infos']

    infoToGet_res['TotalRecords'] = len(recordsList)
    infoToGet_res['ParameterNames'] = paramNames
    infoToGet_res['Records'] = recordsList

    infoToGet_res['Extras'] = infosForPolicy

    return infoToGet_res

################################################################################

  def getInfoForPanel( self, info, granularityForPanel, nameForPanel ):

    #get single RSS policy results
    policyResToGet = info.keys()[0]
    #FIXME: rename...
    pol_res = self.rmClient.selectPolicyResult( nameForPanel, policyResToGet )
    if pol_res != []:
      pol_res_dict = { 'Status' : pol_res[0], 'Reason' : pol_res[1] }
    else:
      pol_res_dict = { 'Status' : 'Unknown', 'Reason' : 'Unknown' }
    self.lockObj.acquire()
    try:
      self.infoForPanel_res[policyResToGet] = pol_res_dict
    finally:
      self.lockObj.release()

    #get policy description
    desc = self._getPolicyDesc(policyResToGet)

    #get other info
    othersInfo = info.values()[0]
    if not isinstance(othersInfo, list):
      othersInfo = [othersInfo]

    info_res = {}

    for oi in othersInfo:
      format_ = oi.keys()[0]
      what = oi.values()[0]

      info_bit_got = self._getInfo(granularityForPanel, nameForPanel, format_, what)

      info_res[format_] = info_bit_got

    self.lockObj.acquire()
    try:
      self.infoForPanel_res[policyResToGet]['infos'] = info_res
      self.infoForPanel_res[policyResToGet]['desc'] = desc
    finally:
      self.lockObj.release()

################################################################################

  def _getStatus( self, name, panel ):

    #get RSS status
    RSSStatus = self._getInfoFromRSSDB(name, panel)[0][1]

    #get DIRAC status
    if panel in ('Site_Panel', 'SE_Panel'):

      if panel == 'Site_Panel':
        DIRACStatus = self.wmsAdmin.getSiteMaskLogging( name )
        if DIRACStatus['OK']:
          DIRACStatus = DIRACStatus['Value'][name].pop()[0]
        else:
          gLogger.error( DIRACStatus[ 'Message' ] )
          return None

      elif panel == 'SE_Panel':
        ra = self.rsClient.selectStatusElement( 'Resource', 'Status', name, 'ReadAccess' )[ 'Value' ]
        wa = self.rsClient.selectStatusElement( 'Resource', 'Status', name, 'WriteAccess' )[ 'Value' ]        
        DIRACStatus = { 'ReadAccess': ra, 'WriteAccess': wa }

      status = { name : { 'RSSStatus': RSSStatus, 'DIRACStatus': DIRACStatus } }

    else:
      status = { name : { 'RSSStatus': RSSStatus} }


    return status

################################################################################

  def _getInfo( self, granularity, name, format_, what ):

    if format_ == 'RSS':
      info_bit_got = self._getInfoFromRSSDB(name, what)
    else:
      if isinstance(what, dict):
        command = what['CommandIn']
        extraArgs = what['args']
      else:
        command = what
        extraArgs = None

      info_bit_got = self.commandCaller.commandInvocation( granularity, name, None,
                                                           None, command, extraArgs )

      try:
        info_bit_got = info_bit_got['Result']
      except:
        pass

    return info_bit_got

################################################################################

  def _getInfoFromRSSDB( self, name, what ):

    paramsL = ['Status']

    siteName = None
    serviceName = None
    resourceName = None
    storageElementName = None
    serviceType = None
    gridSiteName = None

    if what == 'ServiceOfSite':
      gran = 'Service'
      paramsL.insert(0, 'ServiceName')
      paramsL.append('Reason')
      siteName = name
    elif what == 'ResOfCompService':
      gran = 'Resources'
      paramsL.insert(0, 'ResourceName')
      paramsL.append('Reason')
      serviceType = name.split('@')[0]
      gridSiteName = getGOCSiteName(name.split('@')[1])
      if not gridSiteName['OK']:
        gLogger.error( gridSiteName['Message'] )
        return None
      gridSiteName = gridSiteName['Value']
    elif what == 'ResOfStorService':
      gran = 'Resources'
      paramsL.insert(0, 'ResourceName')
      paramsL.append('Reason')
      serviceType = name.split('@')[0]
      gridSiteName = getGOCSiteName(name.split('@')[1])
      if not gridSiteName['OK']:
        gLogger.error( gridSiteName['Message'] )
        return None
      gridSiteName = gridSiteName['Value']
    elif what == 'ResOfStorEl':
      gran = 'StorageElements'
      paramsL.insert(0, 'ResourceName')
      paramsL.append('Reason')
      storageElementName = name
    elif what == 'StorageElementsOfSite':
      gran = 'StorageElements'
      paramsL.insert(0, 'StorageElementName')
      paramsL.append('Reason')
      if '@' in name:
        DIRACsiteName = name.split('@').pop()
      else:
        DIRACsiteName = name
      gridSiteName = getGOCSiteName(DIRACsiteName)
      if not gridSiteName['OK']:
        gLogger.error( gridSiteName['Message'] )
        return None
      gridSiteName = gridSiteName['Value']
    elif what == 'Site_Panel':
      gran = 'Site'
      paramsL.insert(0, 'SiteName')
      siteName = name
    elif what == 'Service_Computing_Panel':
      gran = 'Service'
      paramsL.insert(0, 'ServiceName')
      serviceName = name
    elif what == 'Service_Storage_Panel':
      gran = 'Service'
      paramsL.insert(0, 'ServiceName')
      serviceName = name
    elif what == 'Service_VO-BOX_Panel':
      gran = 'Services'
      paramsL.insert(0, 'ServiceName')
      serviceName = name
    elif what == 'Service_VOMS_Panel':
      gran = 'Services'
      paramsL.insert(0, 'ServiceName')
      serviceName = name
    elif what == 'Resource_Panel':
      gran = 'Resource'
      paramsL.insert(0, 'ResourceName')
      resourceName = name
    elif what == 'SE_Panel':
      gran = 'StorageElement'
      paramsL.insert(0, 'StorageElementName')
      storageElementName = name

    info_bit_got = self.rsClient.getMonitoredsList(gran, paramsList = paramsL, siteName = siteName,
                                                   serviceName = serviceName, serviceType = serviceType,
                                                   resourceName = resourceName,
                                                   storageElementName = storageElementName,
                                                   gridSiteName = gridSiteName)

    return info_bit_got

################################################################################

  def _getPolicyDesc( self, policyName ):

    return self.configModule.Policies[policyName]['Description']

################################################################################

  def __getNameForPanel( self, granularity, name, panel ):

    if granularity in ('Site', 'Sites'):
      if panel == 'Service_Computing_Panel':
        granularity = 'Service'
        name = 'Computing@' + name
      elif panel == 'Service_Storage_Panel':
        granularity = 'Service'
        name = 'Storage@' + name
      elif panel == 'OtherServices_Panel':
        granularity = 'Service'
        name = 'OtherS@' + name
      elif panel == 'Service_VOMS_Panel':
        granularity = 'Service'
        name = 'VOMS@' + name
      elif panel == 'Service_VO-BOX_Panel':
        granularity = 'Service'
        name = 'VO-BOX@' + name
#      else:
#        granularity = granularity
#        name = name
#    else:
#      granularity = granularity
#      name = name

    return (granularity, name)

################################################################################

  def _resExist( self, granularity, name ):

    siteName = None
    serviceName = None
    resourceName = None
    storageElementName = None

    if granularity in ('Site', 'Sites'):
      siteName = name
    elif granularity in ('Service', 'Services'):
      serviceName = name
    elif granularity in ('Resource', 'Resources'):
      resourceName = name
    elif granularity in ('StorageElement', 'StorageElements'):
      storageElementName = name

    res = self.rsClient.getMonitoredsList( granularity, siteName = siteName,
                                           serviceName = serviceName, resourceName = resourceName,
                                           storageElementName = storageElementName)

    if res == []:
      return False
    else:
      return True

################################################################################
################################################################################
################################################################################

# FUNCTIONS RESCUED FROM OLD CODE, Still used here, but scratching them and
# writting new ones sounds like a brilliant idea.

  def getMonitoredsList( self, granularity, paramsList = None, siteName = None,
                         serviceName = None, resourceName = None, storageElementName = None,
                         status = None, siteType = None, resourceType = None,
                         serviceType = None, countries = None, gridSiteName = None):
    """
    Get Present Sites/Services/Resources/StorageElements lists.

    :params:
      :attr:`granularity`: a ValidRes

      :attr:`paramsList`: a list of parameters can be entered. If not given,
      a custom list is used.

      :attr:`siteName`, `serviceName`, `resourceName`, `storageElementName`:
      a string or a list representing the site/service/resource/storageElement name.
      If not given, fetch all.

      :attr:`status`: a string or a list representing the status. If not given, fetch all.

      :attr:`siteType`: a string or a list representing the site type.
      If not given, fetch all.

      :attr:`serviceType`: a string or a list representing the service type.
      If not given, fetch all.

      :attr:`resourceType`: a string or a list representing the resource type.
      If not given, fetch all.

      :attr:`countries`: a string or a list representing the countries extensions.
      If not given, fetch all.

      :attr:`gridSiteName`: a string or a list representing the grid site name.
      If not given, fetch all.

      See :mod:`DIRAC.ResourceStatusSystem.Utilities.Utils` for these parameters.

    :return:
      list of monitored paramsList's values
    """

    #get the parameters of the query

    getInfo = []

    if granularity in ('Site', 'Sites'):
      DBname = 'SiteName'
      DBtable = 'PresentSites'
      getInfo = getInfo + ['SiteName', 'SiteType', 'GridSiteName']
    elif granularity in ('Service', 'Services'):
      DBname = 'ServiceName'
      DBtable = 'PresentServices'
      getInfo = getInfo + ['SiteName', 'SiteType', 'ServiceName', 'ServiceType']
    elif granularity in ('Resource', 'Resources'):
      DBname = 'ResourceName'
      DBtable = 'PresentResources'
      getInfo = getInfo + ['SiteType', 'ResourceName', 'ResourceType', 'ServiceType', 'GridSiteName']
    elif granularity in ('StorageElement', 'StorageElements'):
      DBname = 'StorageElementName'
      DBtable = 'PresentStorageElements'
      getInfo = getInfo + ['StorageElementName', 'GridSiteName']
    else:
      raise InvalidRes, where(self, self.getMonitoredsList)

    #paramsList
    if (paramsList == None or paramsList == []):
      params = DBname + ', Status, FormerStatus, DateEffective, LastCheckTime '
    else:
      if type(paramsList) is not type([]):
        paramsList = [paramsList]
      params = ','.join([x.strip()+' ' for x in paramsList])

    #siteName
    if 'SiteName' in getInfo:
      if (siteName == None or siteName == []):
        r = "SELECT SiteName FROM PresentSites"
        resQuery = self.db._query(r)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.getMonitoredsList)+resQuery['Message']
        if not resQuery['Value']:
          siteName = []
        siteName = [ x[0] for x in resQuery['Value']]
        siteName = ','.join(['"'+x.strip()+'"' for x in siteName])
      else:
        if type(siteName) is not type([]):
          siteName = [siteName]
        siteName = ','.join(['"'+x.strip()+'"' for x in siteName])

    #gridSiteName
    if 'GridSiteName' in getInfo:
      if (gridSiteName == None or gridSiteName == []):
        r = "SELECT GridSiteName FROM GridSites"
        resQuery = self.db._query(r)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.getMonitoredsList)+resQuery['Message']
        if not resQuery['Value']:
          gridSiteName = []
        gridSiteName = [ x[0] for x in resQuery['Value']]
        gridSiteName = ','.join(['"'+x.strip()+'"' for x in gridSiteName])
      else:
        if type(gridSiteName) is not type([]):
          gridSiteName = [gridSiteName]
        gridSiteName = ','.join(['"'+x.strip()+'"' for x in gridSiteName])

    #serviceName
    if 'ServiceName' in getInfo:
      if (serviceName == None or serviceName == []):
        r = "SELECT ServiceName FROM PresentServices"
        resQuery = self.db._query(r)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.getMonitoredsList)+resQuery['Message']
        if not resQuery['Value']:
          serviceName = []
        serviceName = [ x[0] for x in resQuery['Value']]
        serviceName = ','.join(['"'+x.strip()+'"' for x in serviceName])
      else:
        if type(serviceName) is not type([]):
          serviceName = [serviceName]
        serviceName = ','.join(['"'+x.strip()+'"' for x in serviceName])

    #resourceName
    if 'ResourceName' in getInfo:
      if (resourceName == None or resourceName == []):
        r = "SELECT ResourceName FROM PresentResources"
        resQuery = self.db._query(r)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.getMonitoredsList)+resQuery['Message']
        if not resQuery['Value']:
          resourceName = []
        resourceName = [ x[0] for x in resQuery['Value']]
        resourceName = ','.join(['"'+x.strip()+'"' for x in resourceName])
      else:
        if type(resourceName) is not type([]):
          resourceName = [resourceName]
        resourceName = ','.join(['"'+x.strip()+'"' for x in resourceName])

    #storageElementName
    if 'StorageElementName' in getInfo:
      if (storageElementName == None or storageElementName == []):
        r = "SELECT StorageElementName FROM PresentStorageElements"
        resQuery = self.db._query(r)
        if not resQuery['OK']:
          raise RSSDBException, where(self, self.getMonitoredsList)+resQuery['Message']
        if not resQuery['Value']:
          storageElementName = []
        storageElementName = [ x[0] for x in resQuery['Value']]
        storageElementName = ','.join(['"'+x.strip()+'"' for x in storageElementName])
      else:
        if type(storageElementName) is not type([]):
          storageElementName = [storageElementName]
        storageElementName = ','.join(['"'+x.strip()+'"' for x in storageElementName])

    #status
    if (status == None or status == []):
      status = ValidStatus
    else:
      if type(status) is not type([]):
        status = [status]
    status = ','.join(['"'+x.strip()+'"' for x in status])

    #siteType
    if 'SiteType' in getInfo:
      if (siteType == None or siteType == []):
        siteType = ValidSiteType
      else:
        if type(siteType) is not type([]):
          siteType = [siteType]
      siteType = ','.join(['"'+x.strip()+'"' for x in siteType])

    #serviceType
    if 'ServiceType' in getInfo:
      if (serviceType == None or serviceType == []):
        serviceType = ValidServiceType
      else:
        if type(serviceType) is not type([]):
          serviceType = [serviceType]
      serviceType = ','.join(['"'+x.strip()+'"' for x in serviceType])

    #resourceType
    if 'ResourceType' in getInfo:
      if (resourceType == None or resourceType == []):
        resourceType = ValidResourceType
      else:
        if type(resourceType) is not type([]):
          resourceType = [resourceType]
      resourceType = ','.join(['"'+x.strip()+'"' for x in resourceType])

    #countries
    if (countries == None or countries == []):
      countries = self.getCountries(granularity)
    else:
      if type(countries) is not type([]):
        countries = [countries]
    if countries == None:
      countries = " '%%'"
    else:
      str = ' OR %s LIKE ' %DBname
      countries = str.join(['"%.'+x.strip()+'"' for x in countries])


    #storageElementType
#    if 'StorageElementType' in getInfo:
#      if (storageElementType == None or storageElementType == []):
#        storageElementType = ValidStorageElementType
#      else:
#        if type(storageElementType) is not type([]):
#          storageElementType = [storageElementType]
#      storageElementType = ','.join(['"'+x.strip()+'"' for x in storageElementType])


    #query construction
    #base
    req = "SELECT %s FROM %s WHERE" %(params, DBtable)
    #what "names"
    if 'SiteName' in getInfo:
      if siteName != [] and siteName != None and siteName is not None and siteName != '':
        req = req + " SiteName IN (%s) AND" %(siteName)
    if 'GridSiteName' in getInfo:
      if gridSiteName != [] and gridSiteName != None and gridSiteName is not None and gridSiteName != '':
        req = req + " GridSiteName IN (%s) AND" %(gridSiteName)
    if 'ServiceName' in getInfo:
      if serviceName != [] and serviceName != None and serviceName is not None and serviceName != '':
        req = req + " ServiceName IN (%s) AND" %(serviceName)
    if 'ResourceName' in getInfo:
      if resourceName != [] and resourceName != None and resourceName is not None and resourceName != '':
        req = req + " ResourceName IN (%s) AND" %(resourceName)
    if 'StorageElementName' in getInfo:
      if storageElementName != [] and storageElementName != None and storageElementName is not None and storageElementName != '':
        req = req + " StorageElementName IN (%s) AND" %(storageElementName)
    #status
    req = req + " Status IN (%s)" % (status)
    #types
    if 'SiteType' in getInfo:
      req = req + " AND SiteType IN (%s)" % (siteType)
    if 'ServiceType' in getInfo:
      req = req + " AND ServiceType IN (%s)" % (serviceType)
    if 'ResourceType' in getInfo:
      req = req + " AND ResourceType IN (%s)" % (resourceType)
#    if 'StorageElementType' in getInfo:
#      req = req + " WHERE StorageElementName LIKE \'%" + "%s\'" %(storageElementType)
    if granularity not in ('StorageElement', 'StorageElements'):
      req = req + " AND (%s LIKE %s)" % (DBname, countries)

    resQuery = self.db._query(req)
    if not resQuery['OK']:
      raise RSSDBException, where(self, self.getMonitoredsList)+resQuery['Message']
    if not resQuery['Value']:
      return []
    list = []
    list = [ x for x in resQuery['Value']]
    return list


################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF