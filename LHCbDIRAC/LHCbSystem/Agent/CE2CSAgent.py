########################################################################
# $HeadURL$
########################################################################

__RCSID__ = "$Id$"

""" Queries BDII for unknown CE.
    Queries BDII for CE information and put it to CS.
"""

from DIRAC                                                    import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Base.AgentModule                              import AgentModule
from DIRAC.Core.Utilities                                     import List
from DIRAC.Core.Utilities.Shifter                             import setupShifterProxyInEnv
from DIRAC.Core.Utilities.ldapsearchBDII                      import ldapSite, ldapCluster,ldapCE,ldapCEState
from DIRAC.FrameworkSystem.Client.NotificationClient          import NotificationClient
from DIRAC.ConfigurationSystem.Client.CSAPI                   import CSAPI

import sys, os

AGENT_NAME = "LHCb/CE2CSAgent"

class CE2CSAgent(AgentModule):

  def initialize( self ):

    self.logLevel = self.am_getOption('LogLevel','INFO')
    gLogger.info("LogLevel",self.logLevel)
    gLogger.setLevel(self.logLevel)
    self.pollingTime = self.am_getOption('PollingTime',120)
    gLogger.info("PollingTime %d hours" %(int(self.pollingTime)/3600))
    self.addressTo = self.am_getOption('MailTo','lhcb-SAM@cern.ch')
    gLogger.info("MailTo",self.addressTo)
    self.addressFrom =  self.am_getOption('MailFrom','lhcb-SAM@cern.ch')
    gLogger.info("MailFrom",self.addressFrom)
    self.subject = "CE2CSAgent"
    self.useProxies = self.am_getOption('UseProxies','True').lower() in ( "y", "yes", "true" )
    self.proxyLocation = self.am_getOption('ProxyLocation', '' )
    if not self.proxyLocation:
      self.proxyLocation = False

    if self.useProxies:
      self.am_setModuleParam( "shifterProxy", "SAMManager" )
      
    return S_OK()

  def execute(self):

    gLogger.info("Executing %s"%(self.name))
      
    os.system('dirac-proxy-info')
    
    self.csAPI = CSAPI()      

    self._lookForCE()
    self._infoFromCE()
    gLogger.info("Executing %s finished"%(self.name) )
    
    return S_OK()


  def _lookForCE(self):
  
    sites = gConfig.getSections('/Resources/Sites/LCG')['Value']

    bannedCEs = gConfig.getValue(self.section+'/BannedCEs','')
    if bannedCEs:
      knownces = List.fromChar( bannedCEs)
    else:
      knownces = []

    for site in sites:

      opt = gConfig.getOptionsDict('/Resources/Sites/LCG/%s'%site)['Value']
      name = opt.get('Name','')
      ces = List.fromChar( opt.get('CE',''))
      knownces += ces

    response = ldapCEState('')
    if not response['OK']:
      gLogger.error( "Error during BDII request",response['Message'])
      return responce

    newces = {}
    for queue in response['Value']:
      try:
        queuename = queue['GlueCEUniqueID']
      except:
        continue
    
      cename = queuename.split(":")[0]
      if not cename in knownces:
        newces[cename] = None
        gLogger.debug("newce",cename)

    body = ""
    
    for ce in newces.iterkeys():
      response = ldapCluster(ce)
      if not response['OK']:
        gLogger.warn( "Error during BDII request",response['Message'])
        continue
      clusters = response['Value']
      if len(clusters)!=1:
        gLogger.warn( "Error in cluster leng"," CE %s Leng %d"%(ce,len(clusters)))
      if len(clusters)==0:
        continue
      cluster = clusters[0]
      fkey = cluster.get('GlueForeignKey',[])
      if type(fkey) == type(''):
        fkey = [fkey]
      nameBDII = None
      for entry in fkey:
        if entry.count('GlueSiteUniqueID'):
          nameBDII = entry.split('=')[1]
          break
      if not nameBDII:
        continue
      
      cestring = "CE: %s, GOCDB Name: %s"%(ce,nameBDII) 
      gLogger.info(cestring)
  
      response = ldapCE(ce)
      if not response['OK']:
        gLogger.warn("Error during BDII request",response['Message'])
        continue

      ceinfos = response['Value']
      if len(ceinfos):
        ceinfo = ceinfos[0] 
        SystemName = ceinfo.get('GlueHostOperatingSystemName','Unknown')
        SystemVersion = ceinfo.get('GlueHostOperatingSystemVersion','Unknown')
        SystemRelease = ceinfo.get('GlueHostOperatingSystemRelease','Unknown')
      else:
        SystemName = "Unknown"
        SystemVersion = "Unknown"
        SystemRelease = "Unknown"


      osstring = "SystemName: %s, SystemVersion: %s, SystemRelease: %s"%(SystemName,SystemVersion,SystemRelease)
      gLogger.info(osstring)

      response = ldapCEState(ce)
      if not response['OK']:
        gLogger.warn("Error during BDII request",response['Message'])
        continue
       
      newcestring =  "\n\n%s\n%s"%(cestring, osstring)
      usefull = False
      cestates = response['Value']
      for cestate in cestates:
        queuename = cestate.get('GlueCEUniqueID','UnknownName')
        queuestatus = cestate.get('GlueCEStateStatus','UnknownStatus')
        
        queuestring = "%s %s"%(queuename,queuestatus)
        gLogger.info(queuestring)
        newcestring += "\n%s"%queuestring
        if queuestatus.count('Production'):
          usefull = True
      if usefull:
        body += newcestring
    if body:
      body = "We are glade to inform You about new CE(s) possibly suitable for LHCb:\n" + body
      body += "\n\nTo suppress information about CE add its name to %s/BannedCEs list."%self.section
      gLogger.info(body)
      notification = NotificationClient()
      result = notification.sendMail(self.addressTo,self.subject,body,self.addressFrom,localAttempt=False)

    return S_OK()

  def _infoFromCE(self):
  
    sites = gConfig.getSections('/Resources/Sites/LCG')['Value']

    changed = False
    body = ""

    for site in sites:
#      if site[-2:]!='ru':
#        continue
      opt = gConfig.getOptionsDict('/Resources/Sites/LCG/%s'%site)['Value']
      name = opt.get('Name','')
      if name:
        coor = opt.get('Coordinates','Unknown')
        mail = opt.get('Mail','Unknown')
    
        result = ldapSite(name)
        if not result['OK']:
          gLogger.warn("BDII site",result['Message'])
        else:
          bdiisites = result['Value']
          if len(bdiisites)==0:
            gLogger.warn(name, "Error in bdii: leng = 0")
          else:
            if not len(bdiisites)==1:
              gLogger.warn(name, "Warning in bdii: leng = %d"%len(bdiisites))
      
            bdiisite = bdiisites[0]

            try:
              longitude = bdiisite['GlueSiteLongitude']
              latitude = bdiisite['GlueSiteLatitude']
              newcoor = "%s:%s"%(longitude,latitude)
            except:
              gLogger.warn("Error in bdii coor")
              newcoor = "Unknown"

            try:
              newmail = bdiisite['GlueSiteSysAdminContact'].split(":")[-1].strip()
            except:
              gLogger.warn("Error in bdii mail")
              newmail = "Unknown"

            gLogger.debug( "%s %s %s"%(name, newcoor, newmail))

            if newcoor != coor:
              gLogger.info("%s"%(name),"%s -> %s"%(coor,newcoor))
              section = '/Resources/Sites/LCG/%s/Coordinates'%site
              if coor == 'Unknown':
                self.csAPI.setOption(section,newcoor)
              else:
                self.csAPI.modifyValue(section,newcoor)
              changed = True

            if newmail != mail:
              gLogger.info("%s"%(name),"%s -> %s"%(mail,newmail))
              section = '/Resources/Sites/LCG/%s/Mail'%site
              if mail == 'Unknown':
                self.csAPI.setOption(section,newmail)
              else:
                self.csAPI.modifyValue(section,newmail)
              changed = True
      
      celist = List.fromChar( opt.get('CE',''))

      if not celist:
        gLogger.warn(site,'Empty site list')
        continue
    
#      result = gConfig.getSections('/Resources/Sites/LCG/%s/CEs'%site)
#      if not result['OK']:
#        gLogger.debug("Section CEs:",result['Message'])
      
      for ce in celist:
        result = gConfig.getOptionsDict('/Resources/Sites/LCG/%s/CEs/%s'%(site,ce))
        if not result['OK']:
          gLogger.debug("Section CE",result['Message'])
          wnTmpDir = 'Unknown'
          arch = 'Unknown'
          os = 'Unknown'
          si00 = 'Unknown'
          pilot = 'Unknown'
        else:
          ceopt = result['Value']
          wnTmpDir = ceopt.get('wnTmpDir','Unknown')
          arch = ceopt.get('architecture','Unknown')
          os = ceopt.get('OS','Unknown')
          si00 = ceopt.get('SI00','Unknown')
          pilot = ceopt.get('Pilot','Unknown')

        result = ldapCE(ce)
        if not result['OK']:
          gLogger.warn('Error in bdii for %s'%ce, result['Message'])
          continue
        try:
          bdiice = result['Value'][0]
        except:
          gLogger.warn('Error in bdii for %s'%ce, result)
          bdiice = None
        if bdiice:
          try:
            newwnTmpDir = bdiice['GlueSubClusterWNTmpDir']
          except:
            newwnTmpDir = 'Unknown'
          if wnTmpDir != newwnTmpDir and newwnTmpDir != 'Unknown':
            section = '/Resources/Sites/LCG/%s/CEs/%s/wnTmpDir'%(site,ce)
            gLogger.info( section, " -> ".join((wnTmpDir,newwnTmpDir)))
            if wnTmpDir == 'Unknown':
              self.csAPI.setOption(section,newwnTmpDir)
            else:
              self.csAPI.modifyValue(section,newwnTmpDir)
            changed = True

          try:
            newarch = bdiice['GlueHostArchitecturePlatformType']
          except:
            newarch = 'Unknown'
          if arch != newarch and newarch != 'Unknown':
            section = '/Resources/Sites/LCG/%s/CEs/%s/architecture'%(site,ce)
            gLogger.info( section, " -> ".join((arch,newarch)))
            if arch == 'Unknown':
              self.csAPI.setOption(section,newarch)
            else:
              self.csAPI.modifyValue(section,newarch)
            changed = True

          try:
            newos = '_'.join((bdiice['GlueHostOperatingSystemName'],bdiice['GlueHostOperatingSystemVersion'],bdiice['GlueHostOperatingSystemRelease']))
          except:
            newos = 'Unknown'
          if os != newos and newos != 'Unknown':
            section = '/Resources/Sites/LCG/%s/CEs/%s/OS'%(site,ce)
            gLogger.info( section, " -> ".join((os,newos)))
            if os == 'Unknown':
              self.csAPI.setOption(section,newos)
            else:
              self.csAPI.modifyValue(section,newos)
            changed = True
            body = body + "OS was changed %s -> %s for %s at %s\n"%(os,newos,ce,site)

          try:
            newsi00 = bdiice['GlueHostBenchmarkSI00']
          except:
            newsi00 = 'Unknown'
          if si00 != newsi00 and newsi00 != 'Unknown':
            section = '/Resources/Sites/LCG/%s/CEs/%s/SI00'%(site,ce)
            gLogger.info( section, " -> ".join((newsi00,si00)))
            if si00 == 'Unknown':
              self.csAPI.setOption(section,newsi00)
            else:
              self.csAPI.modifyValue(section,newsi00)
            changed = True

          try:
            rte = bdiice['GlueHostApplicationSoftwareRunTimeEnvironment']
            if 'VO-lhcb-pilot' in rte:
              newpilot = 'True'
            else:
              newpilot = 'False'        
          except:
            newpilot = 'Unknown'
          if pilot != newpilot and newpilot != 'Unknown':
            section = '/Resources/Sites/LCG/%s/CEs/%s/Pilot'%(site,ce)
            gLogger.info( section, " -> ".join((pilot,newpilot)))
            if pilot == 'Unknown':
              self.csAPI.setOption(section,newpilot)
            else:
              self.csAPI.modifyValue(section,newpilot)
            changed = True

        result = ldapCEState(ce)        #getBDIICEVOView
        if not result['OK']:
          gLogger.warn( 'Error in bdii for queue %s'%ce, result['Message'])
          continue
        try:
          queues = result['Value']
        except:
          gLogger.warn('Error in bdii for queue %s'%ce, result['Massage'])
          continue
      
        queueSectionString = '/Resources/Sites/LCG/%s/CEs/%s/Queues'%(site,ce)
        for queue in queues:
          try:
            queueName = queue['GlueCEUniqueID'].split('/')[-1]
            newmaxCPUTime = queue['GlueCEPolicyMaxCPUTime']
          except:
            gLogger.warn('error in queue',queue)
            continue
        
          result = gConfig.getOptionsDict(queueSectionString+'/%s'%(queueName))
          if not result['OK']:
            gLogger.warn("Section Queues",result['Message'])
            maxCPUTime = 'Unknown'
          else:
            queueopt = result['Value']
            maxCPUTime = queueopt.get('maxCPUTime','Unknown')

          if maxCPUTime != newmaxCPUTime and newmaxCPUTime != 'Unknown':
            section = queueSectionString + '/%s/maxCPUTime'%(queueName)
            gLogger.info( section, " -> ".join((maxCPUTime,newmaxCPUTime)))
            if maxCPUTime == 'Unknown':
              self.csAPI.setOption(section,newmaxCPUTime)
            else:
              self.csAPI.modifyValue(section,newmaxCPUTime)
            changed = True

    if changed:
      gLogger.info(body)      
      if body:
        notification = NotificationClient()
        result = notification.sendMail(self.addressTo,self.subject,body,self.addressFrom,localAttempt=False)
        
      return self.csAPI.commitChanges(sortUsers=False)
    else:
      gLogger.info("No changes found")      
      return S_OK()
