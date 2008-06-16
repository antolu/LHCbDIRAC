# $Id: ProductionDB.py,v 1.34 2008/06/16 07:27:24 atsareg Exp $
"""
    DIRAC ProductionDB class is a front-end to the pepository database containing
    Workflow (templates) Productions and vectors to create jobs.

    The following methods are provided for public usage:

"""
__RCSID__ = "$Revision: 1.34 $"

import string
from DIRAC.Core.Base.DB import DB
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC  import gLogger, S_OK, S_ERROR
from DIRAC.Core.Transformation.TransformationDB import TransformationDB
from DIRAC.Core.Utilities import Time


class ProductionDB(TransformationDB):
#class ProductionDB(DB):

  def __init__( self, maxQueueSize=10 ):
    """ Constructor
    """

    TransformationDB.__init__(self,'ProductionDB', 'ProductionManagement/ProductionDB', maxQueueSize)
    #DB.__init__(self,'ProductionDB', 'ProductionManagement/ProductionDB', maxQueueSize)

################ WORKFLOW SECTION ####################################

  def publishWorkflow(self, wf_name, wf_parent, wf_descr, wf_descr_long, wf_body, authorDN, authorGroup, update=False):

    if not self._isWorkflowExists(wf_name):
      # workflow is not exists
      result = self._insert('Workflows', [ 'WFName', 'WFParent', 'Description', 'LongDescription','AuthorDN', 'AuthorGroup', 'Body' ],
                            [wf_name, wf_parent, wf_descr, wf_descr_long, authorDN, authorGroup, wf_body])
      if result['OK']:
        gLogger.verbose('Workflow "%s" published by DN="%s"' % (wf_name, authorDN))
      else:
        error = 'Workflow "%s" FAILED to be published by DN="%s" with message "%s"' % (wf_name, authorDN, result['Message'])
        gLogger.error(error)
        return S_ERROR( error )
    else: # workflow already exists
      if update: # we were asked to update

        # KGG tentative code !!!! have to check (seems working)
        result = self._escapeString( wf_body )
        if not result['OK']: return result
        wf_body_esc = result['Value'][1:len(result['Value'])-1] # we have to remove trailing " left by self._escapeString()
        result = self._escapeString( wf_descr_long )
        if not result['OK']: return result
        wf_descr_long_esc = result['Value'][1:len(result['Value'])-1] # we have to remove trailing " left by self._escapeString()
        # end of tentative code

        cmd = "UPDATE Workflows Set WFParent='%s', Description='%s', LongDescription='%s', AuthorDN='%s', AuthorGroup='%s', Body='%s' WHERE WFName='%s' " \
              % (wf_parent, wf_descr, wf_descr_long_esc, authorDN, authorGroup, wf_body_esc, wf_name)

        result = self._update( cmd )
        if result['OK']:
          gLogger.verbose( 'Workflow "%s" updated by DN="%s"' % (wf_name, authorDN) )
        else:
          error = 'Workflow "%s" FAILED on update by DN="%s"' % (wf_name, authorDN)
          gLogger.error( error )
          return S_ERROR( error )
      else: # update was not requested
        error = 'Workflow "%s" is exist in the repository, it was published by DN="%s"' % (wf_name, authorDN)
        gLogger.error( error )
        return S_ERROR( error )
    return result

  def getListWorkflows(self):
    cmd = "SELECT  WFName, WFParent, Description, LongDescription, AuthorDN, AuthorGroup, PublishingTime from Workflows;"
    result = self._query(cmd)
    if result['OK']:
      newres=[] # repacking
      for pr in result['Value']:
        newres.append({'WFName':pr[0], 'WFParent':pr[1], 'Description':pr[2], 'LongDescription':pr[3],
                       'AuthorDN':pr[4], 'AuthorGroup':pr[5], 'PublishingTime':pr[6].isoformat(' ')})
      return S_OK(newres)
    return result


  def getWorkflow(self, name):
    cmd = "SELECT Body from Workflows WHERE WFName='%s'" % name
    result = self._query(cmd)
    if result['OK']:
      return S_OK(result['Value'][0][0]) # we
    else:
      return S_ERROR("Failed to retrive Workflow with name '%s' " % name)

  def _isWorkflowExists(self, name):
    cmd = "SELECT COUNT(*) from Workflows WHERE WFName='%s'" % name
    result = self._query(cmd)
    if not result['OK']:
      gLogger.error("Failed to check if Workflow of name %s exists %s" % (name, result['Message']))
      return False
    elif result['Value'][0][0] > 0:
        return True
    return False

  def deleteWorkflow(self, name):
    if not self._isWorkflowExists(name):
      return S_ERROR("There is no Workflow with the name '%s' in the repository" % (name))

    cmd = "DELETE FROM Workflows WHERE WFName=\'%s\'" % (name)
    result = self._update(cmd)
    if not result['OK']:
      return S_ERROR("Failed to delete Workflow '%s' with the message %s" % (name, result['Message']))
    return result

  def getWorkflowInfo(self, name):
    cmd = "SELECT  WFName, WFParent, Description, LongDescription, AuthorDN, AuthorGroup, PublishingTime from Workflows WHERE WFname='%s'" % name
    result = self._query(cmd)
    if result['OK']:
      pr=result['Value'][0]

      newres = {'WFName':pr[0], 'WFParent':pr[1], 'Description':pr[2], 'LongDescription':pr[3],
                'AuthorDN':pr[4], 'AuthorGroup':pr[5], 'PublishingTime':pr[6].isoformat(' ')}
      return S_OK(newres)
    else:
      return S_ERROR('Failed to retrive Workflow with the name=%s' % (name, result['Message']) )


################ PRODUCTION SECTION ####################################


  def addProduction(self, name, parent, description, long_description, body, fileMask, groupsize, authorDN, authorGroup, update=False, inheritedFrom=0L):

    if fileMask == '' or fileMask == None: # if mask is empty it is simulation
      type_ = "SIMULATION"
    else:
      type_ = "PROCESSING"
    plugin = "NONE"
    agentType = "MANUAL"

    #status = "NEW" # alwais NEW when created
    # WE HAVE TO CHECK IS WORKFLOW EXISTS
    TransformationID = self.getTransformationID(name)
    if TransformationID > 0: # Transformation exists
      error = 'Production "%s" exists in the database"' % (name)
      gLogger.error( error )
      return S_ERROR( error )

    result = TransformationDB.addTransformation(self, name, description, long_description, authorDN, authorGroup, type_, plugin, agentType, fileMask)

    print result, 'This is the result of addTransformation'

    if not result['OK']:
      error = 'Transformation "%s" FAILED to be published by DN="%s" with message "%s"' % (name, authorDN, result['Message'])
      gLogger.error(error)
      return S_ERROR( error )

    TransformationID = result['Value']
    result = self.__insertProductionParameters(TransformationID, groupsize, parent, body, inheritedFrom)
    print result, 'This is the result of __insertProductionParameters'
    if not result['OK']:
      # if for some reason this failed we have to roll back
      result_rollback1 = TransformationDB.deleteTransformation(self, TransformationID)
      error = 'Transformation "%s" ID=$d FAILED to add ProductionsParameters with message "%s"' % (name, TransformationID, result['Message'])
      gLogger.error(error)
      return S_ERROR( error )

    result = self.__addJobTable(TransformationID)
    print result,'This is the result of __addJobTable'
    if not result['OK']:
      # if for some reason this failed we have to roll back
      result_rollback2 = self.__deleteProductionParameters(TransformationID)
      result_rollback1 = TransformationDB.deleteTransformation(self, TransformationID)
      error = 'Transformation "%s" ID=$d FAILED to add JobTable with message "%s"' % (name, TransformationID, result['Message'])
      gLogger.error(error)
      return S_ERROR( error )
    return S_OK(TransformationID)

    #elif update:
      # update
      #result = TransformationDB.modifyTransformation(self, name, description, long_description, authorDN, authorGroup, type_, plugin, agentType, fileMask)
      #if result['OK']:
        #result = self.__insertProductionParameters(TransformationID, groupsize, parent, body)
        #if result['OK']:
      #return result


  def addDerivedProduction(self, name, parent, description, long_description, body, fileMask, groupsize, authorDN, authorGroup, originaProdIDOrName):
    """ Create a new production derived from a previous one
    """

    TransformationID = self.getTransformationID(name)
    if TransformationID > 0: # Transformation exists
      error = 'Production "%s" exists in the database"' % (name)
      gLogger.error( error )
      return S_ERROR( error )

    originalProdID = self.getTransformationID(originaProdIDOrName)
    result = self.setTransformationStatus(originalProdID,'Stopped')
    if not result['OK']:
      return result
    message = 'Status changed to "Stopped" due to creation of the Derived Production'
    resultlog = self.updateTransformationLogging(originalProdID,message,authorDN) #ignoring result

    result = self.addProduction(name, parent, description, long_description, body, fileMask, groupsize, authorDN, authorGroup, False, originalProdID)
    if not result['OK']:
      return result

    newProdID = result['Value']

    # Mark the previously processed files
    ids = []
    req = "SELECT FileID from T_%s WHERE Status<>'Unused';" % (originalProdID)
    result = self._query(req)
    if result['OK']:
      if result['Value']:
        ids = [ str(x[0]) for x in result['Value'] ]
    if ids:
      idstring = ','.join(ids)
      req = "UPDATE T_%s SET Status='Inherited' WHERE FileID IN (%s)" % (newProdID,idstring)
      result = self._update(req)
      if not result['OK']:
        # rollback the operation
        print "KGG We need to add code to restore original production status"
        result = self.deleteProduction(newProdID)
        if not result['OK']:
          gLogger.warn('Failed to rollback the production creation')
        return S_ERROR('Failed to create derived production: error while marking already processed files')

    return S_OK(newProdID)

  def __insertProductionParameters(self, TransformationID, groupsize, parent, body, inheritedFrom=0):
    """
    Inserts additional parameters into ProductionParameters Table
    """
    inFields = ['TransformationID', 'GroupSize', 'Parent', 'Body', 'InheritedFrom']
    inValues = [TransformationID, groupsize, parent, body, inheritedFrom]
    result = self._insert('ProductionParameters',inFields,inValues)
    if not result['OK']:
      error = "Failed to add production parameters into ProductionParameters table for the TransformationID %s with message: %s" % (TransformationID, result['Message'])
      gLogger.error( error )
      return S_ERROR( error )
    return result

  def getProductionParameters(self, transName):
    """
    Get additional parameters from ProductionParameters Table
    """
    id_ = self.getTransformationID(transName)
    cmd = "SELECT GroupSize, Parent, Body, InheritedFrom  from ProductionParameters WHERE TransformationID='%d';" % id_
    result = self._query(cmd)
    retdict={}
    if not result['OK']:
      error = "Failed to get production parameters from ProductionParameters table for the Transformation %s with message: %s" % (transName, result['Message'])
      gLogger.error( error )
      return S_ERROR( error )

    if result['Value']:
      retdict['GroupSize']=result['Value'][0][0]
      retdict['Parent']=result['Value'][0][1]
      retdict['Body']=result['Value'][0][2]
      retdict['InheritedFrom']=result['Value'][0][3]
    else:
      retdict['GroupSize']=0
      retdict['Parent']=""
      retdict['Body']=""
      retdict['InheritedFrom']=0

    return S_OK(retdict)

  def getProductionParametersWithoutBody(self, transName):
    """
    Get additional parameters from ProductionParameters Table
    """
    id_ = self.getTransformationID(transName)
    cmd = "SELECT GroupSize, Parent, InheritedFrom  from ProductionParameters WHERE TransformationID='%d'" % id_
    result = self._query(cmd)
    retdict={}
    if not result['OK']:
      error = "Failed to get production parameters from ProductionParameters table for the Transformation %s with message: %s" % (transName, result['Message'])
      gLogger.error( error )
      return S_ERROR( error )

    if result['Value']:
      retdict['GroupSize']=result['Value'][0][0]
      retdict['Parent']=result['Value'][0][1]
      retdict['InheritedFrom']=result['Value'][0][2]
    else:
      retdict['GroupSize']=0
      retdict['Parent']=""
      retdict['InheritedFrom']=0


    return S_OK(retdict)

  def __deleteProductionParameters(self, TransformationID):
    """
    Removes additional parameters into ProductionParameters Table
    """
    req = "DELETE FROM ProductionParameters WHERE TransformationID='%s';" % TransformationID
    result = self._update(req)
    if not result['OK']:
      error = "Failed to remove production parameters from the ProductionParameters table for the TransformationID %s with message: %s" % (TransformationID, result['Message'])
      gLogger.error( error )
      return S_ERROR( error )
    return result

  def __addJobTable(self, TransformationID):
    """ Method to add Job table
    """

    req = """
CREATE TABLE Jobs_%s(
JobID INTEGER NOT NULL AUTO_INCREMENT,
WmsStatus char(16) DEFAULT 'Created',
JobWmsID char(16) DEFAULT '',
TargetSE char(32) DEFAULT 'Unknown',
CreationTime DATETIME NOT NULL,
LastUpdateTime DATETIME NOT NULL,
InputVector BLOB,
PRIMARY KEY(JobID),
INDEX(WmsStatus)
);
    """ % str(TransformationID)
    result = self._update(req)
    if not result['OK']:
      error = "Failed to add new Job table with the transformationID %s message: %s" % (str(TransformationID), result['Message'])
      gLogger.error( error )
      return S_ERROR( error )
    return result

  def __deleteJobTable(self, TransformationID):
    """ Method removes Job Table """
    req = "DROP TABLE IF EXISTS Jobs_%s;" % TransformationID
    result = self._update(req)
    if not result['OK']:
      error = "Failed to remove Job table for the transformationID %s message: %s" % (TransformationID, result['Message'])
      gLogger.error( error )
      return S_ERROR( error )
    return result

  def getProductionList(self):
    cmd = "SELECT  TransformationID, TransformationName, Description, LongDescription, CreationDate, AuthorDN, AuthorGroup, Type, Plugin, AgentType, Status, FileMask from Transformations;"
    result = self._query(cmd)
    if result['OK']:
      newres=[] # repacking
      for pr in result['Value']:
        newres.append({'TransformationID':pr[0], 'TransformationName':pr[1], 'Description':pr[2], 'LongDescription':pr[3], 'CreationDate':Time.toString(pr[4]),
                            'AuthorDN':pr[5], 'AuthorGroup':pr[6], 'Type':pr[7], 'Plugin':pr[8], 'AgentType':pr[9], 'Status':pr[10], 'FileMask':pr[11] })
      return S_OK(newres)
    return result

  def getAllProductions(self):
    result1 = TransformationDB.getAllTransformations(self)
    if not result1['OK']:
        return result1
    for prod in result1['Value']:
      id_ = long(prod['TransID'])
      result2 = self.getProductionParametersWithoutBody(id_)
      if result2['OK']:
        prod['GroupSize']=result2['Value']['GroupSize']
        prod['Parent']=result2['Value']['Parent']
        prod['InheritedFrom']=result2['Value']['InheritedFrom']
     # we can ignore errors for now
      #else:
      #  return result2
    return result1

  def deleteProduction(self, transName):
    """ Remove the Production specified by id
    """

    transID = self.getTransformationID(transName)
    if transID == 0:
      return S_ERROR("No Transformation with the name '%s' in the TransformationDB" % transName)
    result_step1 = TransformationDB.deleteTransformation(self, transID)
    if result_step1['OK']:
      # we have to execute all
      result_step2 = self.__deleteProductionParameters(transID)
      result_step3 = self.__deleteJobTable(transID)
      if not result_step2['OK']:
        return result_step2
      if not result_step3['OK']:
        return result_step3
      return S_OK()
    else:
      return  result_step1

  def getProductionBody(self, transName):
    transID = self.getTransformationID(transName)
    result = self.getProductionParameters(transID)
    if result['OK']:
      return S_OK(result['Value']['Body']) # we
    else:
      return S_ERROR("Failed to retrive Production Body with Transformation '%s' " % transName)


  def setProductionBody(self, id_, body, name, parent, description, descr_long):

    transID = self.getTransformationID(id_)
    transIDnew = self.getTransformationID(name)

    # KGG tentative code !!!! have to check (seems working)
    result = self._escapeString( body )
    if not result['OK']: return result
    body_esc = result['Value'][1:len(result['Value'])-1] # we have to remove trailing " left by self._escapeString()
    result = self._escapeString( descr_long )
    if not result['OK']: return result
    descr_long_esc = result['Value'][1:len(result['Value'])-1] # we have to remove trailing " left by self._escapeString()
    # end of tentative code

    if transIDnew >0 and transIDnew != transID: # checking if transformation exists
      error = 'Production "%s" exists in the database with the id=%s"' % (name, transIDnew)
      gLogger.error( error )
      return S_ERROR( error )

    req = "UPDATE ProductionParameters SET Body='%s', Parent='%s' WHERE TransformationID=%d" % (body_esc, parent, transID)
    result = self._update(req)
    if not result['OK']:
        return result

    req = "UPDATE Transformations SET TransformationName='%s', Description='%s' , LongDescription='%s' WHERE TransformationID=%d" % (name, description, descr_long_esc, transID)
    result = self._update(req)
    return result


  def getProductionInfo(self, transName):
    transID = self.getTransformationID(transName)
    prod = self.getTransformation(transID)
    result = self.getProductionParametersWithoutBody(transID)
    if result['OK']:
      prod['GroupSize']=result['Value']['GroupSize']
      prod['Parent']=result['Value']['Parent']
      prod['InheritedFrom']=result['Value']['InheritedFrom']
      return S_OK(prod)
    else:
      return S_ERROR('Failed to retrive Production=%s message=%s' % (transName, result['Message']))

  def getProduction(self, transName):
    transID = self.getTransformationID(transName)
    prod = self.getTransformation(transID)
    result = self.getProductionParameters(transID)
    if result['OK']:
      prod['Value']['GroupSize']=result['Value']['GroupSize']
      prod['Value']['Parent']=result['Value']['Parent']
      prod['Value']['Body']=result['Value']['Body']
      prod['Value']['InheritedFrom']=result['Value']['InheritedFrom']
      return prod
    else:
      return S_ERROR('Failed to retrive Production=%s message=%s' % (transName, result['Message']))


  def addProductionJob(self, transName, inputVector='', se='Unknown'):
      """ Add one job to Production
      """
      productionID = self.getTransformationID(transName)
      self.lock.acquire()
      req = "INSERT INTO Jobs_%s (WmsStatus, JobWmsID, TargetSE, CreationTime, LastUpdateTime, InputVector) VALUES\
      ('%s','%d','%s', UTC_TIMESTAMP(), UTC_TIMESTAMP(),'%s');" % (productionID,'Created', 0, se, inputVector)
      result = self._getConnection()
      if result['OK']:
        connection = result['Value']
      else:
        return S_ERROR('Failed to get connection to MySQL: '+result['Message'])
      result = self._update(req,connection)

      if not result['OK']:
        self.lock.release()
        error = 'Job FAILED to be published for Production="%s" with the input vector %s and with the message "%s"' % (productionID, inputVector, result['Message'])
        gLogger.error(error)
        return S_ERROR( error )

      result2 = self._query("SELECT LAST_INSERT_ID();", connection)
      self.lock.release()
      if not result2['OK']:
        return result2
      jobID = int(result2['Value'][0][0])
      gLogger.verbose('Job published for Production="%s" with the input vector "%s"' % (productionID, inputVector))
      return S_OK(jobID)
      
  def extendProduction(self, transName, nJobs):
    """ Extend SIMULATION type production by nJobs number of jobs
    """    
    result = self.getTransformation(transName)
    if not result['OK']:
      return result
      
    ttype = result['Value']['Type']
    if ttype.lower() != 'simulation':
      return S_ERROR('Can not extend non-SIMULATION type production')  
    
    jobIDs = []
    for job in range(nJobs):
      result = self.addProductionJob(transName)
      if result['OK']:
        jobIDs.append(result['Value'])
      else:
        return result
    return S_OK(jobIDs)      
    

######################## Job section ############################

  def selectWMSJobs(self,productionID,statusList = [],newer = 0):
    """ Select WMS job IDs for the given production having one of the specified
        statuses and optionally with last status update older than "newer" number
        of minutes
    """

    req = "SELECT JobID, JobWmsID, WmsStatus FROM Jobs_%d" % int(productionID)
    condList = []
    if statusList:
      statusString = ','.join(["'"+x+"'" for x in statusList])
      condList.append("WmsStatus IN (%s)" % statusString)
    if newer:
      condList.append("LastUpdateTime < DATE_SUB(UTC_TIMESTAMP(),INTERVAL %d MINUTE)" % newer)

    if condList:
      condString = " AND ".join(condList)
      req += " WHERE %s" % condString

    result = self._query(req)
    if not result['OK']:
      return result
    resultDict = {}
    if result['Value']:
      for row in result['Value']:
        if row[1] and int(row[1]) != 0:
          resultDict[int(row[1])] = (row[0],row[2])

    return S_OK(resultDict)

  def selectJobs(self,productionID,statusList = [],numJobs=1,site=''):
    """ Select jobs with the given status from the given production
    """

    req = "SELECT JobID,InputVector,TargetSE,WmsStatus FROM Jobs_%d" % int(productionID)
    if statusList:
      statusString = ','.join(["'"+x+"'" for x in statusList])
      req += " WHERE WmsStatus IN (%s)" % statusString
    if not site:
      # do not uncomment this  req += " AND TargetSE='%s'" % site
      req += " LIMIT %d" % numJobs

    result = self._query(req)
    if not result['OK']:
      return result

    # Prepare Site-SE resolution mapping
    site_se_mapping = {}
    mappingKeys = gConfig.getOptions('/Resources/SiteLocalSEMapping')
    for site_tmp in mappingKeys['Value']:
      seStr = gConfig.getValue('/Resources/SiteLocalSEMapping/%s' %(site_tmp))
      site_se_mapping[site_tmp] = [ x.strip() for x in string.split(seStr,',')]


    resultDict = {}
    if result['Value']:
      if not site:
        # We do not care about the destination site
        for row in result['Value']:
          se = row[2]
          targetSite = ''
          for s,sList in site_se_mapping.items():
            if se in sList:
              targetSite = s
          if targetSite:
            resultDict[int(row[0])] = {'InputData':row[1],'TargetSE':se,'Status':row[3],'Site':targetSite}
          else:
            gLogger.warn('Can not find corresponding site for se: '+se)
      else:
        # Get the jobs now

        for row in result['Value']:
          if len(resultDict) < numJobs:
            se = row[2]
            targetSite = ''
            for s,sList in site_se_mapping.items():
              if se in sList:
                targetSite = s
            if targetSite and targetSite == site:
              resultDict[int(row[0])] = {'InputData':row[1],'TargetSE':row[2],'Status':row[3],'Site':targetSite}
          else:
            break

    return S_OK(resultDict)

  def getJobStats(self,productionID):
    """ Returns dictionary with number of jobs per status in the given production. The status
        is one of the following:
        Created - this counter returns the total number of jobs already created;
        Submitted - jobs submitted to the WMS;
        Running;
        Done;
        Failed;
    """
    productionID = self.getTransformationID(productionID)
    req = "SELECT DISTINCT WmsStatus FROM Jobs_%s;" % productionID
    result1 = self._query(req)
    if not result1['OK']:
      return result1

    statusList = {}
    for s in ['Created','Submitted','Waiting','Running','Stalled','Done','Failed']:
      statusList[s] = 0

    total = 0;
    for stat in result1["Value"]:
      stList = ['Created']
      status = stat[0]
      req = "SELECT count(JobID) FROM Jobs_%d WHERE WmsStatus='%s';" % (productionID, status)
      result2 = self._query(req)
      if not result2['OK']:
        return result2
      count = int(result2['Value'][0][0])

      # Choose the status to report
      if not status == "Created" and not status == "Reserved":
        if not "Submitted" in stList:
          stList.append("Submitted")
      if status == "Waiting" or status == "Received" or status == "Checking" or \
         status == "Matched" or status == "Submitted":
        if not "Waiting" in stList:
          stList.append("Waiting")
      if status == "Running" or status == "Completed":
        if not "Running" in stList:
          stList.append("Running")
      if status == "Done" or status == "Failed" or status == "Stalled":
        stList.append(status)

      for st in stList:
        statusList[st] += count

    return S_OK(statusList)

  def getJobWmsStats(self,productionID):
    """ Returns dictionary with number of jobs per status in the given production.
    """

    req = "SELECT DISTINCT WmsStatus FROM Jobs_%d;" % int(productionID)
    result1 = self._query(req)
    if not result1['OK']:
      return result1

    statusList = {}
    for status_ in result1["Value"]:
      status = status_[0]
      statusList[status]=0
      req = "SELECT count(JobID) FROM Jobs_%d WHERE WmsStatus='%s';" % (productionID, status)
      result2 = self._query(req)
      if not result2['OK']:
        return result2
      statusList[status]=result2['Value'][0][0]
    return S_OK(statusList)

  def setJobStatus(self,productionID,jobID,status):
    """ Set status for job with jobID in production with productionID
    """

    req = "UPDATE Jobs_%d SET WmsStatus='%s', LastUpdateTime=UTC_TIMESTAMP() WHERE JobID=%d;" % (productionID,status,jobID)
    result = self._update(req)
    return result

  def setJobWmsID(self,productionID,jobID,jobWmsID):
    """ Set WmsID for job with jobID in production with productionID
    """

    req = "UPDATE Jobs_%d SET JobWmsID='%s', LastUpdateTime=UTC_TIMESTAMP() WHERE JobID=%d;" % (productionID,jobWmsID,jobID)
    result = self._update(req)
    return result

  def setJobStatusAndWmsID(self,productionID,jobID,status,jobWmsID):
    """ Set status and JobWmsID for job with jobID in production with productionID
    """

    req = "UPDATE Jobs_%d SET WmsStatus='%s', JobWmsID='%s', LastUpdateTime=UTC_TIMESTAMP() WHERE JobID=%d;" % (productionID,status,jobWmsID,jobID)
    result = self._update(req)
    return result

  def setJobTagetSE(self,productionID,jobID,se):
    """ Set TargetSE for job with jobID in production with productionID
    """

    req = "UPDATE Jobs_%d SET TargetSE='%s', LastUpdateTime=UTC_TIMESTAMP() WHERE JobID=%d;" % (productionID,se,jobID)
    result = self._update(req)
    return result

  def setJobInputVector(self,productionID,jobID,inputVector):
    """ Set TargetSE for job with jobID in production with productionID
    """

    req = "UPDATE Jobs_%d SET InputVector='%s', LastUpdateTime=UTC_TIMESTAMP() WHERE JobID=%d;" % (productionID,inputVector,jobID)
    result = self._update(req)
    return result

  def deleteJob(self,productionID,jobID):
    """ DeleteJob with jobID in production with productionID
    """
    productionID = self.getTransformationID(productionID)
    req = "DELETE FROM Jobs_%d WHERE JobID=%d;" % (productionID,jobID)
    result = self._update(req)
    return result

  def deleteJobs(self,productionID,jobIDbottom, jobIDtop):
    """ DeleteJob with jobID in production with productionID
    """
    productionID = self.getTransformationID(productionID)
    req = "DELETE FROM Jobs_%d WHERE JobID>=%d and JobID<=%d;" % (productionID,jobIDbottom, jobIDtop)
    result = self._update(req)
    return result

  def getJobInfo(self,productionID,jobID):
    """ returns dictionary with information for given Job of given productionID
    """
    productionID = self.getTransformationID(productionID)
    req = "SELECT JobID,JobWmsID,WmsStatus,TargetSE,CreationTime,LastUpdateTime,InputVector FROM Jobs_%d WHERE JobID='%d';" % (productionID,jobID)
    result = self._query(req)
    # lets create dictionary
    jobDict = {}
    if result['OK'] and result['Value']:
      jobDict['JobID']=result['Value'][0][0]
      jobDict['JobWmsID']=result['Value'][0][1]
      jobDict['WmsStatus']=result['Value'][0][2]
      jobDict['TargetSE']=result['Value'][0][3]
      jobDict['CreationTime']=result['Value'][0][4]
      jobDict['LastUpdateTime']=result['Value'][0][5]
      jobDict['InputVector']=result['Value'][0][6]
      return S_OK(jobDict)
    else:
      if result['OK']:
        result['Message']='' #used for printing error message
      error = "Failed to find job with JobID=%s in the Jobs_%s table with message: %s" % (jobID, productionID, result['Message'])
      gLogger.error( error )
      return S_ERROR( error )
