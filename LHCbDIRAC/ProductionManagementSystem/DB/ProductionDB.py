# $Id$
"""
    DIRAC ProductionDB class is a front-end to the pepository database containing
    Workflow (templates) Productions and vectors to create jobs.

    The following methods are provided for public usage:

"""
__RCSID__ = "$Revision: 1.65 $"

import string, types
from DIRAC.Core.Base.DB import DB
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC  import gLogger, S_OK, S_ERROR
from DIRAC.TransformationSystem.DB.TransformationDB import TransformationDB
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


  def addProduction(self, name, parent, description, long_description, body, 
                    fileMask, groupsize, authorDN, authorGroup, update=False, 
                    inheritedFrom=0L, bkQuery = {}, plugin='',productionType='', productionGroup='',
                    maxJobs=0,addFiles=True):

    prod_type = productionType
    if not prod_type:
      # if there is no input specification it is simulation
      if (fileMask == '' or fileMask == None) and not bkQuery: 
        prod_type = "Simulation"
      else:
        prod_type = "Processing"
    if not plugin:
      plugin = "None"
    agentType = "Manual"

    #status = "NEW" # alwais NEW when created
    # WE HAVE TO CHECK IS WORKFLOW EXISTS
    TransformationID = self.getTransformationID(name)
    if TransformationID > 0: # Transformation exists
      error = 'Production "%s" exists in the database"' % (name)
      gLogger.error( error )
      return S_ERROR( error )

    result = self.addTransformation(name, description, long_description, 
                                    authorDN, authorGroup, prod_type, plugin, 
                                    agentType, fileMask, bkQuery=bkQuery,
                                    transformationGroup=productionGroup,addFiles=addFiles)
    if not result['OK']:
      error = 'Transformation "%s" FAILED to be published by DN="%s" with message "%s"' % (name, authorDN, result['Message'])
      gLogger.error(error)
      return S_ERROR( error )

    TransformationID = result['Value']
    result = self.__insertProductionParameters(TransformationID, groupsize, parent, body, 
                                               inheritedFrom,maxJobs=maxJobs)
    if not result['OK']:
      # if for some reason this failed we have to roll back
      result_rollback1 = self.deleteTransformation(TransformationID)
      error = 'Transformation "%s" ID=%d FAILED to add ProductionsParameters with message "%s"' % (name, TransformationID, result['Message'])
      gLogger.error(error)
      return S_ERROR( error )
      
    return S_OK(TransformationID)

  def addDerivedProduction(self, name, parent, description, long_description, body, fileMask, groupsize, 
                           authorDN, authorGroup, originaProdIDOrName,bkQuery = {}, plugin='', 
                           productionType='', productionGroup='',maxJobs=0):
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

    result = self.addProduction(name, parent, description, long_description, body, fileMask, 
                                groupsize, authorDN, authorGroup, False, originalProdID,
                                bkQuery=bkQuery, plugin=plugin,productionType=productionType,
                                productionGroup=productionGroup,addFiles=False)
    if not result['OK']:
      return result

    newProdID = result['Value']

    # Mark the previously processed files
    ids = {}
    req = "SELECT FileID,Status,JobID,TargetSE,UsedSE from T_%s WHERE Status<>'Unused';" % (originalProdID)
    result = self._query(req)
    if result['OK']:
      if result['Value']:
        for fileID,status,jobID,targetSE,usedSE in result['Value']:
          if jobID:
            jobName = str(int(originalProdID)).zfill(8)+'_'+str(int(jobID)).zfill(8)
            req = "INSERT INTO T_%s (Status,JobID,FileID,TargetSE,UsedSE,LastUpdate) VALUES ('%s','%s',%d,'%s','%s',UTC_TIMESTAMP())" % (newProdID,status,jobName,int(fileID),targetSE,usedSE)
          else:
            req = "INSERT INTO T_%s (Status,FileID,TargetSE,LastUpdate) VALUES ('%s',%d,'%s',UTC_TIMESTAMP())" % (newProdID,status,int(fileID),targetSE)
          result = self._update(req)
          if not result['OK']:
            # rollback the operation
            print "KGG We need to add code to restore original production status"
            result = self.deleteProduction(newProdID)
            if not result['OK']:
              gLogger.warn('Failed to rollback the production creation')
            return S_ERROR('Failed to create derived production: error while marking already processed files')

    if fileMask:
      result = self.updateTransformation(newProdID)

    return S_OK(newProdID)

  def __insertProductionParameters(self, TransformationID, groupsize, parent, body, 
                                   inheritedFrom=0,maxJobs=0):
    """
    Inserts additional parameters into ProductionParameters Table
    """
    inFields = ['TransformationID', 'GroupSize', 'Parent', 'Body', 'InheritedFrom','MaxNumberOfJobs']
    inValues = [TransformationID, groupsize, parent, body, inheritedFrom, maxJobs]
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
    cmd = "SELECT GroupSize, Parent, Body, InheritedFrom,MaxNumberOfJobs,EventsPerJob  from ProductionParameters WHERE TransformationID='%d';" % id_
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
      retdict['MaxNumberOfJobs']=result['Value'][0][4]
      retdict['EventsPerJob']=result['Value'][0][5]
    else:
      retdict['GroupSize']=0
      retdict['Parent']=""
      retdict['Body']=""
      retdict['InheritedFrom']=0
      retdict['MaxNumberOfJobs']=0
      retdict['EventsPerJob']=0

    return S_OK(retdict)
    
  def getDistinctAttributeValues(self, attribute, selectDict ):  
    """ Get distinct values of the given production attribute
    """
    
    transformationParameters = ['Type','Plugin','AgentType','Status','TransformationGroup']
    
    if not attribute in transformationParameters:
      return S_ERROR('Can not serve values for attribute %s' % attribute) 
    
    selectionList = []
    selectionString = ''
    for name,value in selectDict.items():
      selectionList.append("%s='%s'" % (name,value) )
    if selectionList:
      selectionString = ' AND '.join(selectionList)
      
    req = 'SELECT DISTINCT(%s) FROM Transformations' % attribute
    if selectionString:
      req += " WHERE %s" % selectionString
            
    result = self._query(req)
    if not result['OK']:
      return result
      
    valueList = [ x[0] for x in result['Value'] ]
    return S_OK(valueList)        
        

  def getProductionParametersWithoutBody(self, transName):
    """
    Get additional parameters from ProductionParameters Table
    """
    id_ = self.getTransformationID(transName)
    cmd = "SELECT GroupSize, Parent, InheritedFrom,MaxNumberOfJobs,EventsPerJob  from ProductionParameters WHERE TransformationID='%d'" % id_
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
      retdict['MaxNumberOfJobs']=result['Value'][0][3]
      retdict['EventsPerJob']=result['Value'][0][4]
    else:
      retdict['GroupSize']=0
      retdict['Parent']=""
      retdict['InheritedFrom']=0
      retdict['MaxNumberOfJobs']=0
      retdict['EventsPerJob']=0

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
  
  def __deleteProductionJobs(self, TransformationID):
    """ Delete all the jobs from the Jobs table for production with TransformationID
    """
    
    req = "DELETE FROM Jobs WHERE TransformationID=%d" % int(TransformationID)
    result = self._update(req)
    if not result['OK']:
      error = "Failed to remove jobs for the transformationID %s message: %s" % (TransformationID, result['Message'])
      gLogger.error( error )
      return S_ERROR( error )
    return result

  def getProductionList(self):
    cmd = "SELECT  TransformationID, TransformationName, Description, LongDescription, CreationDate, AuthorDN, AuthorGroup, Type, Plugin, AgentType, Status, FileMask from Transformations;"
    result = self._query(cmd)
    if result['OK']:
      newres=[] # repacking
      for pr in result['Value']:
        newres.append({'TransformationID':pr[0], 
                       'TransformationName':pr[1], 
                       'Description':pr[2], 
                       'LongDescription':pr[3], 
                       'CreationDate':Time.toString(pr[4]),
                       'AuthorDN':pr[5], 
                       'AuthorGroup':pr[6], 
                       'Type':pr[7], 
                       'Plugin':pr[8], 
                       'AgentType':pr[9], 
                       'Status':pr[10], 
                       'FileMask':pr[11] })
      return S_OK(newres)
    return result

  def getAllProductions(self):
    result1 = self.getAllTransformations()
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
    result_step1 = self.deleteTransformation(transID)
    if result_step1['OK']:
      # we have to execute all
      result_step2 = self.__deleteProductionParameters(transID)
      result_step3 = self.__deleteProductionJobs(transID)
      if not result_step2['OK']:
        return result_step2
      if not result_step3['OK']:
        return result_step3
      return S_OK()
    else:
      return  result_step1

  def cleanProduction(self, transName):
    """ Remove the jobs and the files for the supplied production
    """
    transID = self.getTransformationID(transName)
    if transID == 0:
      return S_OK()
    res = self.__deleteProductionJobs(transID)
    if not res['OK']:
      return res
    return self._deleteTransformationFiles(transID)

  def getProductionBody(self, transName):
    transID = self.getTransformationID(transName)
    result = self.getProductionParameters(transID)
    if result['OK']:
      return S_OK(result['Value']['Body']) # we
    else:
      return S_ERROR("Failed to retrieve Production Body with Transformation '%s' " % transName)


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
      prod['MaxNumberOfJobs']=result['Value']['MaxNumberOfJobs']
      prod['EventsPerJob']=result['Value']['EventsPerJob']
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
      prod['Value']['MaxNumberOfJobs']=result['Value']['MaxNumberOfJobs']
      return prod
    else:
      return S_ERROR('Failed to retrive Production=%s message=%s' % (transName, result['Message']))


  def addProductionJob(self, transName, inputVector='', se='Unknown'):
      """ Add one job to Production
      """
      productionID = self.getTransformationID(transName)
      self.lock.acquire()
      req = "INSERT INTO Jobs (TransformationID, WmsStatus, JobWmsID, TargetSE, CreationTime, LastUpdateTime) VALUES\
      (%s,'%s','%d','%s', UTC_TIMESTAMP(), UTC_TIMESTAMP());" % (productionID,'Created', 0, se)
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
      
      if inputVector:
        fields = ['TransformationID','JobID','InputVector']
        values = [productionID,jobID,inputVector]
        result = self._insert('JobInputs',fields,values,connection)
        if not result['OK']:
          return S_ERROR('Failed to add input vector for job %d' % jobID)
      
      return S_OK(jobID)

  def extendProduction(self, transName, nJobs, authorDN):
    """ Extend SIMULATION type production by nJobs number of jobs
    """
    result = self.getTransformation(transName)
    if not result['OK']:
      return result

    ttype = result['Value']['Type']
    if ttype.lower() not in ['simulation','mcsimulation']:
      return S_ERROR('Can not extend non-SIMULATION type production')

    jobIDs = []
    for job in range(nJobs):
      result = self.addProductionJob(transName)
      if result['OK']:
        jobIDs.append(result['Value'])
      else:
        return result
      
    # Add information to the production logging
    message = 'Production extended by %d jobs' % nJobs
    resultlog = self.updateTransformationLogging(transName,message,authorDN)  
      
    return S_OK(jobIDs)


######################## Job section ############################

  def selectWMSJobs(self,productionID,statusList = [],newer = 0):
    """ Select WMS job IDs for the given production having one of the specified
        statuses and optionally with last status update older than "newer" number
        of minutes
    """

    req = "SELECT JobID, JobWmsID, WmsStatus FROM Jobs"
    condList = ['TransformationID=%d' % int(productionID)]
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

  def __get_site_se_mapping(self):
    """ Helper function to prepare a dictionary of local SEs per site defined
        in the Configuration Service
    """

    mappingDict = {}

    result = gConfig.getSections('/Resources/Sites')
    if not result['OK']:
      return result
    gridTypes = result['Value']
    for gridType in gridTypes:
      result = gConfig.getSections('/Resources/Sites/'+gridType)
      if not result['OK']:
        continue
      siteList = result['Value']
      for site in siteList:
        ses = gConfig.getValue('/Resources/Sites/%s/%s/SE' % (gridType,site),[])
        if ses:
          mappingDict[site] = ses

    return S_OK(mappingDict)

  def selectJobs(self,productionID,statusList = [],numJobs=1,site='',older=None,newer=None):
    """ Select jobs with the given status from the given production
    """

    # Prepare Site-SE resolution mapping
    result = self.__get_site_se_mapping()
    site_se_mapping = {}
    if result['OK']:
      site_se_mapping = result['Value']

    req = "SELECT JobID,CreationTime,TargetSE,WmsStatus FROM Jobs" 
    conditions = ['TransformationID=%d' % int(productionID)]
    if statusList:
      statusString = ','.join(["'"+x+"'" for x in statusList])
      conditions.append(" WmsStatus IN (%s) " % statusString)
    if older:
      conditions.append(" LastUpdateTime < '%s' " % older)
    if newer:
      conditions.append(" LastUpdateTime > '%s' " % newer)    
    if conditions:
      req += " WHERE %s " % ' AND '.join(conditions)  
      
    if not site:
      # do not uncomment this  req += " AND TargetSE='%s'" % site
      req += " LIMIT %d" % numJobs        
    result = self._query(req)

    if not result['OK']:
      return result

    resultDict = {}
    if result['Value']:
      jobList = [ int(x[0]) for x in result['Value']]
      resultInput = self.getJobInputVector(int(productionID),jobList)
      inputVectorDict = {}
      if resultInput['OK']:
        inputVectorDict = resultInput['Value']
      if not site:
        # We do not care about the destination site
        for row in result['Value']:
          jobID = int(row[0])
          inputVector = ''
          if inputVectorDict.has_key(jobID):
            inputVector = inputVectorDict[jobID]
          if inputVector:
            se = row[2]
            targetSite = ''
            for s,sList in site_se_mapping.items():
              if se in sList:
                targetSite = s
            if targetSite:
              resultDict[jobID] = {'InputData':inputVector,'TargetSE':se,'Status':row[3],'Site':targetSite}
            else:
              gLogger.warn('Can not find corresponding site for se: '+se)
          else:
            resultDict[jobID] = {'InputData':inputVector,'TargetSE':row[2],'Status':row[3],'Site':'ANY'}
      else:
        # Get the jobs now

        for row in result['Value']:
          if len(resultDict) < numJobs:
            jobID = int(row[0])
            inputVector = ''
            if inputVectorDict.has_key(jobID):
              inputVector = inputVectorDict[jobID]
            if inputVector:
              se = row[2]
              targetSite = ''
              for s,sList in site_se_mapping.items():
                if se in sList:
                  targetSite = s
              if targetSite and targetSite == site:
                resultDict[jobID] = {'InputData':inputVector,'TargetSE':row[2],'Status':row[3],'Site':targetSite}
            else:
              resultDict[jobID] = {'InputData':inputVector,'TargetSE':row[2],'Status':row[3],'Site':'ANY'}
          else:
            break

    return S_OK(resultDict)

  def getJobStats(self,productionID):
    """ Returns dictionary with number of jobs per status in the given production. 
        The status is one of the following:
        Created - this counter returns the total number of jobs already created;
        Submitted - jobs submitted to the WMS;
        Waiting - jobs being checked or waiting for execution in the WMS;
        Running - jobs being executed in the WMS;
        Stalled - jobs stalled in the WMS;
        Done - jobs completed in the WMS;
        Failed - jobs completed with errors in the WMS;
    """
    productionID = self.getTransformationID(productionID)

    if productionID:
      resultStats = self.getCounters('Jobs',['WmsStatus'],{'TransformationID':productionID}) 
    else:
      resultStats = self.getCounters('Jobs',['WmsStatus','TransformationID'],{})   
    if not resultStats['OK']:
      return resultStats
    if not resultStats['Value']:
      return S_ERROR('No records found')
    

    statusList = {}
    for s in ['Created','Submitted','Waiting','Running','Stalled','Done','Failed']:
      statusList[s] = 0

    for attrDict,count in resultStats['Value']:
      status = attrDict['WmsStatus']
      stList = ['Created']
      # Choose the status to report
      if not status == "Created" and not status == "Reserved":
        if not "Submitted" in stList:
          stList.append("Submitted")
      if status == "Waiting" or status == "Received" or status == "Checking" or \
         status == "Matched" or status == "Submitted" or status == "Staging":
        if not "Waiting" in stList:
          stList.append("Waiting")
      if status == "Done" or status == "Completed":
        if not "Done" in stList:
          stList.append("Done")
      if status == "Failed" or status == "Stalled" or status == "Running":
        stList.append(status)

      for st in stList:
        statusList[st] += count

    return S_OK(statusList)

  def getJobWmsStats(self,productionID):
    """ Returns dictionary with number of jobs per status for the given production.
    """

    productionID = self.getTransformationID(productionID)

    if productionID:
      resultStats = self.getCounters('Jobs',['WmsStatus'],{'TransformationID':productionID})
    else:
      resultStats = self.getCounters('Jobs',['WmsStatus','TransformationID'],{})      
    if not resultStats['OK']:
      return resultStats
    if not resultStats['Value']:
      return S_ERROR('No records found')
    
    statusList = {}

    for attrDict,count in resultStats['Value']:
      status = attrDict['WmsStatus']
      statusList[status] = count

    return S_OK(statusList)
    
  def reserveJob(self,productionID,jobID):
    """ Reserve the job jobID from production productionID for submission to WMS
    """   
    
    req = "UPDATE Jobs SET WmsStatus='Reserved' WHERE TransformationID=%d and JobID=%d;" % (int(productionID),jobID)
    result = self._update(req)
    if not result['OK']:
      return S_ERROR('Failed to set Reserved status for job %d - MySQL error: '+result['Message'] % int(jobID))
    if not result['Value']:
      return S_ERROR('Failed to set Reserved status for job %d - already Reserved' % int(jobID) )
    # The job is reserved, update the time stamp
    result = self.setJobStatus(productionID,jobID,'Reserved')
    if not result['OK']:
      return S_ERROR('Failed to set Reserved status for job %d - failed to update the time stamp' % int(jobID))
    
    return S_OK()

  def setJobStatus(self,productionID,jobID,status):
    """ Set status for job with jobID in production with productionID
    """

    if type(jobID) != types.ListType:
      jobIDList = [jobID]
    else:
      jobIDList = list(jobID)  
      
    jobString = ','.join([ str(x) for x in jobIDList ])  
    req = "UPDATE Jobs SET WmsStatus='%s', LastUpdateTime=UTC_TIMESTAMP() WHERE TransformationID=%d AND JobID in (%s)" % (status,productionID,jobString)
    result = self._update(req)
    return result

  def setJobWmsID(self,productionID,jobID,jobWmsID):
    """ Set WmsID for job with jobID in production with productionID
    """

    req = "UPDATE Jobs SET JobWmsID='%s', LastUpdateTime=UTC_TIMESTAMP() WHERE JobID=%d;" % (productionID,jobWmsID,jobID)
    result = self._update(req)
    return result

  def setJobStatusAndWmsID(self,productionID,jobID,status,jobWmsID):
    """ Set status and JobWmsID for job with jobID in production with productionID
    """

    req = "UPDATE Jobs SET WmsStatus='%s', JobWmsID='%s', LastUpdateTime=UTC_TIMESTAMP() WHERE TransformationID=%d AND JobID=%d;" % (status,jobWmsID,productionID,jobID)
    result = self._update(req)
    return result

  def setJobTagetSE(self,productionID,jobID,se):
    """ Set TargetSE for job with jobID in production with productionID
    """

    req = "UPDATE Jobs SET TargetSE='%s', LastUpdateTime=UTC_TIMESTAMP() WHERE TransformationID=%d AND JobID=%d;" % (se,productionID,jobID)
    result = self._update(req)
    return result

  def setJobInputVector(self,productionID,jobID,inputVector):
    """ Set TargetSE for job with jobID in production with productionID
    """

    req = "UPDATE JobInputs SET InputVector='%s' WHERE JobID=%d;" % (inputVector,jobID)
    result = self._update(req)
    if not result['OK']:
      return result
    
    req = "UPDATE Jobs SET LastUpdateTime=UTC_TIMESTAMP() WHERE TransformationID=%d AND JobID=%d;" % (productionID,jobID)
    result = self._update(req)
    return result

  def deleteJob(self,productionID,jobID):
    """ DeleteJob with jobID in production with productionID
    """
    productionID = self.getTransformationID(productionID)
    req = "DELETE FROM Jobs WHERE TransformationID=%d AND JobID=%d;" % (productionID,jobID)
    result = self._update(req)
    return result

  def deleteJobs(self,productionID,jobIDbottom, jobIDtop):
    """ DeleteJob with jobID in production with productionID
    """
    productionID = self.getTransformationID(productionID)
    req = "DELETE FROM Jobs WHERE TransformationID=%d AND JobID>=%d AND JobID<=%d;" % (productionID,jobIDbottom, jobIDtop)
    result = self._update(req)
    return result

  def getJobInputVector(self,productionID,jobID):
    """ Get input vector for the given job
    """
    
    if type(jobID) != types.ListType:
      jobIDList = [jobID]
    else:
      jobIDList = list(jobID)
      
    jobString = ','.join(["'"+str(x)+"'" for x in jobIDList])      
    
    req = "SELECT JobID,InputVector FROM JobInputs WHERE JobID in (%s) AND TransformationID='%d';" % (jobString,productionID)
    result = self._query(req)
    inputVectorDict = {}
    if result['OK'] and result['Value']:
      for row in result['Value']:
        inputVectorDict[row[0]] = row[1] 
      
    return S_OK(inputVectorDict)  

  def getJobInfo(self,productionID,jobID):
    """ returns dictionary with information for given Job of given productionID
    """
    productionID = self.getTransformationID(productionID)
    req = "SELECT JobID,JobWmsID,WmsStatus,TargetSE,CreationTime,LastUpdateTime FROM Jobs WHERE TransformationID=%d AND JobID='%d';" % (productionID,jobID)
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
    else:
      if result['OK']:
        result['Message']='' #used for printing error message
      error = "Failed to find job with JobID=%s/TransformationID=%s in the Jobs table with message: %s" % (jobID, productionID, result['Message'])
      gLogger.error( error )
      return S_ERROR( error )
    
    result = self.getJobInputVector(productionID,jobID)
    jobDict['InputVector']=''
    if result['OK']:
      if result['Value'].has_key(jobID):
        jobDict['InputVector']=result['Value'][jobID]    
    
    return S_OK(jobDict)

  def createProductionRequest(self, requestDict={}):
    """ Create new Production Request
    """

    requestFields = ['RequestName','Description','EventType','RequestType','NumberOfEvents','CPUPerEvent']
    mandatoryFields = ['RequestName','Description']
    for field in mandatoryFields:
      if field not in requestDict.keys():
        return S_ERROR('Mandatory field %s not provided' % field)

    rDict = {}
    rDict['EventType'] = 'Unknown'
    rDict['RequestType'] = 'Simulation'
    rDict['NumberOfEvents'] = 0
    rDict['CPUPerEvent'] = 0.0
    rDict.update(requestDict)


    self.lock.acquire()
    req = "INSERT INTO ProductionRequests ("
    req += ','.join(requestFields)+',CreationTime) VALUES '
    req += "('%s','%s','%s','%s','%s','%s',UTC_TIMESTAMP())" % (rDict["RequestName"], \
                                                                rDict["Description"], \
                                                                rDict['EventType'], \
                                                                rDict['RequestType'], \
                                                                rDict['NumberOfEvents'], \
                                                                rDict['CPUPerEvent'])

    result = self._getConnection()
    if result['OK']:
      connection = result['Value']
    else:
      return S_ERROR('Failed to get connection to MySQL: '+result['Message'])
    res = self._update(req,connection)
    if not res['OK']:
      self.lock.release()
      return res
    req = "SELECT LAST_INSERT_ID();"
    res = self._query(req,connection)
    self.lock.release()
    if not res['OK']:
      return res
    requestID = int(res['Value'][0][0])

    return S_OK(requestID)

  def getProductionRequest(self,requestIDList):
    """ Get the Production Request details
    """

    requestFields = ['RequestID','RequestName','Description','EventType','RequestType',
                     'NumberOfEvents','CPUPerEvent','CreationTime','ProductionID']
    fieldList = ','.join(requestFields)
    req = "SELECT %s FROM ProductionRequests" % fieldList
    if requestIDList:
      reqList = ','.join([ str(int(x)) for x in requestIDList])
      req += " WHERE RequestID IN (%s)" % reqList
    result = self._query(req)
    if not result['OK']:
      return result

    if not result['Value']:
      return S_ERROR('No request found')

    resultDict = {}
    for row in result['Value']:
      requestID = row[0]
      requestDict = {}
      for i in range(len(requestFields)-1):
        if requestFields[i+1] == 'NumberOfEvents':
          requestDict[requestFields[i+1]] = int(row[i+1])
        elif requestFields[i+1] == 'CPUPerEvent':
          requestDict[requestFields[i+1]] = float(row[i+1])
        else:
          requestDict[requestFields[i+1]] = str(row[i+1])
      resultDict[requestID] = requestDict

    return S_OK(resultDict)

  def updateProductionRequest(self,requestID,requestDict):
    """ Update existing production request
    """

    requestFields = ['RequestName','Description','EventType','RequestType',
                     'NumberOfEvents','CPUPerEvent']
    # Check request fields
    for field in requestDict.keys():
      if field not in requestFields:
        return S_ERROR('Field %s not known' % field)


    setString = ','.join([x+"='"+requestDict[x]+"'" for x in requestDict.keys()])
    req = 'UPDATE ProductionRequests SET %s WHERE RequestID=%d' % (setString,int(requestID))
    result = self._update(req)
    return result
