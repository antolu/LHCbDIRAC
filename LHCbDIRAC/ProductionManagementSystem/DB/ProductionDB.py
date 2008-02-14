# $Id: ProductionDB.py,v 1.7 2008/02/14 00:27:17 gkuznets Exp $
"""
    DIRAC ProductionDB class is a front-end to the pepository database containing
    Workflow (templates) Productions and vectors to create jobs.

    The following methods are provided for public usage:

"""
__RCSID__ = "$Revision: 1.7 $"

from DIRAC.Core.Base.DB import DB
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC  import gLogger, S_OK, S_ERROR
from DIRAC.Core.Transformation.TransformationDB import TransformationDB

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
      pr=result['Value']
      newres = {'WFName':pr[0], 'WFParent':pr[1], 'Description':pr[2], 'LongDescription':pr[3],
                'AuthorDN':pr[4], 'AuthorGroup':pr[5], 'PublishingTime':pr[6].isoformat(' ')}
      return S_OK(newres)
    else:
      return S_ERROR('Failed to retrive Workflow with the name=%s' % (name, result['Message']) )


################ TRANSFORMATION SECTION ####################################


  def addProduction(self, name, parent, description, long_description, body, fileMask, groupsize, authorDN, authorGroup):

    if fileMask == '' or fileMask == None: # if mask is empty it is simulation
      type = "SIMULATION"
    else:
      type = "PROCESSING"
    plugin = "NONE"
    agentType = "MANUAL"
    #status = "NEW" # alwais NEW when created
    # WE HAVE TO CHECK IS WORKFLOW EXISTS
    if self.getTransformationID(name) == 0: # workflow is not exists

        #result = self._escapeString( body )
        #if not result['OK']: return result
        #body_esc = result['Value'][1:len(result['Value'])-1] # we have to remove trailing " left by self._escapeString()
        #result = self._escapeString( descr_long )
        #if not result['OK']: return result
        #descr_long_esc = result['Value'][1:len(result['Value'])-1] # we have to remove trailing " left by self._escapeString()

        result_step1 = TransformationDB.addTransformation(self, name, description, long_description, authorDN, authorGroup, type, plugin, agentType, fileMask)

        if result_step1['OK']:
          TransformationID = result_step1['Value']
          result_step2 = self.__insertProductionParameters(TransformationID, groupsize, parent, body)
          if result_step2['OK']:
            result_step3 = self.__addJobTable(TransformationID)
            if not result_step3['OK']:
              # if for some reason this faled we have to roll back
              result_rollback2 = self.__removeProductionParameters(TransformationID)
              result_rollback1 = TransformationDB.removeTranformationID(self, TransformationID)
              return result_step3
            else:
              # everithing OK
              return result_step1
          else:
            # if for some reason this faled we have to roll back
            result_rollback1 = TransformationDB.removeTransformationID(self, TransformationID)
            return result_step2

        if not (result_step1['OK'] and result_step2['OK'] and result_step1['OK']):
          error = 'Tansformation "%s" FAILED to be published by DN="%s" with message "%s"' % (name, authorDN, result['Message'])
          gLogger.error(error)
          return S_ERROR( error )

    else: # update was not requested
      error = 'Production "%s" is exist in the repository, it was published by DN="%s"' % (name, authorDN)
      gLogger.error( error )
      return S_ERROR( error )

  def __insertProductionParameters(self, TransformationID, groupsize, parent, body):
    """
    Inserts additional parameters into ProductionParameters Table
    """
    inFields = ['TransformationID', 'GroupSize', 'Parent', 'Body']
    inValues = [TransformationID, groupsize, parent, body]
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
    id = self.getTransformationID(transName)
    cmd = "SELECT GroupSize, Parent, Body  from ProductionParameters WHERE TransformationID='%d'" % id
    result = self._query(cmd)
    dict={}
    if not result['OK']:
      error = "Failed to get production parameters from ProductionParameters table for the Transformation %s with message: %s" % (transName, result['Message'])
      gLogger.error( error )
      return S_ERROR( error )

    dict['GroupSize']=result['Value'][0][0]
    dict['Parent']=result['Value'][0][1]
    dict['Body']=result['Value'][0][2]
    return S_OK(dict)

  def getProductionParametersWithoutBody(self, transName):
    """
    Get additional parameters from ProductionParameters Table
    """
    id = self.getTransformationID(transName)
    cmd = "SELECT GroupSize, Parent  from ProductionParameters WHERE TransformationID='%d'" % id
    result = self._query(cmd)
    dict={}
    if not result['OK']:
      error = "Failed to get production parameters from ProductionParameters table for the Transformation %s with message: %s" % (transName, result['Message'])
      gLogger.error( error )
      return S_ERROR( error )

    dict['GroupSize']=result['Value'][0][0]
    dict['Parent']=result['Value'][0][1]
    return S_OK(dict)

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
    """ Method to add Job table """
    req = """CREATE TABLE Jobs_%s(
    JobID INTEGER NOT NULL AUTO_INCREMENT,
    WmsStatus char(16) DEFAULT 'CREATED',
    JobWmsID char(16),
    TimeStamp TIMESTAMP,
    InputVector BLOB,
    PRIMARY KEY(JobID),
    INDEX(WmsStatus)
    )""" % str(TransformationID)
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

  def getProductionsList(self):
    cmd = "SELECT  TransformationID, TransformationName, Description, LongDescription, CreationDate, AuthorDN, AuthorGroup, Type, Plugin, AgentType, Status, FileMask from Transformations;"
    result = self._query(cmd)
    if result['OK']:
      newres=[] # repacking
      for pr in result['Value']:
        newres.append({'TransformationID':pr[0], 'TransformationName':pr[1], 'Description':pr[2], 'LongDescription':pr[3], 'CreationDate':pr[4],
                            'AuthorDN':pr[5], 'AuthorGroup':pr[6], 'Type':pr[7], 'Plugin':pr[8], 'AgentType':pr[9], 'Status':pr[10], 'FileMask':pr[11] })
      return S_OK(newres)
    return result

  def getAllProductions(self):
    result1 = TransformationDB.getAllTransformations(self)
    if not result1['OK']:
        return result1
    for prod in result1['Value']:
      id = long(prod['TransID'])
      result2 = self.getProductionParametersWithoutBody(id)
      if result2['OK']:
        prod['GroupSize']=result2['Value']['GroupSize']
        prod['Parent']=result2['Value']['Parent']
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
    result = self.getProductionParameters(transName)
    if result['OK']:
      return S_OK(result['Value']['Body']) # we
    else:
      return S_ERROR("Failed to retrive Production Body with Transformation '%s' " % transName)

#  def deleteTranformation(self, name):
#    if self._transformationExists(name):
#      result = TransformationDB.removeTransformation(self, name)
#      if not result['OK']:
#        return S_ERROR("Failed to delete Transformation '%s' with the message %s" % (name, result['Message']))
#      return result
#    return S_ERROR("No Transformation with the name '%s' in the ProductionDB" % name)
#
#
#  def deleteProductionID(self, id):
#    if self._isProductionIDExists(id):
#      cmd = "DELETE FROM Productions WHERE ProductionID=\'%d\'" % (id)
#      result = self._update(cmd)
#      if not result['OK']:
#        return S_ERROR("Failed to delete Production with ID '%d' with the message %s" % (id, result['Message']))
#      return result
#    return S_ERROR("No production with the ID '%d' in the Production Repository" % id)


  def getProductionInfo(self, transName):
    transID = self.getTransformationID(transName)
    prod = getTransformation(transID)
    result = self.getProductionParametersWithoutBody(transID)
    result = self._query(cmd)
    if result['OK']:
      prod['GroupSize']=result['Value']['GroupSize']
      prod['Parent']=result['Value']['Parent']
      return S_OK(prod)
    else:
      return S_ERROR('Failed to retrive Production=%s message=%s' % (transName, result['Message']))


  def addProductionJob(self, productionID, inputVector):
      """ Production ID is number without prepending 0000
      """
      self.lock.acquire()
      table = 'Jobs_%d'% long(productionID)
      result = self._insert(table, [ 'WmsStatus', 'JobWmsID', 'InputVector' ],
                            ['Created', 0, inputVector])
      if not result['OK']:
        self.lock.release()
        error = 'Job FAILED to be published for Production="%s" with the input vector %s and with the message "%s"' % (productionID, inputVector, result['Message'])
        gLogger.error(error)
        return S_ERROR( error )

      result2 = self._query("SELECT LAST_INSERT_ID();")
      self.lock.release()
      if not result2['OK']:
        return result2
      jobID = int(result2['Value'][0][0])
      gLogger.verbose('Job published for Production="%s" with the input vector "%s"' % (productionID, inputVector))
      return S_OK(jobID)

