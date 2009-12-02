""" Client class to access the production manager service
"""
# $Id$
__RCSID__ = "$Revision: 1.6 $"

from DIRAC  import gLogger, gConfig, S_OK, S_ERROR
from DIRAC.Core.Utilities.List import breakListIntoChunks
from DIRAC.Core.DISET.RPCClient import RPCClient
import types

class ProductionClient:

  #############################################################################
  def __init__(self):
    pass

  #############################################################################
  def getProductionsWithStatus(self,status):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getTransformationWithStatus(status)

  #############################################################################
  def getAllProductions(self):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getAllProductions()

  #############################################################################
  def getParameters(self,prodID,pname=''):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    res = server.getTransformation(int(prodID))
    if not res['OK']: 
      return res
    paramDict = res['Value']
    if paramDict.has_key('Additional'):
      paramDict.update(paramDict.pop('Additional'))
    if pname:
      if paramDict.has_key(pname):
        return S_OK(paramDict[pname])
      else:
        return S_ERROR('Parameter %s not found for production' % pname)
    return S_OK(paramDict)
  
  #############################################################################  
  def setProductionStatus(self,prodID,status):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.setTransformationStatus(prodID, status)
  
  #############################################################################  
  def setTransformationAgentType(self,prodID,status):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.setTransformationAgentType(prodID, status)  

  #############################################################################  
  def deleteProduction(self,prodID):
    """ Deletes a production from the production management system ONLY.
    """    
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.deleteProduction(prodID)
  
  #############################################################################
  def selectWMSJobs(self,prodID,statusList=[],newer=0):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.selectWMSJobs(prodID,statusList,newer)
  
  #############################################################################
  def cleanProduction(self,prodID):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.cleanProduction(prodID)

  #############################################################################
  def getProduction(self,prodID):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getTransformation(prodID)
  
  #############################################################################
  def getProductionStatus(self,prodID):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    res = server.getTransformation(prodID)
    if not res['OK']:
      return res
    return S_OK(res['Value']['Status'])

  #############################################################################
  def getProductionLastUpdate(self,prodID):
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getTransformationLastUpdate(prodID)
  
  #############################################################################
  def getAllProductions(self):
    """Returns a dictionary of production IDs and metadata.
    """
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getProductionSummary()

  #############################################################################
  def getProductionInfo(self,prodID):
    """Returns a dictionary of production information.
    """
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getProductionInfo(prodID)
  
  #############################################################################
  def getTransformationLogging(self,prodID):
    """The logging information for the given production is returned.  This includes
       the operation performed, any messages associated with the operation and the
       DN of the responsible performing the action.
    """  
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getTransformationLogging(prodID)  
  
  #############################################################################
  def getProductionSummary(self): 
    """Returns a detailed summary for the productions in the system.
    """ 
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getProductionSummary()

  #############################################################################
  def getJobWMSStats(self,prodID):
    """Retrieve job WMS statistics for a given production ID. 
    """
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getJobWmsStats(prodID)
  
  #############################################################################  
  def getFilesForTransformation(self,prodID,orderOutput=True):
    """Retrieve transformation LFNs for given production ID. 
    """
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getFilesForTransformation(prodID,orderOutput) 

  #############################################################################    
  def getFileSummary(self,lfns,prodID):
    """Return the status of files for a given transformation.
    """
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getFileSummary(lfns,prodID)   
  
  #############################################################################    
  def setFileStatusForTransformation(self,prodID,status,lfns):
    """ Update file status for a given transformation.
    """  
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.setFileStatusForTransformation(prodID,status,lfns)   
  
  #############################################################################      
  def getJobInfo(self,prodID,prodJobID):
    """ Return job info.
    """  
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getJobInfo(prodID,prodJobID)

  #############################################################################      
  def getJobsToSubmit(self,prodID,numberOfJobs,site):
    """ Returns the information about jobs to submit. 
    """      
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.getJobsToSubmit(prodID,numberOfJobs,site)
    
  #############################################################################      
  def extendProduction(self,prodID,numberOfJobs):
    """ Allows to extend a production by specified number of jobs. 
    """         
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    return server.extendProduction(prodID,numberOfJobs)

  #############################################################################      
  def createProduction(self,fileName,fileMask='',groupSize=1,bkQuery={},plugin='',productionGroup='',productionType='',derivedProd='',maxJobs=0):
    """ Create a production, based on the supplied parameters.
        Any input data can be specified by either fileMast or bkQuery. If both are specified then the BKQuery takes precedence.

        Usage: createProduction <filename> <filemask> <groupsize> <bkquery> <plugin> <prodGroup> <prodType> <maxJobs>
    """  
    result=S_ERROR()
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)
    if derivedProd:
      result = server.publishDerivedProduction(derivedProd,xmlString,fileMask,groupSize,bkQuery,plugin,productionType,productionGroup,maxJobs,False)
    else:
      result = server.publishProduction(xmlString,fileMask,groupSize,False,bkQuery,plugin,productionGroup,productionType,maxJobs)

    return result

  #############################################################################        
  def setJobStatus(self,prodID,jobNumber,status):
    """ Update the status of a job for a given production. 
    """
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)    
    return server.setJobStatus(prodID,jobNumber,status)

  #############################################################################        
  def setJobStatusAndWmsID(self,prodID,jobNumber,status,wmsID):
    """ Update the status of a job for a given production. 
    """
    server = RPCClient('ProductionManagement/ProductionManager',timeout=120)     
    return server.setJobStatusAndWmsID(prodID,jobNumber,status,wmsID)

                