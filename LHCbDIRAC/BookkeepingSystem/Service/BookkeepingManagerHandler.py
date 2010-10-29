########################################################################
# $Id$
########################################################################

""" BookkeepingManaher service is the front-end to the Bookkeeping database 
"""

__RCSID__ = "$Id$"

from LHCbDIRAC.BookkeepingSystem.DB.BookkeepingDatabaseClient                         import BookkeepingDatabaseClient
from LHCbDIRAC.BookkeepingSystem.Agent.XMLReader.XMLFilesReaderManager                import XMLFilesReaderManager

from types                                                                        import *
from DIRAC.Core.DISET.RequestHandler                                              import RequestHandler
from DIRAC                                                                        import gLogger, S_OK, S_ERROR
from DIRAC.ConfigurationSystem.Client.Config                                      import gConfig
from DIRAC.DataManagementSystem.Client.ReplicaManager                             import ReplicaManager
from DIRAC.Core.Utilities.Shifter                                                 import setupShifterProxyInEnv
from DIRAC.Core.Utilities                                                         import DEncode
import time,sys,os
import cPickle


dataMGMT_ = None

reader_ = None

def initializeBookkeepingManagerHandler( serviceInfo ):
  """ Put here necessary initializations needed at the service start
  """
  global dataMGMT_
  dataMGMT_ = BookkeepingDatabaseClient()
  
  global reader_
  reader_ = XMLFilesReaderManager()

  return S_OK()

ToDoPath = gConfig.getValue("stuart","/opt/bookkeeping/XMLProcessing/ToDo")
    
class BookkeepingManagerHandler(RequestHandler):

  ###########################################################################
  # types_<methodname> global variable is a list which defines for each exposed 
  # method the types of its arguments, the argument types are ignored if the list is empty.

  types_echo = [StringType]
  def export_echo(self,input):
    """ Echo input to output
    """
    return S_OK(input)

  #############################################################################
  types_sendBookkeeping = [StringType, StringType]
  def export_sendBookkeeping(self, name, data):
      """
      This method send XML file to the ToDo directory
      """
      try:
          result  = reader_.readXMLfromString(name, data)
          if not result['OK']:
            return S_ERROR(result['Message'])
          if result['Value']=='':
            return S_OK("The send bookkeeping finished successfully!")
          else:
            return result
          """
          stamp = time.strftime('%Y.%m.%d-%H.%M.%S',time.gmtime())
          
          fileID=int(repr(time.time()).split('.')[1])
          
          filePath ="%s%s.%08d.%s"%(ToDoPath+os.sep, stamp, fileID, name)  
          update_file = open(filePath, "w")
          
          print >>update_file, data
          update_file.close()
          #copyXML(filePath)
          """
          return #S_OK("The send bookkeeping finished successfully!")
      except Exception, x:
          print str(x)
          return S_ERROR('Error during processing '+name)
  
  #############################################################################
  types_getAvailableSteps = [DictType]
  def export_getAvailableSteps(self, dict = {}):
    retVal = dataMGMT_.getAvailableSteps(dict)
    if retVal['OK']:
      if dict.has_key('StepId'):
        parameters = ['FileType', 'Visible']
        records = []
        for record in retVal['Value']:
          value = [record[0], record[1]]
          records += [value]
      else:
        parameters = ['StepId', 'StepName','ApplicationName', 'ApplicationVersion','OptionFiles','DDDB','CONDDB','ExtraPackages','Visibile']
        records = []
        for record in retVal['Value']:
          value = [record[0],record[1],record[2],record[3],record[4],record[5],record[6],record[7],record[8]]
          records += [value]
      parameters = ['StepId','StepName', 'ApplicationName','ApplicationVersion','Optionfiles','DDDB','CONDDB', 'ExtraPackages','Visible']
      records = []
      for record in retVal['Value']:
        records += [[record[0],record[1],record[2],record[3],record[4],record[5],record[6],record[7],record[8]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    else:
      return S_ERROR(retVal['Message'])
  
  #############################################################################
  types_getStepInputFiles = [IntType]
  def export_getStepInputFiles(self, StepId):
    retVal = dataMGMT_.getStepInputFiles(StepId)
    if retVal['OK']:
      records = []
      parameters = ['FileType','Visible']
      for record in retVal['Value']:
        records += [[record[0],record[1]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    else:
      return retVal
    
  #############################################################################
  types_getStepOutputFiles = [IntType]
  def export_getStepOutputFiles(self, StepId):                    
    retVal = dataMGMT_.getStepInputFiles(StepId)
    if retVal['OK']:
      records = []
      parameters = ['FileType','Visible']
      for record in retVal['Value']:
        records += [[record[0],record[1]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    else:
      return retVal
    
  #############################################################################
  types_getAvailableFileTypes = []
  def export_getAvailableFileTypes(self):
    return dataMGMT_.getAvailableFileTypes()
  
  #############################################################################
  types_insertFileTypes = [StringType,StringType]
  def export_insertFileTypes(self, ftype, desc):
    return dataMGMT_.insertFileTypes(ftype, desc)
  
  #############################################################################
  types_insertStep = [DictType]
  def export_insertStep(self, dict):
    return dataMGMT_.insertStep(dict)
  
  #############################################################################
  types_deleteStep = [IntType]
  def export_deleteStep(self, stepid):
    return dataMGMT_.deleteStep(stepid)
  
  #############################################################################
  types_updateStep = [DictType]
  def export_updateStep(self, dict):
    return dataMGMT_.updateStep(dict)
  
  ##############################################################################
  types_getAvailableConfigNames = []
  def export_getAvailableConfigNames(self):
    retVal = dataMGMT_.getAvailableConfigNames()
    if retVal['OK']:
      records = []
      parameters = ['Configuration Name']
      for record in retVal['Value']:
        records += [[record[0]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    else:
      return retVal
    
  #############################################################################
  types_getConfigVersions = [StringType]
  def export_getConfigVersions(self, configname):
    retVal =  dataMGMT_.getConfigVersions(configname)
    if retVal['OK']:
      records = []
      parameters = ['Configuration Version']
      for record in retVal['Value']:
        records += [[record[0]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    else:
      return retVal
  
 #############################################################################
  types_getConditions = [StringType, StringType ]
  def export_getConditions(self, configName, configVersion):
    retVal =  dataMGMT_.getConditions(configName, configVersion)
    if retVal['OK']:
      values = retVal['Value']
      if len(values) > 0:
        if values[0][0] != None:
          records = []
          parameters = ['SimId','Description']
          for record in values:
            records += [[record[0], record[1],record[2],record[3],record[4],record[5],record[6],record[7],record[8]]]
          return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
        elif values[0][1] != None:
          records = []
          parameters = ['DaqperiodId','Description']
          for record in values:
            records += [[record[1], record[9],record[10], record[11], record[12], record[13], record[14], record[15], record[16], record[17], record[18], record[19], record[20], record[21], record[22], record[23], record[24], record[25]]]
          return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
        else:
          return S_ERROR("Condition does not existis!")
      else:
        return S_ERROR("Condition does not existis!")
    else:
      return retVal
  
  #############################################################################
  types_getProcessingPass = [StringType, StringType, StringType, StringType]
  def export_getProcessingPass(self, configName, configVersion, conddescription, path):
    return dataMGMT_.getProcessingPass(configName, configVersion, conddescription, path)
    
  #############################################################################
  types_getProductions = [DictType]
  def export_getProductions(self, dict):
    configName = 'ALL'
    configVersion='ALL' 
    conddescription='ALL' 
    processing='ALL'
    evt='ALL'
    if dict.has_key('ConfigName'):
      configName = dict['ConfigName']
    
    if dict.has_key('ConfigVersion'):
      configVersion = dict['ConfigVersion']
    
    if dict.has_key('ConditionDescription'):
      conddescription = dict['ConditionDescription']
    
    if dict.has_key('ProcessingPass'):
      processing = dict['ProcessingPass']
    
    if dict.has_key('EventTypeId'):
      evt = dict['EventTypeId']
    
    retVal = dataMGMT_.getProductions(configName, configVersion, conddescription, processing, evt)
    if retVal['OK']:
      records = []
      parameters = ['Production/RunNumber']
      for record in retVal['Value']:
        records += [[record[0]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    else:
      return retVal 
  
  #############################################################################
  types_getFileTypes = [DictType]
  def export_getFileTypes(self, dict):
    configName = 'ALL'
    configVersion='ALL' 
    conddescription='ALL' 
    processing='ALL'
    evt='ALL'
    production = 'ALL'
    
    if dict.has_key('ConfigName'):
      configName = dict['ConfigName']
    
    if dict.has_key('ConfigVersion'):
      configVersion = dict['ConfigVersion']
    
    if dict.has_key('ConditionDescription'):
      conddescription = dict['ConditionDescription']
    
    if dict.has_key('ProcessingPass'):
      processing = dict['ProcessingPass']
    
    if dict.has_key('EventTypeId'):
      evt = dict['EventTypeId']
    
    if dict.has_key('Production'):
      production = dict['Production']
    
    retVal = dataMGMT_.getFileTypes(configName, configVersion, conddescription, processing, evt, production)
    if retVal['OK']:
      records = []
      parameters = ['FileTypes']
      for record in retVal['Value']:
        records += [[record[0]]]
      return S_OK({'ParameterNames':parameters,'Records':records,'TotalRecords':len(records)})
    else:
      return retVal