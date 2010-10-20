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