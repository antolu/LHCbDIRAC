########################################################################
# $Id: BookkeepingManagerHandler.py,v 1.14 2008/02/04 17:58:49 zmathe Exp $
########################################################################

""" BookkeepingManaher service is the front-end to the Bookkeeping database 
"""

__RCSID__ = "$Id: BookkeepingManagerHandler.py,v 1.14 2008/02/04 17:58:49 zmathe Exp $"

from types import *
from DIRAC.Core.DISET.RequestHandler import RequestHandler
from DIRAC import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Service.copyFiles import copyXMLfile
import time,sys,os

ToDoPath = "/opt/bookkeeping/XMLProcessing/ToDo"

def initializeBookkeepingManagerHandler( serviceInfo ):
  """ Put here necessary initializations needed at the service start
  """
  return S_OK()
    
class BookkeepingManagerHandler(RequestHandler):

  ###########################################################################
  # types_<methodname> global variable is a list which defines for each exposed 
  # method the types of its arguments, the argument types are ignored if the list is empty.

  types_echo = [StringType]
  def export_echo(self,input):
    """ Echo input to output
    """
    return S_OK(input)

  types_sendBookkeeping = [StringType, StringType]
  def export_sendBookkeeping(self, name, data):
      """
      This method send XML file to the ToDo directory
      """
      try:
          stamp = time.strftime('%Y.%m.%d-%H.%M.%S',time.gmtime())
          
          fileID=int(repr(time.time()).split('.')[1])
          
          filePath ="%s%s.%08d.%s"%("/opt/bookkeeping/XMLProcessing/ToDo"+os.sep, stamp, fileID, name)  
          print "--------------------",filePath
          update_file = open(filePath, "w")
          print >>update_file, data
          update_file.close()
          #copyXML(filePath)
          return S_OK()
      except Exception, x:
          print str(x)
          return S_ERROR('Error during processing '+name)

