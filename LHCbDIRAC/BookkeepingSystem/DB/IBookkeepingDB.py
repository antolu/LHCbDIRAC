########################################################################
# $Id: IBookkeepingDB.py 18161 2009-11-11 12:07:09Z acasajus $
########################################################################

"""

"""

__RCSID__ = "$Id: IBookkeepingDB.py 18161 2009-11-11 12:07:09Z acasajus $"

from DIRAC                                      import gLogger, S_OK, S_ERROR

class IBookkeepingDB(object):
  
  #############################################################################
  def __init__(self):
    pass
  
  #############################################################################
  def file(self, fileName):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def fileTypeAndFileTypeVersion(self, type, version):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def eventType(self, eventTypeId):
    gLogger.warn("not implemented")
    return S_ERROR()
  

  #############################################################################
  def insertJob(self, job):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertJobParameter(self, jobID, name, value, type):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertInputFile(self, jobID, inputfile):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertOutputFile(self, jobID, name, value):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertOutputFile(self, job, file):
    gLogger.warn("not implemented")
    return S_ERROR()
    
  #############################################################################
  def insertFileParam(self, fileID, name, value):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertReplica(self, fileID, name, location):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def deleteJob(self, job):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def deleteFile(self, file):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertQuality(self, fileID, group, flag ):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertQualityParam(self, fileID, qualityID, name, value):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def modifyReplica(self, fileID , name, value):
    gLogger.warn("not implemented")
    return S_ERROR()
  
  