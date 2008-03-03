########################################################################
# $Id: IBookkeepingDB.py,v 1.2 2008/03/03 09:57:11 zmathe Exp $
########################################################################

"""

"""

__RCSID__ = "$Id: IBookkeepingDB.py,v 1.2 2008/03/03 09:57:11 zmathe Exp $"

from DIRAC                                      import gLogger, S_OK, S_ERROR

class IBookkeepingDB(object):
  
  #############################################################################
  def __init__(self):
    pass
  
  #############################################################################
  def file(self, fileName):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def fileTypeAndFileTypeVersion(self, type, version):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def eventType(self, eventTypeId):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertJob(self, jobName, jobConfVersion, date):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertJobParameter(self, jobID, name, value, type):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertInputFile(self, jobID, inputfile):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertOutputFile(self, jobID, name, value):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertFileParam(self, fileID, name, value):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertReplica(self, fileID, name, location):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def deleteJob(self, job):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def deleteFile(self, file):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertQuality(self, fileID, group, flag ):
    gLoogger.warn("not implemented")
    return S_ERROR()
  
  #############################################################################
  def insertQualityParam(self, fileID, qualityID, name, value):
    gLoogger.warn("not implemented")
    return S_ERROR()
 
  #############################################################################