########################################################################
# $Id: File.py,v 1.5 2008/07/02 09:49:11 zmathe Exp $
########################################################################

"""

"""

from DIRAC                                                                  import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: File.py,v 1.5 2008/07/02 09:49:11 zmathe Exp $"

class File:


  #############################################################################  
  def __init__(self):
    self.name_ = ""
    self.type_ = ""
    self.typeID_ = -1
    self.version_ = ""
    self.params_ = []
    self.replicas_ = []
    self.qualities_ = []
    self.fileID_ = -1
  
  #############################################################################  
  def setFileID(self, id):
    self.fileID_ = id
  
  #############################################################################  
  def getFileID(self):
    return self.fileID_
  
  #############################################################################  
  def setFileName(self, name):
    self.name_ = name

  #############################################################################  
  def getFileName(self):
    return self.name_
  
  #############################################################################  
  def setFileVersion(self, version):
    self.version_ = version
      
  #############################################################################  
  def getFileVersion(self):
    return self.version_
  
  #############################################################################  
  def setFileType(self, type):
    self.type_ = type
      
  #############################################################################  
  def getFileType(self):
    return self.type_

  #############################################################################  
  def addFileParam(self, param):
    self.params_ += [param]
      
  #############################################################################  
  def getFileParams(self):
    return self.params_
  
  #############################################################################  
  def setTypeID(self, id):
    self.typeID_ = id
  
  #############################################################################  
  def getTypeID(self):
    return self.typeID_
  
  #############################################################################  
  def addReplicas(self, replica):
    self.replicas_ += [replica]
  
  #############################################################################  
  def getReplicas(self):
    return self.replicas_
  
  #############################################################################  
  def addQuality(self, quality):
    self.qualities_ += [quality]
  
  #############################################################################
  def getQualities(self):
    return self.qualities_
  
  #############################################################################  
  def __repr__(self):
    result = '\n File : \n'
    result += self.name_ + ' ' + self.version_ + ' ' + self.type_
      
    for param in self.params_:
      result += str(param)
          
    return result
  
  #############################################################################  
  def writeToXML(self):
    s = '  <OutputFile   Name="'+self.getFileName()+'" TypeName="'+self.getFileType()+'" TypeVersion="'+self.getFileVersion()+'">\n'
    
    for replica in self.getReplicas():
      s += replica.writeToXML()
    
    for param in self.getFileParams():
      s += param.writeToXML()
    """
    for param in self.getQualities():
      s += param.writeToXML()
    """  
    s += '  </OutputFile>\n'
    
    return s
  #############################################################################  

        