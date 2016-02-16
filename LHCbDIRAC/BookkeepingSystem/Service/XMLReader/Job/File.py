########################################################################
# $Id: File.py 54098 2012-07-02 16:43:53Z zmathe $
########################################################################

"""
stores a file
"""

__RCSID__ = "$Id: File.py 54098 2012-07-02 16:43:53Z zmathe $"

class File:
  """
  File class
  """

  #############################################################################
  def __init__(self):
    """initialize the class members"""
    self.name_ = ""
    self.type_ = ""
    self.typeID_ = -1
    self.version_ = ""
    self.params_ = []
    self.replicas_ = []
    self.qualities_ = []
    self.fileID_ = -1

  #############################################################################
  def setFileID(self, fileid):
    """sets the file identifier"""
    self.fileID_ = fileid

  #############################################################################
  def getFileID(self):
    """returns the file identifier"""
    return self.fileID_

  #############################################################################
  def setFileName(self, name):
    """sets the file name"""
    self.name_ = name

  #############################################################################
  def getFileName(self):
    """returns the file name"""
    return self.name_

  #############################################################################
  def setFileVersion(self, version):
    """sets the file format"""
    self.version_ = version

  #############################################################################
  def getFileVersion(self):
    """returns the file format"""
    return self.version_

  #############################################################################
  def setFileType(self, filetype):
    """sets the file type"""
    self.type_ = filetype

  #############################################################################
  def getFileType(self):
    """returns the file type"""
    return self.type_

  #############################################################################
  def addFileParam(self, param):
    """adds a file parameter"""
    self.params_ += [param]

  #############################################################################
  def exists(self, fileParam):
    """checks a given parameter"""
    ok = False
    for i in self.params_:
      if i.getParamName() == fileParam:
        ok = True
    return ok

  #############################################################################
  def getParam(self, fileParam):
    """returns the file parameters"""
    param = None
    for i in self.params_:
      if i.getParamName() == fileParam:
        param = i
    return param

  #############################################################################
  def removeFileParam(self, param):
    """removes a file parameter"""
    self.params_.remove(param)

  #############################################################################
  def getFileParams(self):
    """returns the file parameters"""
    return self.params_

  #############################################################################
  def setTypeID(self, typeid):
    """set the file type identifier"""
    self.typeID_ = typeid

  #############################################################################
  def getTypeID(self):
    """returns the type identifier"""
    return self.typeID_

  #############################################################################
  def addReplicas(self, replica):
    """adds a replicas"""
    self.replicas_ += [replica]

  #############################################################################
  def getReplicas(self):
    """returns the replicas"""
    return self.replicas_

  #############################################################################
  def addQuality(self, quality):
    """adds the data quality"""
    self.qualities_ += [quality]

  #############################################################################
  def getQualities(self):
    """returns the data quality"""
    return self.qualities_

  #############################################################################
  def __repr__(self):
    """formats the output of print"""
    result = '\n File : \n'
    result += self.name_ + ' ' + self.version_ + ' ' + self.type_

    for param in self.params_:
      result += str(param)

    return result

  #############################################################################
  def writeToXML(self):
    """creates an xml string"""
    string = "  <OutputFile   Name='%s' TypeName='%s' TypeVersion='%s'>\n" % (self.getFileName(),
                                                                              self.getFileType(),
                                                                              self.getFileVersion()
                                                                              )

    for replica in self.getReplicas():
      string += replica.writeToXML()

    for param in self.getFileParams():
      string += param.writeToXML()

#    for param in self.getQualities():
#      string += param.writeToXML()

    string += '  </OutputFile>\n'

    return string
  #############################################################################

