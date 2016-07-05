"""
stores the replica readed from an xml
"""

from DIRAC                                                    import gLogger

__RCSID__ = "$Id$"

class Replica:
  """
  Replica class
  """

  #############################################################################
  def __init__( self ):
    """initialize the class members"""
    self.params_ = []
    self.fileName_ = ""

  #############################################################################
  def addParam( self, param ):
    """sets the parameters"""
    self.params_ += [param]

  #############################################################################
  def getaprams( self ):
    """returns the list of parameters"""
    return self.params_

  #############################################################################
  def getFileName( self ):
    """returns the file name"""
    return self.fileName_

  #############################################################################
  def setFileName( self, name ):
    """sets the file name"""
    self.fileName_ = name

  #############################################################################
  def __repr__( self ):
    """It idents the print output"""
    result = "\nReplica: "
    result += self.fileName_ + "\n"
    for param in self.params_:
      result += str( param )

    return result

  #############################################################################
  def writeToXML( self ):
    """writs an XML file"""
    gLogger.info( "Replica XML writing!!!" )
    result = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE Replicas SYSTEM "book.dtd">
<Replicas>
"""
    for param in self.getaprams():
      result += param.writeToXML( False )

    result += '</Replicas>'
    return result
  #############################################################################
