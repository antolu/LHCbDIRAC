########################################################################
# $Id$
########################################################################

"""

"""

from DIRAC                                                    import gLogger

__RCSID__ = "$Id$"

class Replica:


  #############################################################################
  def __init__( self ):
      self.params_ = []
      self.fileName_ = ""

  #############################################################################
  def addParam( self, param ):
      self.params_ += [param]

  #############################################################################
  def getaprams( self ):
    return self.params_

  #############################################################################
  def getFileName( self ):
    return self.fileName_

  #############################################################################
  def setFileName( self, name ):
    self.fileName_ = name

  #############################################################################
  def __repr__( self ):
    result = "\nReplica: "
    result += self.fileName_ + "\n"
    for param in self.params_:
        result += str( param )

    return result

  #############################################################################
  def writeToXML( self ):
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
