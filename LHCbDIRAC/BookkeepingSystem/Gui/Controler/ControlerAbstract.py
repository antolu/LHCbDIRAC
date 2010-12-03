########################################################################
# $Id: ControlerAbstract.py 18175 2009-11-11 14:02:19Z zmathe $
########################################################################

__RCSID__ = "$Id: ControlerAbstract.py 18175 2009-11-11 14:02:19Z zmathe $"

#############################################################################  
class ControlerAbstract(object):
  
  #############################################################################  
  def __init__(self, widget, parent):
    self.__widget = widget
    self.__children = {}
    self.__parent = parent
  
  #############################################################################  
  def getWidget(self):
    return self.__widget
  
  #############################################################################  
  def messageFromParent(self, message):
    pass
   
  #############################################################################   
  def messageFromChild(self, sender, message):
    pass
  
  #############################################################################  
  def addChild(self, name, child):
    self.__children[name]=child
  
  #############################################################################  
  def removeChild(self, name):
    del self.__children[name]
  
  #############################################################################  
  def getChildren(self):
    return self.__children
  
  #############################################################################  
  def getParent(self):
    return self.__parent 
  
  #############################################################################  
  def setParent(self, parent):
    self.__parent = parent
  
    