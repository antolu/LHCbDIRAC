########################################################################
# $Id$
########################################################################


__RCSID__ = "$Id$"
#############################################################################
class Item(dict):

  #############################################################################
  def __init__(self, properties, parent):
    super(Item, self).__init__(properties)
    self.__parent = parent
    self.__children = {}
    self.__childrenNumber = 0
    self.__childmap = {}
  #############################################################################
  def getParent(self):
    return self.__parent

  #############################################################################
  def addItem(self, item):
    name = item['name']
    self.__children[name] = item
    self.__childmap[self.__childrenNumber] = item
    self.__childrenNumber = self.__childrenNumber + 1


  #############################################################################
  def getChildren(self):
    return self.__children

  #############################################################################
  def child(self, i):
    return self.__childmap[i]

  #############################################################################
  def name(self):
    return self['name']

  #############################################################################
  def childnum(self):
    return self.__childrenNumber

  #############################################################################
  def expandable(self):
    return self['expandable']

  #############################################################################
