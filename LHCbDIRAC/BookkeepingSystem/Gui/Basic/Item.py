"""used to store a tree structure"""
########################################################################
# $Id: Item.py 54002 2012-06-29 09:01:26Z zmathe $
########################################################################


__RCSID__ = "$Id: Item.py 54002 2012-06-29 09:01:26Z zmathe $"
#############################################################################
class Item(dict):
  """Item class"""
  #############################################################################
  def __init__(self, properties, parent):
    """ initializes the class member"""
    super(Item, self).__init__(properties)
    self.__parent = parent
    self.__children = {}
    self.__childrenNumber = 0
    self.__childmap = {}
  #############################################################################
  def getParent(self):
    """returns the parent of a node"""
    return self.__parent

  #############################################################################
  def addItem(self, item):
    """adds an item to the node"""
    name = item['name']
    self.__children[name] = item
    self.__childmap[self.__childrenNumber] = item
    self.__childrenNumber = self.__childrenNumber + 1


  #############################################################################
  def getChildren(self):
    """returns the children"""
    return self.__children

  #############################################################################
  def child(self, i):
    """retuns the i-th csild"""
    return self.__childmap[i]

  #############################################################################
  def name(self):
    """returns the name of the node"""
    return self['name']

  #############################################################################
  def childnum(self):
    """returns the number of childs"""
    return self.__childrenNumber

  #############################################################################
  def expandable(self):
    """is the node has children"""
    return self['expandable']

  #############################################################################
