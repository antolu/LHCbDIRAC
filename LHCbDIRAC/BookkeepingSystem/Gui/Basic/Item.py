########################################################################
# $Id: Item.py 18161 2009-11-11 12:07:09Z acasajus $
########################################################################


__RCSID__ = "$Id: Item.py 18161 2009-11-11 12:07:09Z acasajus $"
#############################################################################  
class Item(dict):
  
  #############################################################################  
  def __init__(self, properties = {}):
    #odict.__init__(self)
    if isinstance(properties, types.ListType):
      for key in properties:
        self[key] = None  # find a simpler way to declare all keys
    elif isinstance(properties, type(odict)):
      if not (len(properties) == 0):
        self.update(properties)
    elif isinstance(properties, types.DictType):
      if not (len(properties) == 0):
        self.update(properties.items())
    else:
      gLogger.warn("Cannot create Item from properties:" + str(properties))
    self.__children={}
    self.__childrenNumber = 0
    self.__parent = {}  
    self.__childmap = {}
  
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
