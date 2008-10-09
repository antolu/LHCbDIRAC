########################################################################
# $Id: objects.py,v 1.9 2008/10/09 17:37:10 zmathe Exp $
########################################################################

"""
"""
from DIRAC                                                           import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Client                                  import IndentMaker
from UserDict                                                        import UserDict

import types



__RCSID__ = "$Id: objects.py,v 1.9 2008/10/09 17:37:10 zmathe Exp $"
#############################################################################
class odict(UserDict):
  
  #############################################################################
  def __init__(self, dict = None):
    self._keys = []
    UserDict.__init__(self, dict)

  #############################################################################
  def __delitem__(self, key):
    UserDict.__delitem__(self, key)
    self._keys.remove(key)

  #############################################################################
  def __setitem__(self, key, item):
    UserDict.__setitem__(self, key, item)
    if key not in self._keys: self._keys.append(key)

  #############################################################################
  def clear(self):
    UserDict.clear(self)
    self._keys = []
  
  #############################################################################
  def copy(self):
    dict = UserDict.copy(self)
    dict._keys = self._keys[:]
    return dict
  
  #############################################################################
  def items(self):
    return zip(self._keys, self.values())

  #############################################################################
  def keys(self):
    return self._keys
  
  #############################################################################
  def popitem(self):
    try:
      key = self._keys[-1]
    except IndexError:
      raise KeyError('dictionary is empty')

    val = self[key]
    del self[key]

    return (key, val)
  
  #############################################################################
  def setdefault(self, key, failobj = None):
    UserDict.setdefault(self, key, failobj)
    if key not in self._keys: self._keys.append(key)

  #############################################################################
  def update(self, dict):
    UserDict.update(self, dict)
    for key in dict.keys():
      if key not in self._keys: self._keys.append(key)
  
  #############################################################################
  def values(self):
    return map(self.get, self._keys)



############################################################################
class Entity(dict):
  
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
      gLogger.warn("Cannot create Entity from properties:" + str(properties))
            
  #############################################################################
  """
  def __repr__(self):
    if len(self) == 0 :
      s = "{\n " + str(None) + "\n}"
    else:
      s  = "{"
      keys = self.keys()
      for key in keys:
        #if key == 'fullpath':    
          s += "\n " + str(key) + " : "
          value = self[key]

          if isinstance(value, types.DictType):
            value = Entity(value)
            s += "\n" + IndentMaker.prepend(str(value), (len(str(key))+3)*" ")
            #childrenString += str(Entity(child)) + "\n"
          else:
             s +=  str(value)
          s += "\n}"                
     #        s = IndentMaker.prepend(s, "_______")                
    return s
  """
  def __repr__(self):
    if len(self) == 0 :
        s = "{\n " + str(None) + "\n}"
    else:
        s  = "{"
        keys = self.keys()
        if 'fullpath' in keys:
          s += '\n' + 'fullpath: ' + str(self['fullpath'])
        for key in keys:
          if key != 'name' and key != 'fullpath':
            s += "\n " + str(key) + " : "
            value = self[key]
            # some entities do not have this key. Ignore then.
            try:
                if key in self['not2show']:
                    s +=  '-- not shown --'
                    continue
            except:
                pass
            if isinstance(value, types.DictType):
                value = Entity(value)
                s += "\n" + IndentMaker.prepend(str(value), (len(str(key))+3)*" ")
            #childrenString += str(Entity(child)) + "\n"
            else:
                s +=  str(value)
        s += "\n}"                
  #        s = IndentMaker.prepend(s, "_______")                
    return s   
