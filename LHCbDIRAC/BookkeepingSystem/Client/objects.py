"""
reimplementation of the dictionary
"""
########################################################################
# $Id: objects.py 69359 2013-08-08 13:57:13Z phicharp $
########################################################################
from DIRAC                                                               import gLogger
from LHCbDIRAC.BookkeepingSystem.Client                                  import IndentMaker
from UserDict                                                            import UserDict

import types

global VERBOSE
VERBOSE = True

__RCSID__ = "$Id: objects.py 69359 2013-08-08 13:57:13Z phicharp $"
#############################################################################
class odict(UserDict):
  """user defined dictionary"""
  #############################################################################
  def __init__(self, dict=None):
    """initialize"""
    self._keys = []
    UserDict.__init__(self, dict)

  #############################################################################
  def __delitem__(self, key):
    """delete"""
    UserDict.__delitem__(self, key)
    self._keys.remove(key)

  #############################################################################
  def __setitem__(self, key, item):
    """set"""
    UserDict.__setitem__(self, key, item)
    if key not in self._keys: self._keys.append(key)

  #############################################################################
  def clear(self):
    """clear"""
    UserDict.clear(self)
    self._keys = []

  #############################################################################
  def copy(self):
    """copy"""
    dict = UserDict.copy(self)
    dict._keys = self._keys[:]
    return dict

  #############################################################################
  def items(self):
    """items"""
    return zip(self._keys, self.values())

  #############################################################################
  def keys(self):
    """keys"""
    return self._keys

  #############################################################################
  def popitem(self):
    """popitem"""
    try:
      key = self._keys[-1]
    except IndexError:
      raise KeyError('dictionary is empty')

    val = self[key]
    del self[key]

    return (key, val)

  #############################################################################
  def setdefault(self, key, failobj=None):
    """default value"""
    UserDict.setdefault(self, key, failobj)
    if key not in self._keys:
      self._keys.append(key)

  #############################################################################
  def update(self, dict):
    """update"""
    UserDict.update(self, dict)
    for key in dict.keys():
      if key not in self._keys:
        self._keys.append(key)

  #############################################################################
  def values(self):
    """values"""
    return map(self.get, self._keys)



############################################################################
class Entity(dict):
  """Entity class"""
  #############################################################################
  def __init__(self, properties={}):
    """initialize an Entity"""
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
#
#  def __repr__(self):
#    if len(self) == 0 :
#      s = "{\n " + str(None) + "\n}"
#    else:
#      s  = "{"
#      keys = self.keys()
#      for key in keys:
#        #if key == 'fullpath':
#          s += "\n " + str(key) + " : "
#          value = self[key]
#
#          if isinstance(value, types.DictType):
#            value = Entity(value)
#            s += "\n" + IndentMaker.prepend(str(value), (len(str(key))+3)*" ")
#            #childrenString += str(Entity(child)) + "\n"
#          else:
#             s +=  str(value)
#          s += "\n}"
#     #        s = IndentMaker.prepend(s, "_______")
#    return s
#
  def __repr__(self):
    """print """
    if len(self) == 0 :
      string = "{\n " + str(None) + "\n}"
    else:
      string = "{"
      keys = self.keys()
      if 'fullpath' in keys:
        string += '\n' + 'fullpath: ' + str(self['fullpath'])
      if VERBOSE:
        for key in keys:
          if key != 'name' and key != 'level' and key != 'fullpath' \
          and key != 'expandable' and key != 'selection'\
          and key != 'method' and key != 'showFiles':
            string += "\n " + str(key) + " : "
            value = self[key]
            # some entities do not have this key. Ignore then.
            try:
              if key in self['not2show']:
                string += '-- not shown --'
                continue
            except Exception, ex:
              pass
            if isinstance(value, types.DictType):
              value = Entity(value)
              string += "\n" + IndentMaker.prepend(str(value), (len(str(key)) + 3) * " ")
            #childrenString += str(Entity(child)) + "\n"
            else:
              string += str(value)
      else:
        for key in keys:
          if key != 'name' and key != 'fullpath' and  key == 'FileName':
            value = self[key]
            if isinstance(value, types.DictType):
              value = Entity(value)
              string += "\n" + IndentMaker.prepend(str(value), (len(str(key)) + 3) * " ")
            else:
              string += str(value)

      string += "\n}"
  #        string = IndentMaker.prepend(string, "_______")
    return string

