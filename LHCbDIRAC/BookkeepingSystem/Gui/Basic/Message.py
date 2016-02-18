"""
This Message used by the controllers to deliver information
"""

__RCSID__ = "$Id$"

#############################################################################
class Message(dict):
  """Message class"""
  #############################################################################
  def __init__(self, message):
    """inherits from the dictionary"""
    dict.__init__(self, message)

  #############################################################################
  def action(self):
    """action be performed on the views"""
    return self['action']