"""
This Message used by the controllers to deliver information
"""
########################################################################
# $Id: Message.py 54002 2012-06-29 09:01:26Z zmathe $
########################################################################

__RCSID__ = "$Id: Message.py 54002 2012-06-29 09:01:26Z zmathe $"

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