########################################################################
# $Id$
########################################################################

__RCSID__ = "$Id$"

#############################################################################
class Message(dict):

  #############################################################################
  def __init__(self, message):
    super(Message, self).__init__(message)

  #############################################################################
  def action(self):
    return self['action']