########################################################################
# $Id: Message.py,v 1.1 2008/09/25 15:50:32 zmathe Exp $
########################################################################

__RCSID__ = "$Id: Message.py,v 1.1 2008/09/25 15:50:32 zmathe Exp $"

#############################################################################  
class Message(dict):
  
  #############################################################################  
  def __init__(self, message):
    super(Message, self).__init__(message)
  
  #############################################################################  
  def action(self):
    return self['action']
  
  #############################################################################  