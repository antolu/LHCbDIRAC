########################################################################
# $Id: JobOption.py 54098 2012-07-02 16:43:53Z zmathe $
########################################################################

"""
stores the job options
"""

__RCSID__ = "$Id$"

class JobOption:
  """
  JobOption class
  """

  #############################################################################
  def __init__(self):
    """iniztialize the class members"""
    self.recipient_ = ""
    self.name_ = ""
    self.value_ = ""

  #############################################################################
  def setRecipient(self, recipient):
    """sets the recipient"""
    self.recipient_ = recipient

  #############################################################################
  def getRecipient(self):
    """returns the recipient"""
    return self.recipient_

  #############################################################################
  def setName(self, name):
    """sets the name"""
    self.name_ = name

  #############################################################################
  def getName(self):
    """returns the name"""
    return self.name_

  #############################################################################
  def setValue(self, value):
    """sets the value"""
    self.value_ = value

  #############################################################################
  def getValue(self):
    """returns the value"""
    return self.value_

  #############################################################################
  def __repr__(self):
    """formats the output of the print"""
    result = '\nJobOption: \n' + self.name_ + ' ' + self.value_ + ' ' + self.recipient_
    return result

  #############################################################################

