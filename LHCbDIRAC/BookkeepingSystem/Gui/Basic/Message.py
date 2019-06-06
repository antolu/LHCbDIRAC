###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
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