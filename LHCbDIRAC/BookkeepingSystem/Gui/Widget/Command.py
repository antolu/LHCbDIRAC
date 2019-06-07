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
Interfcae of the history browser command
"""


__RCSID__ = "$Id$"

########################################################################
class Command:
  """Command inteface"""
  def __init__(self):
    pass
  ########################################################################
  def execute(self):
    """must be reimplemented"""
    pass

  ########################################################################
  def unexecute(self):
    """must be reimplemented"""
    pass
