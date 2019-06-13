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
########################################################################
# File:  StorageUsageClient.py
########################################################################
""" :mod: StorageUsageClient
    ========================

    .. module: StorageUsageClient
    :synopsis: Lightweight possbile client to the StorageUsageDB.
"""

__RCSID__ = "$Id$"

from DIRAC.Core.Base.Client import Client


class StorageUsageClient(Client):
  """
  .. class:: StorageUsageClient
  """
  def __init__(self, url=None, **kwargs):
    """ c'tor """
    Client.__init__(self, **kwargs)
    if url:
      self.setServer(url)
    self.setServer('DataManagement/StorageUsage')
