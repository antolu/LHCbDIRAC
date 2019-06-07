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
"""interface for the database"""

from LHCbDIRAC.BookkeepingSystem.DB.IBookkeepingDatabaseClient             import IBookkeepingDatabaseClient
from LHCbDIRAC.BookkeepingSystem.DB.OracleBookkeepingDB                    import OracleBookkeepingDB

__RCSID__ = "$Id$"

class BookkeepingDatabaseClient( IBookkeepingDatabaseClient ):
  """simple class"""
  #############################################################################
  def __init__(self, databaseManager=OracleBookkeepingDB()):
    super(BookkeepingDatabaseClient, self).__init__(databaseManager)

