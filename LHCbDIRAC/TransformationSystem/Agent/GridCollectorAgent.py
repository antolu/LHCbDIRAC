# """ GridCollectorAgent gets metadata from events stored in input file and saves it into outputfile.
#     This metadata is generated at times of stripping and used for indexing events in those files.
# """
#
# import os
# import json
# from DIRAC import S_OK, S_ERROR
# from DIRAC import gLogger
# from DIRAC.Core.Base.AgentModule import AgentModule
# #TO-DO: use lb-run (RunApplication module)
#
# from grid_collector.status_db import StatusDB #pylint: disable=F0401
# from grid_collector.event_index_request import EventIndexRequest #pylint: disable=F0401
#
# __RCSID__ = "$Id$"
#
# CONFIG_PATH = "/home/dirac/eindex_3/grid_collector/grid_collector/lbvobox27_config.json"
#
# class GridCollectorAgent( AgentModule ):
#   def __init__( self, *args, **kwargs ):
#     with open(CONFIG_PATH) as config_file:
#       self.config = json.load(config_file)
#     AgentModule.__init__( self, *args, **kwargs )
#     self.status_db = StatusDB(self.config['StatusDB_file_name'])
#
#   def initialize(self):
#     self.am_setOption('shifterProxy', 'DataManager')
#     return S_OK()
#
#
#   def execute(self):
#     request_tuple = self.status_db.pull_new_request()
#     if not request_tuple:
#       return S_OK()
#     gLogger.info("Starting processing request %s" % str(request_tuple.uuid))
#     self.status_db.set_status( request_tuple.uuid,
#                                self.status_db.STATUS_RUNNING,
#                                "Running SetupProject")
#     da_vinci_environment = getProjectEnvironment( self.config['CMTConfig'],
#                                                   self.config['da_vinci_version'],
#                                                   'gfal CASTOR lfc',
#                                                   env=self.config['SetupProject_env'])
#     if not da_vinci_environment or \
#            'OK' not in da_vinci_environment or \
#            not da_vinci_environment['OK'] or \
#            'Value' not in da_vinci_environment or \
#            not da_vinci_environment['Value']:
#       self.status_db.set_status( request_tuple.uuid,
#                                  self.status_db.STATUS_FAIL,
#                                  "Failed to SetupProject")
#       return S_ERROR()
#     try:
#       request = EventIndexRequest( os.path.join(self.config["requests_folder"], "%s.json" % request_tuple.uuid),
#                                    request_tuple.uuid,
#                                    da_vinci_environment['Value'],
#                                    self.config,
#                                    20 )
#       request.process_event_index_request()
#     except:
#       if self.status_db.get_status( request_tuple.uuid ).short != self.status_db.STATUS_FAIL:
#         self.status_db.set_status( request_tuple.uuid, self.status_db.STATUS_FAIL,
#                                    "grid_collector.event_index_request unhandled failure")
#       raise
#     return S_OK()
