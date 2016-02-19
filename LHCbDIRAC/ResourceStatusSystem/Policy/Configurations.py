''' LHCbDIRAC.ResourceStatusSystem.Policy.Configurations

   POLICIESMETA_LHCB
   policies
   POLICIESMETA

'''

from DIRAC.ResourceStatusSystem.Policy.Configurations import POLICIESMETA

__RCSID__ = "$Id$"

#...............................................................................
# LHCb Policies

POLICIESMETA_LHCB = {'GGUSTickets' : {'description' : "Open GGUS tickets",
                                      'module'      : 'GGUSTicketsPolicy',
                                      'command'     : ( 'GGUSTicketsCommand', 'GGUSTicketsCommand' ),
                                      'args'        : { 'onlyCache' : False }},
                     'TransferQualitySource' :{'description' : 'Transfers from element, quality measure',
                                               'module'      : 'TransferQualityPolicy',
                                               'command'     : ( 'TransferCommand', 'TransferCommand' ),
                                               'args'        : {'direction' : 'Source',
                                                                'metric' : 'Quality',
                                                                'hours' : 2 }},
                     'TransferQualityDestination' : {'description' : 'Transfers to element, quality measure',
                                                     'module'      : 'TransferQualityPolicy',
                                                     'command'     : ( 'TransferCommand', 'TransferCommand' ),
                                                     'args'        : {'direction' : 'Destination', 'metric' :
                                                                      'Quality', 'hours' : 2 }},
#  'PilotEfficiency' :
#    {
#      'description'     : 'Pilots efficiency extracted from WMS',
#      'module'          : 'PilotEfficiencyPolicy',
#      'command'         : ( 'PilotCommand', 'PilotCommand' ),
#      'args'            : None,
#    }
}

################################################################################
# Old stuff

policies = { 'DTScheduled' : {'description' : 'Ongoing and scheduled down-times',
                              'module' : 'DTPolicy',
                              'commandInNewRes' : ( 'GOCDBStatusCommand', 'GOCDBStatusCommand' ),
                              # 'command'         : ( 'GOCDBStatusCommand', 'DTCachedCommand' ),
                              'command'         : ( 'GOCDBStatusCommand', 'GOCDBStatusCommand' ),
                              'args'            : { 'hours' : 12 },  # Fix to avoid querying the CS on load time, to be fixed
                              'Site_Panel' : [ {'WebLink': {'CommandIn': ( 'GOCDBStatusCommand',
                                                                           'DTInfoCachedCommand' ),
                                                            'args': None}}],
                              'Resource_Panel' : [ {'WebLink': {'CommandIn': ( 'GOCDBStatusCommand',
                                                                               'DTInfoCachedCommand' ),
                                                                'args': None}}]},
            'GGUSTickets' : {'description' : 'Open GGUS tickets',
                             'module'      : 'GGUSTicketsPolicy',
                             'command'     : ( 'GGUSTicketsCommand', 'GGUSTicketsOpen' ),
                             'args'        : None,
                             'Site_Panel' : [ {'WebLink': {'CommandIn': ( 'GGUSTicketsCommand', 'GGUSTicketsLink' ),
                                                           'args': None}},
                                             {'TextInfo': {'CommandIn': ( 'GGUSTicketsCommand', 'GGUSTicketsInfo' ),
                                                           'args': None}}]},
            'SAM_CE' :{'description' : 'Latest SAM results on the LCG Computing Element',
                       'module'      : 'SAMResultsPolicy',
                       'command'     : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
                       'args'        : ( None, ['LHCb CE-lhcb-availability', 'LHCb CE-lhcb-install',
                                                'LHCb CE-lhcb-job-Boole', 'LHCb CE-lhcb-job-Brunel',
                                                'LHCb CE-lhcb-job-DaVinci', 'LHCb CE-lhcb-job-Gauss',
                                                'LHCb CE-lhcb-os', 'LHCb CE-lhcb-queues', 'bi',
                                                'csh', 'js', 'gfal', 'swdir', 'voms'] ),
                       'Resource_Panel' : [ {'SAM': {'CommandIn':( 'SAMResultsCommand', 'SAMResultsCommand' ),
                                                     'args': ( None, ['LHCb CE-lhcb-availability',
                                                                      'LHCb CE-lhcb-install',
                                                                      'LHCb CE-lhcb-job-Boole',
                                                                      'LHCb CE-lhcb-job-Brunel',
                                                                      'LHCb CE-lhcb-job-DaVinci',
                                                                      'LHCb CE-lhcb-job-Gauss',
                                                                      'LHCb CE-lhcb-os', 'LHCb CE-lhcb-queues',
                                                                      'LHCb CE-lhcb-queues',
                                                                      'bi', 'csh', 'js', 'gfal', 'swdir', 'voms'] ) }}]},
            'SAM_CREAMCE' :{'description' : 'Latest SAM results on the CREAM Computing Element',
                            'module'      : 'SAMResultsPolicy',
                            'command'     : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
                            'args'        : ( None, [ 'bi', 'csh', 'gfal', 'swdir', 'creamvoms' ] ),
                            'Resource_Panel' : [ { 'SAM':
                                                  { 'CommandIn' : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
                                                   'args'      : ( None, [ 'bi', 'csh', 'gfal', 'swdir', 'creamvoms' ] )}}, ]},
            'SAM_SE' :{'description' : 'Latest SAM results on the SRM nodes',
                       'module'      : 'SAMResultsPolicy',
                       'command'     : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
                       'args'        : ( None, [ 'DiracTestUSER', 'FileAccessV2', 'LHCb-cr' ] ),
                       'Resource_Panel' : [ {'SAM': {'CommandIn':( 'SAMResultsCommand', 'SAMResultsCommand' ),
                                                     'args': ( None, ['DiracTestUSER', 'FileAccessV2', 'LHCb-cr'] ) }}, ]},
            'SAM_LFC_C' :{'description' : 'Latest SAM results on the central LFC nodes',
                          'module'      : 'SAMResultsPolicy',
                          'command'     : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
                          'args'        : ( None, [ 'lfcwf', 'lfclr', 'lfcls', 'lfcping' ] ),
                          'Resource_Panel' : [ { 'SAM': { 'CommandIn' : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
                                                         'args'      : ( None, [ 'lfcwf', 'lfclr', 'lfcls', 'lfcping' ] )}}, ]},
            'SAM_LFC_L' :{'description' : 'Latest SAM results on the slave LFC nodes',
                          'module'      : 'SAMResultsPolicy',
                          'command'     : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
                          'args'        : ( None, [ 'lfcstreams', 'lfclr', 'lfcls', 'lfcping' ] ),
                          'Resource_Panel' : [ { 'SAM': { 'CommandIn':( 'SAMResultsCommand', 'SAMResultsCommand' ),
                                                         'args'     : ( None, [ 'lfcstreams', 'lfclr', 'lfcls', 'lfcping' ] )}}, ]},
            'SAM_FTS' :{'description' : 'Latest SAM results on the FTS nodes',
                        'module'      : 'SAMResultsPolicy',
                        'command'     : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
                        'args'        : ( None, [ 'ftschn', 'ftsinfo' ] ),
                        'Resource_Panel' : [ { 'SAM': {'CommandIn':( 'SAMResultsCommand', 'SAMResultsCommand' ),
                                                       'args': ( None, ['ftschn', 'ftsinfo'] ) }}, ]},
#   'JobsEfficiencySimple' :
#     {
#       'description'     : 'Simple jobs efficiency',
#       'module'          : 'JobsEfficiencySimplePolicy',
#       'commandInNewRes' : ( 'JobsCommand', 'JobsEffSimpleCommand' ),
#       'command'         : ( 'JobsCommand', 'JobsEffSimpleCachedCommand' ),
#       'args'            : None,
#
#       'Service_Computing_Panel' : [ {'FillChart - Successfull Jobs in the last 24 hours':
#                                      {'CommandIn': ( 'DIRACAccountingCommand', 'CachedPlotCommand' ),
#                                       'args':( 'Job', 'SuccessfullJobsBySiteSplitted_24' ),
#                                       'CommandInNewRes': ( 'DIRACAccountingCommand', 'DIRACAccountingCommand' ),
#                                       'argsNewRes': ( 'Job', 'NumberOfJobs',
#                                                      {'Format': 'LastHours', 'hours': 24},
#                                                      'Site', {'FinalMajorStatus':'Done'} )}},
#                                     {'FillChart - Failed Jobs in the last 24 hours':
#                                      {'CommandIn': ( 'DIRACAccountingCommand', 'CachedPlotCommand' ),
#                                       'args':( 'Job', 'FailedJobsBySiteSplitted_24' ),
#                                       'CommandInNewRes': ( 'DIRACAccountingCommand', 'DIRACAccountingCommand' ),
#                                       'argsNewRes': ( 'Job', 'NumberOfJobs',
#                                                      {'Format': 'LastHours', 'hours': 24},
#                                                      'Site', {'FinalMajorStatus':'Failed'} )}},
#                                     {'FillChart - Running Jobs in the last 24 hours':
#                                      {'CommandIn': ( 'DIRACAccountingCommand', 'CachedPlotCommand' ),
#                                       'args':( 'Job', 'RunningJobsBySiteSplitted_24' ),
#                                       'CommandInNewRes': ( 'DIRACAccountingCommand', 'DIRACAccountingCommand' ),
#                                       'argsNewRes': ( 'WMSHistory', 'NumberOfJobs',
#                                                      {'Format': 'LastHours', 'hours': 24},
#                                                      'Site', {} )}},
#                                     {'FillChart - Running Jobs in the last week':
#                                      {'CommandIn': ( 'DIRACAccountingCommand', 'CachedPlotCommand' ),
#                                       'args':( 'Job', 'RunningJobsBySiteSplitted_168' ),
#                                       'CommandInNewRes': ( 'DIRACAccountingCommand', 'DIRACAccountingCommand' ),
#                                       'argsNewRes': ( 'WMSHistory', 'NumberOfJobs',
#                                                      {'Format': 'LastHours', 'hours': 168},
#                                                      'Site', {} )}},
#                                     {'FillChart - Running Jobs in the last month':
#                                      {'CommandIn': ( 'DIRACAccountingCommand', 'CachedPlotCommand' ),
#                                       'args':( 'Job', 'RunningJobsBySiteSplitted_720' ),
#                                       'CommandInNewRes': ( 'DIRACAccountingCommand', 'DIRACAccountingCommand' ),
#                                       'argsNewRes': ( 'WMSHistory', 'NumberOfJobs',
#                                                      {'Format': 'LastHours', 'hours': 720},
#                                                                'Site', {} )}},
#                                     {'FillChart - Running Jobs in the last year':
#                                      {'CommandIn': ( 'DIRACAccountingCommand', 'CachedPlotCommand' ),
#                                       'args':( 'Job', 'RunningJobsBySiteSplitted_8760' ),
#                                       'CommandInNewRes': ( 'DIRACAccountingCommand', 'DIRACAccountingCommand' ),
#                                       'argsNewRes': ( 'WMSHistory', 'NumberOfJobs',
#                                                      {'Format': 'LastHours', 'hours': 8760},
#                                                      'Site', {} )}}
#                                     ]
#    },
#   'PilotsEfficiencySimple_Service' :
#     {
#       'description'     : 'Simple pilots efficiency',
#       'module'          : 'PilotsEfficiencySimplePolicy',
#       'commandInNewRes' : ( 'PilotsCommand', 'PilotsEffSimpleCommand' ),
#       'command'         : ( 'PilotsCommand', 'PilotsEffSimpleCachedCommand' ),
#       'args'            : None,
#
#       'Service_Computing_Panel' : [ {'FillChart - Successfull pilots in the last 24 hours':
#                                      {'CommandIn': ( 'DIRACAccountingCommand', 'CachedPlotCommand' ),
#                                       'args': ( 'Pilot', 'SuccessfullPilotsBySiteSplitted_24' ),
#                                       'CommandInNewRes' : ( 'DIRACAccountingCommand', 'DIRACAccountingCommand' ),
#                                       'argsNewRes': ( 'Pilot', 'NumberOfPilots',
#                                                      {'Format': 'LastHours', 'hours': 24},
#                                                      'Site', {'GridStatus':'Done'} )}},
#                                     {'FillChart - Aborted pilots in the last 24 hours':
#                                      {'CommandIn': ( 'DIRACAccountingCommand', 'CachedPlotsCommand' ),
#                                       'args': ( 'Pilot', 'FailedPilotsBySiteSplitted_24' ),
#                                       'CommandInNewRes': ( 'DIRACAccountingCommand', 'DIRACAccountingCommand' ),
#                                       'argsNewRes': ( 'Pilot', 'NumberOfPilots',
#                                                      {'Format': 'LastHours', 'hours': 24},
#                                                      'Site', {'GridStatus':'Aborted'} )}},
#                                     ]
#      },
  'PilotsEfficiencySimple_Resource' :{'description' : 'Simple pilots efficiency',
                                      'module'      : 'PilotsEfficiencySimplePolicy',
                                      'command'     : ( 'PilotsCommand', 'PilotsEffSimpleCommand' ),
                                      'args'        : None,
                                      'Resource_Panel' : [ { 'FillChart - Successfull pilots in the last 24 hours':
                                                            {'CommandIn': ( 'DIRACAccountingCommand', 'CachedPlotCommand' ),
                                                             'args':( 'Pilot', 'SuccessfullPilotsByCESplitted_24' ),
                                                             'CommandInNewRes': ( 'DIRACAccountingCommand', 'DIRACAccountingCommand' ),
                                                             'argsNewRes': ( 'Pilot', 'NumberOfPilots',
                                                                             {'Format': 'LastHours', 'hours': 24},
                                                                             'GridCE', {'GridStatus':'Done'} )}},
                                                          { 'FillChart - Failed pilots in the last 24 hours':
                                                           {'CommandIn': ( 'DIRACAccountingCommand', 'CachedPlotCommand' ),
                                                            'args': ( 'Pilot', 'FailedPilotsByCESplitted_24' ),
                                                            'CommandInNewRes': ( 'DIRACAccountingCommand', 'DIRACAccountingCommand' ),
                                                            'argsNewRes': ( 'Pilot', 'NumberOfPilots',
                                                                            {'Format': 'LastHours', 'hours': 24},
                                                                            'GridCE', {'GridStatus':'Aborted'} )}}, ]},
#   'OnSitePropagation' :
#     {
#       'description' : 'How the site\'s services are behaving in the RSS',
#       'module'      : 'PropagationPolicy',
#       'command'     : ( 'RSCommand', 'ServiceStatsCommand' ),
#       'args'        : ( 'Service', ),
#
#       'Site_Panel' : [ { 'RSS' : 'ServiceOfSite' } ]
#      },
#   'OnComputingServicePropagation' :
#     {
#       'description' : 'How the service\'s computing resources are behaving in the RSS',
#       'module'      : 'PropagationPolicy',
#       'command'     : ( 'RSCommand', 'ResourceStatsCommand' ),
#       'args'        : ( 'Resource', ),
#
#       'Service_Computing_Panel' : [ { 'RSS' : 'ResOfCompService' } ]
#      },
#   'OnStorageServicePropagation_Res' :
#     {
#       'description' : 'How the service\'s storage nodes are behaving in the RSS',
#       'module'      : 'PropagationPolicy',
#       'command'     : ( 'RSCommand', 'ResourceStatsCommand' ),
#       'args'        : ( 'Resource', ),
#
#       'Service_Storage_Panel' : [ { 'RSS' : 'ResOfStorService' } ]
#      },
#   'OnStorageServicePropagation_SE' :
#     {
#       'description' : 'How the service\'s storage elements are behaving in the RSS',
#       'module'      : 'PropagationPolicy',
#       'command'     : ( 'RSCommand', 'StorageElementsStatsCommand' ),
#       'args'        : ( 'StorageElement', ),
#
#       'Service_Storage_Panel' : [ { 'RSS' : 'StorageElementsOfSite' },
#                                   {'Chart - Transfer quality in the last 24 hours, incoming.':
#                                   {'CommandIn': ( 'DIRACAccountingCommand', 'CachedPlotCommand' ),
#                                    'args': ( 'Pilot', 'TransferQualityByDestSplittedSite_24' ),
#                                    'CommandInNewRes': ( 'DIRACAccountingCommand', 'DIRACAccountingCommand' ),
#                                    'argsNewRes': ( 'DataOperation', 'Quality',
#                                                   {'Format': 'LastHours', 'hours': 24},
#                                                   'Destination', {'OperationType':'putAndRegister'} )}},
#                                   {'Chart - Transfer quality in the last 24 hours, outgoing.':
#                                   {'CommandIn': ( 'DIRACAccountingCommand', 'CachedPlotCommand' ),
#                                    'args': ( 'Pilot', 'TransferQualityBySourceSplittedSite_24' ),
#                                    'CommandInNewRes': ( 'DIRACAccountingCommand', 'DIRACAccountingCommand' ),
#                                    'argsNewRes': ( 'DataOperation', 'Quality',
#                                                   {'Format': 'LastHours', 'hours': 24},
#                                                   'Source', {'OperationType':'putAndRegister'} )}},
#                                  ]
#      },
  'VOBOX-SLS' :
    {
      'description' : 'How the VO-Box is behaving in the SLS',
      'module'      : 'SLSPolicy',
      'command'     : ( 'SLSCommand', 'SLSStatusCommand' ),
      'args'        : ( 'VO-BOX', ),

      'Service_VO-BOX_Panel' : [ {'WebLink' : {'CommandIn' : ( 'SLSCommand', 'SLSLinkCommand' ),
                                               'args'      : ( 'VO-BOX', )}     }, ]
     },
  'VOMS-SLS' :
    {
      'description' : 'How the VOMS service is behaving in the SLS',
      'module'      : 'SLSPolicy',
      'command'     : ( 'SLSCommand', 'SLSStatusCommand' ),
      'args'        : ( 'VOMS', ),

      'Service_VOMS_Panel': [ {'WebLink': {'CommandIn' : ( 'SLSCommand', 'SLSLinkCommand' ),
                                           'args'   : ( 'VOMS', )}}, ]},
  'OnStorageElementPropagation': {'description' : 'How the storage element\'s nodes are behaving in the RSS',
                                   'module'      : 'DownHillPropagationPolicy',
                                   'command'     : ( 'RSCommand', 'MonitoredStatusCommand' ),
                                   'args'        : ( 'Resource', ),
                                   'SE_Panel' : [{'RSS' : 'ResOfStorEl'}]},
  'TransferQuality':{ 'description'     : 'SE transfer quality',
      'module'          : 'TransferQualityPolicy',
      'commandInNewRes' : ( 'DIRACAccountingCommand', 'TransferQualityCommand' ),
      'argsNewRes'      : None,
      'command'         : ( 'DIRACAccountingCommand', 'TransferQualityFromCachedPlotCommand' ),
      'args'            : ( 'DataOperation', 'TransferQualityByDestSplitted_2' ),

      'SE_Panel' : [ {'FillChart - Transfer quality in the last 24 hours, incoming in the space token':
                        {'CommandIn'       : ( 'DIRACAccountingCommand', 'CachedPlotCommand' ),
                         'args'            : ( 'DataOperation', 'TransferQualityByDestSplitted_24' ),
                         'CommandInNewRes' : ( 'DIRACAccountingCommand', 'DIRACAccountingCommand' ),
                         'argsNewRes'      : ( 'DataOperation', 'Quality',
                                               {'Format' : 'LastHours', 'hours': 24},
                                               'Destination',
                                               {'OperationType' : 'putAndRegister'} )}}, ]},
  'SEOccupancy' :
    {
      'description' : 'SE occupancy',
      'module'      : 'SEOccupancyPolicy',
      'command'     : ( 'SLSCommand', 'SLSStatusCommand' ),
      'args'        : None,

      'SE_Panel' : [ {
                      'WebLink': {
                        'CommandIn' : ( 'SLSCommand', 'SLSLinkCommand' ),
                        'args'      : None
                                  }
                      },
                   ]
     },
  'SEQueuedTransfers' :
    {
      'description' : 'Queued transfers on the SE',
      'module'      : 'SEQueuedTransfersPolicy',
      'command'     : ( 'SLSCommand', 'SLSServiceInfoCommand' ),
      'args'        : None,

      'SE_Panel' : [ {
                       'WebLink' : {
                          'CommandIn' : ( 'SLSCommand', 'SLSLinkCommand' ),
                          'args'      : None
                                   }
                      },
                    ]
     },
   'Nagios_CE' :
    {
      'description' : 'Checks probes with ServiceFlavour CE for a given resource',
      'module'      : 'NagiosProbesPolicy',
      'command'     : ( 'NagiosProbesCommand', 'NagiosProbesCommand' ),
      'args'        : ( 'CE', )
    },
    'Nagios_CREAMCE' :
    {
      'description' : 'Checks probes with ServiceFlavour CREAM-CE for a given resource',
      'module'      : 'NagiosProbesPolicy',
      'command'     : ( 'NagiosProbesCommand', 'NagiosProbesCommand' ),
      'args'        : ( 'CREAM-CE', )
    },
    'Nagios_SE' :
    {
      'description' : 'Checks probes with ServiceFlavour SRMv2 for a given resource',
      'module'      : 'NagiosProbesPolicy',
      'command'     : ( 'NagiosProbesCommand', 'NagiosProbesCommand' ),
      'args'        : ( 'SRMv2', )
    },
    'Nagios_LFC' :
    {
      'description' : 'Checks probes with ServiceFlavour LFC for a given resource',
      'module'      : 'NagiosProbesPolicy',
      'command'     : ( 'NagiosProbesCommand', 'NagiosProbesCommand' ),
      'args'        : ( 'LFC', )
    }
}

#Update DIRAC policies with LHCbDIRAC policies
POLICIESMETA.update( POLICIESMETA_LHCB )

#...............................................................................
#EOF
