# $HeadURL$
''' Configurations

  Collects everything needed to configure policies.
  
'''

from DIRAC.ResourceStatusSystem.Utilities import CS

__RCSID__ = '$Id$'

pp = CS.getTypedDictRootedAt( 'PolicyParameters' )

#############################################################################
# policies evaluated
#############################################################################

Policies = {
  'DT_OnGoing_Only' :
    { 
     'Description' : 'Ongoing down-times',
      'module'     : 'DT_Policy',
      'commandIn'  : ( 'GOCDBStatus_Command', 'GOCDBStatus_Command' ),
      'args'       : None
     },
  'DT_Scheduled' :
    { 
      'Description'     : 'Ongoing and scheduled down-times',
      'module'          : 'DT_Policy',
      'commandInNewRes' : ( 'GOCDBStatus_Command', 'GOCDBStatus_Command' ),
      'commandIn'       : ( 'GOCDBStatus_Command', 'DTCached_Command' ),
      'args'            : ( pp[ 'DTinHours' ], ),
      
      'Site_Panel' : [ {'WebLink': {'CommandIn': ( 'GOCDBStatus_Command', 'DTInfo_Cached_Command' ),
                                    'args': None}},
                      ],
      'Resource_Panel' : [ {'WebLink': {'CommandIn': ( 'GOCDBStatus_Command', 'DTInfo_Cached_Command' ),
                                        'args': None}},
                      ]
     },
  'GGUSTickets' :
    { 
      'Description' : 'Open GGUS tickets',
      'module'      : 'GGUSTicketsPolicy',
      'commandIn'   : ( 'GGUSTickets_Command', 'GGUSTickets_Open' ),
      'args'        : None,
      
      'Site_Panel' : [ {'WebLink': {'CommandIn': ( 'GGUSTickets_Command', 'GGUSTickets_Link' ),
                                    'args': None}},
                       {'TextInfo': {'CommandIn': ( 'GGUSTickets_Command', 'GGUSTickets_Info' ),
                                    'args': None}},
                     ]
     },
  'SAM_CE' :
    { 
      'Description' : 'Latest SAM results on the LCG Computing Element',
      'module'      : 'SAMResultsPolicy',
      'commandIn'   : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
      'args'        : ( None, [ 'LHCb CE-lhcb-availability', 'LHCb CE-lhcb-install', 
                                'LHCb CE-lhcb-job-Boole', 'LHCb CE-lhcb-job-Brunel', 
                                'LHCb CE-lhcb-job-DaVinci', 'LHCb CE-lhcb-job-Gauss', 
                                'LHCb CE-lhcb-os', 'LHCb CE-lhcb-queues', 'bi', 
                                'csh', 'js', 'gfal', 'swdir', 'voms'] ),

      'Resource_Panel' : [ {'SAM': {'CommandIn':( 'SAMResultsCommand', 'SAMResultsCommand' ),
                                    'args': ( None, ['LHCb CE-lhcb-availability', 'LHCb CE-lhcb-install',
                                                     'LHCb CE-lhcb-job-Boole', 'LHCb CE-lhcb-job-Brunel',
                                                     'LHCb CE-lhcb-job-DaVinci', 'LHCb CE-lhcb-job-Gauss',
                                                     'LHCb CE-lhcb-os', 'LHCb CE-lhcb-queues',
                                                     'LHCb CE-lhcb-queues', 'bi', 'csh', 'js', 'gfal',
                                                     'swdir', 'voms'] ) }},
                         ]
     },
  'SAM_CREAMCE' :
    { 
      'Description' : 'Latest SAM results on the CREAM Computing Element',
      'module'      : 'SAMResultsPolicy',
      'commandIn'   : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
      'args'        : ( None, [ 'bi', 'csh', 'gfal', 'swdir', 'creamvoms' ] ),

      'Resource_Panel' : [ { 'SAM': 
                             { 'CommandIn' : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
                               'args'      : ( None, [ 'bi', 'csh', 'gfal', 'swdir', 'creamvoms' ] ) 
                               }
                            },
                         ]
     },
  'SAM_SE' :
    { 
      'Description' : 'Latest SAM results on the SRM nodes',
      'module'      : 'SAMResultsPolicy',
      'commandIn'   : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
      'args'        : ( None, [ 'DiracTestUSER', 'FileAccessV2', 'LHCb-cr' ] ),

      'Resource_Panel' : [ {'SAM': {'CommandIn':( 'SAMResultsCommand', 'SAMResultsCommand' ),
                                    'args': ( None, ['DiracTestUSER', 'FileAccessV2', 'LHCb-cr'] ) }},
                         ]
     },
  'SAM_LFC_C' :
    { 
      'Description' : 'Latest SAM results on the central LFC nodes',
      'module'      : 'SAMResultsPolicy',
      'commandIn'   : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
      'args'        : ( None, [ 'lfcwf', 'lfclr', 'lfcls', 'lfcping' ] ),
      
      'Resource_Panel' : [ { 'SAM': 
                            { 'CommandIn' : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
                              'args'      : ( None, [ 'lfcwf', 'lfclr', 'lfcls', 'lfcping' ] ) 
                              }
                            },
                          ]
     },
  'SAM_LFC_L' :
    { 
      'Description' : 'Latest SAM results on the slave LFC nodes',
      'module'      : 'SAMResultsPolicy',
      'commandIn'   : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
      'args'        : ( None, [ 'lfcstreams', 'lfclr', 'lfcls', 'lfcping' ] ),
      
      'Resource_Panel' : [ { 'SAM': 
                            { 'CommandIn':( 'SAMResultsCommand', 'SAMResultsCommand' ),
                              'args'     : ( None, [ 'lfcstreams', 'lfclr', 'lfcls', 'lfcping' ] ) 
                            }
                           },
                          ]
     },
  'SAM_FTS' :
    { 
      'Description' : 'Latest SAM results on the FTS nodes',
      'module'      : 'SAMResultsPolicy',
      'commandIn'   : ( 'SAMResultsCommand', 'SAMResultsCommand' ),
      'args'        : ( None, [ 'ftschn', 'ftsinfo' ] ),
     
      'Resource_Panel' : [ { 'SAM': {'CommandIn':( 'SAMResultsCommand', 'SAMResultsCommand' ),
                                    'args': ( None, ['ftschn', 'ftsinfo'] ) }},
                          ]
     },
  'JobsEfficiencySimple' :
    { 
      'Description'     : 'Simple jobs efficiency',
      'module'          : 'JobsEfficiencySimplePolicy',
      'commandInNewRes' : ( 'Jobs_Command', 'JobsEffSimple_Command' ),
      'commandIn'       : ( 'Jobs_Command', 'JobsEffSimpleCached_Command' ),
      'args'            : None,
      
      'Service_Computing_Panel' : [ {'FillChart - Successfull Jobs in the last 24 hours':
                                     {'CommandIn': ( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                                      'args':( 'Job', 'SuccessfullJobsBySiteSplitted_24' ),
                                      'CommandInNewRes': ( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                                      'argsNewRes': ( 'Job', 'NumberOfJobs',
                                                     {'Format': 'LastHours', 'hours': 24},
                                                     'Site', {'FinalMajorStatus':'Done'} )}},
                                    {'FillChart - Failed Jobs in the last 24 hours':
                                     {'CommandIn': ( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                                      'args':( 'Job', 'FailedJobsBySiteSplitted_24' ),
                                      'CommandInNewRes': ( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                                      'argsNewRes': ( 'Job', 'NumberOfJobs',
                                                     {'Format': 'LastHours', 'hours': 24},
                                                     'Site', {'FinalMajorStatus':'Failed'} )}},
                                    {'FillChart - Running Jobs in the last 24 hours':
                                     {'CommandIn': ( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                                      'args':( 'Job', 'RunningJobsBySiteSplitted_24' ),
                                      'CommandInNewRes': ( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                                      'argsNewRes': ( 'WMSHistory', 'NumberOfJobs',
                                                     {'Format': 'LastHours', 'hours': 24},
                                                     'Site', {} )}},
                                    {'FillChart - Running Jobs in the last week':
                                     {'CommandIn': ( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                                      'args':( 'Job', 'RunningJobsBySiteSplitted_168' ),
                                      'CommandInNewRes': ( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                                      'argsNewRes': ( 'WMSHistory', 'NumberOfJobs',
                                                     {'Format': 'LastHours', 'hours': 168},
                                                     'Site', {} )}},
                                    {'FillChart - Running Jobs in the last month':
                                     {'CommandIn': ( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                                      'args':( 'Job', 'RunningJobsBySiteSplitted_720' ),
                                      'CommandInNewRes': ( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                                      'argsNewRes': ( 'WMSHistory', 'NumberOfJobs',
                                                     {'Format': 'LastHours', 'hours': 720},
                                                               'Site', {} )}},
                                    {'FillChart - Running Jobs in the last year':
                                     {'CommandIn': ( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                                      'args':( 'Job', 'RunningJobsBySiteSplitted_8760' ),
                                      'CommandInNewRes': ( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                                      'argsNewRes': ( 'WMSHistory', 'NumberOfJobs',
                                                     {'Format': 'LastHours', 'hours': 8760},
                                                     'Site', {} )}}
                                    ]
   },
  'PilotsEfficiencySimple_Service' :
    { 
      'Description'     : 'Simple pilots efficiency',
      'module'          : 'PilotsEfficiencySimplePolicy',
      'commandInNewRes' : ( 'Pilots_Command', 'PilotsEffSimple_Command' ),
      'commandIn'       : ( 'Pilots_Command', 'PilotsEffSimpleCached_Command' ),
      'args'            : None,
      
      'Service_Computing_Panel' : [ {'FillChart - Successfull pilots in the last 24 hours':
                                     {'CommandIn': ( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                                      'args': ( 'Pilot', 'SuccessfullPilotsBySiteSplitted_24' ),
                                      'CommandInNewRes' : ( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                                      'argsNewRes': ( 'Pilot', 'NumberOfPilots',
                                                     {'Format': 'LastHours', 'hours': 24},
                                                     'Site', {'GridStatus':'Done'} )}},
                                    {'FillChart - Aborted pilots in the last 24 hours':
                                     {'CommandIn': ( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                                      'args': ( 'Pilot', 'FailedPilotsBySiteSplitted_24' ),
                                      'CommandInNewRes': ( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                                      'argsNewRes': ( 'Pilot', 'NumberOfPilots',
                                                     {'Format': 'LastHours', 'hours': 24},
                                                     'Site', {'GridStatus':'Aborted'} )}},
                                    ]
     },
  'PilotsEfficiencySimple_Resource' :
    { 
      'Description' : 'Simple pilots efficiency',
      'module'      : 'PilotsEfficiencySimplePolicy',
      'commandIn'   : ( 'Pilots_Command', 'PilotsEffSimple_Command' ),
      'args'        : None,
      
      'Resource_Panel' : [ { 'FillChart - Successfull pilots in the last 24 hours':
                           { 'CommandIn': ( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                             'args':( 'Pilot', 'SuccessfullPilotsByCESplitted_24' ),
                             'CommandInNewRes': ( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                             'argsNewRes': ( 'Pilot', 'NumberOfPilots',
                                            {'Format': 'LastHours', 'hours': 24},
                                            'GridCE', {'GridStatus':'Done'} )}},
                           { 'FillChart - Failed pilots in the last 24 hours':
                            { 'CommandIn': ( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                             'args': ( 'Pilot', 'FailedPilotsByCESplitted_24' ),
                              'CommandInNewRes': ( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                              'argsNewRes': ( 'Pilot', 'NumberOfPilots',
                                             {'Format': 'LastHours', 'hours': 24},
                                             'GridCE', {'GridStatus':'Aborted'} )}},
                          ]
     },
  'OnSitePropagation' :
    { 
      'Description' : 'How the site\'s services are behaving in the RSS',
      'module'      : 'PropagationPolicy',
      'commandIn'   : ( 'RS_Command', 'ServiceStats_Command' ),
      'args'        : ( 'Service', ),
      
      'Site_Panel' : [ { 'RSS' : 'ServiceOfSite' } ]
     },
  'OnComputingServicePropagation' :
    { 
      'Description' : 'How the service\'s computing resources are behaving in the RSS',
      'module'      : 'PropagationPolicy',
      'commandIn'   : ( 'RS_Command', 'ResourceStats_Command' ),
      'args'        : ( 'Resource', ),
      
      'Service_Computing_Panel' : [ { 'RSS' : 'ResOfCompService' } ]
     },
  'OnStorageServicePropagation_Res' :
    { 
      'Description' : 'How the service\'s storage nodes are behaving in the RSS',
      'module'      : 'PropagationPolicy',
      'commandIn'   : ( 'RS_Command', 'ResourceStats_Command' ),
      'args'        : ( 'Resource', ),
      
      'Service_Storage_Panel' : [ { 'RSS' : 'ResOfStorService' } ]
     },
  'OnStorageServicePropagation_SE' :
    { 
      'Description' : 'How the service\'s storage elements are behaving in the RSS',
      'module'      : 'PropagationPolicy',
      'commandIn'   : ( 'RS_Command', 'StorageElementsStats_Command' ),
      'args'        : ( 'StorageElement', ),
      
      'Service_Storage_Panel' : [ { 'RSS' : 'StorageElementsOfSite' },
                                  {'Chart - Transfer quality in the last 24 hours, incoming.':
                                  {'CommandIn': ( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                                   'args': ( 'Pilot', 'TransferQualityByDestSplittedSite_24' ),
                                   'CommandInNewRes': ( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                                   'argsNewRes': ( 'DataOperation', 'Quality',
                                                  {'Format': 'LastHours', 'hours': 24},
                                                  'Destination', {'OperationType':'putAndRegister'} )}},
                                  {'Chart - Transfer quality in the last 24 hours, outgoing.':
                                  {'CommandIn': ( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                                   'args': ( 'Pilot', 'TransferQualityBySourceSplittedSite_24' ),
                                   'CommandInNewRes': ( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                                   'argsNewRes': ( 'DataOperation', 'Quality',
                                                  {'Format': 'LastHours', 'hours': 24},
                                                  'Source', {'OperationType':'putAndRegister'} )}},
                                 ]
     },
  'VOBOX-SLS' :
    { 
      'Description' : 'How the VO-Box is behaving in the SLS',
      'module'      : 'SLSPolicy',
      'commandIn'   : ( 'SLSCommand', 'SLSStatusCommand' ),
      'args'        : ( 'VO-BOX', ),
      
      'Service_VO-BOX_Panel' : [ {
                                   'WebLink' : {
                                                 'CommandIn' : ( 'SLSCommand', 'SLSLinkCommand' ),
                                                 'args'      : ( 'VO-BOX', )
                                                 }
                                  },
                                ]
     },
  'VOMS-SLS' :
    { 
      'Description' : 'How the VOMS service is behaving in the SLS',
      'module'      : 'SLSPolicy',
      'commandIn'   : ( 'SLSCommand', 'SLSStatusCommand' ),
      'args'        : ( 'VOMS', ),
      
      'Service_VOMS_Panel' : [ {
                                 'WebLink': { 
                                              'CommandIn' : ( 'SLSCommand', 'SLSLinkCommand' ),
                                              'args'   : ( 'VOMS', )
                                             }
                                },
                              ]
     },
  'CondDB-SLS' :
    { 
      'Description' : 'How the CondDB service is behaving in the SLS',
      'module'      : 'SLSPolicy',
      'commandIn'   : ( 'SLSCommand', 'SLSStatusCommand' ),
      'args'        : ( 'CondDB', )
      },
  'OnStorageElementPropagation' :
    { 
      'Description' : 'How the storage element\'s nodes are behaving in the RSS',
      'module'      : 'DownHillPropagationPolicy',
      'commandIn'   : ( 'RS_Command', 'MonitoredStatus_Command' ),
      'args'        : ( 'Resource', ),
      
      'SE_Panel' : [ {
                       'RSS' : 'ResOfStorEl'
                       }
                    ]
     },
  'TransferQuality' :
    { 
      'Description'     : 'SE transfer quality',
      'module'          : 'TransferQualityPolicy',
      'commandInNewRes' : ( 'DIRACAccounting_Command', 'TransferQuality_Command' ),
      'argsNewRes'      : None,
      'commandIn'       : ( 'DIRACAccounting_Command', 'TransferQualityFromCachedPlot_Command' ),
      'args'            : ( 'DataOperation', 'TransferQualityByDestSplitted_2' ),
      
      'SE_Panel' : [ {
                      'FillChart - Transfer quality in the last 24 hours, incoming in the space token':
                        {
                          'CommandIn'       : ( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                          'args'            : ( 'DataOperation', 'TransferQualityByDestSplitted_24' ),
                          'CommandInNewRes' : ( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                          'argsNewRes'      : ( 'DataOperation', 'Quality',
                                               { 
                                                'Format' : 'LastHours', 'hours': 24
                                               },
                                               'Destination', 
                                               {
                                                'OperationType' : 'putAndRegister'
                                                } )
                         }
                       },
                      ]
     },
  'SEOccupancy' :
    { 
      'Description' : 'SE occupancy',
      'module'      : 'SEOccupancyPolicy',
      'commandIn'   : ( 'SLSCommand', 'SLSStatusCommand' ),
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
      'Description' : 'Queued transfers on the SE',
      'module'      : 'SEQueuedTransfersPolicy',
      'commandIn'   : ( 'SLSCommand', 'SLSServiceInfoCommand' ),
      'args'        : None,
      
      'SE_Panel' : [ { 
                       'WebLink' : {
                          'CommandIn' : ( 'SLSCommand', 'SLSLinkCommand' ),
                          'args'      : None
                                   }
                      },
                    ]
     },
  'AlwaysFalse' :
    {
      'Description' : 'A Policy that always returns false',
      'commandIn'   : None,
      'args'        : None,
    },
   'Nagios_CE' :
    {
      'Description' : 'Checks probes with ServiceFlavour CE for a given resource',
      'module'      : 'NagiosProbesPolicy',
      'commandIn'   : ( 'NagiosProbesCommand', 'NagiosProbesCommand' ),
      'args'        : ( 'CE', )
    },
    'Nagios_CREAMCE' :
    {
      'Description' : 'Checks probes with ServiceFlavour CREAM-CE for a given resource',
      'module'      : 'NagiosProbesPolicy',
      'commandIn'   : ( 'NagiosProbesCommand', 'NagiosProbesCommand' ),
      'args'        : ( 'CREAM-CE', )
    },
    'Nagios_SE' :
    {
      'Description' : 'Checks probes with ServiceFlavour SRMv2 for a given resource',
      'module'      : 'NagiosProbesPolicy',
      'commandIn'   : ( 'NagiosProbesCommand', 'NagiosProbesCommand' ),
      'args'        : ( 'SRMv2', )
    },
    'Nagios_LFC' :
    {
      'Description' : 'Checks probes with ServiceFlavour LFC for a given resource',
      'module'      : 'NagiosProbesPolicy',
      'commandIn'   : ( 'NagiosProbesCommand', 'NagiosProbesCommand' ),
      'args'        : ( 'LFC', )
    }
}

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF