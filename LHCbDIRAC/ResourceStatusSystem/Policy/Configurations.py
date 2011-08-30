########################################################################
# $HeadURL:
########################################################################

""" LHCbDIRAC.ResourceStatusSystem.Policy.Configurations Module

    collects everything needed to configure policies
"""

__RCSID__ = "$Id: "

from DIRAC.ResourceStatusSystem.Utilities import CS
from DIRAC.ResourceStatusSystem import ValidStatus, ValidSiteType, ValidResourceType, ValidServiceType

pp = CS.getTypedDictRootedAt("PolicyParameters")

#############################################################################
# policies evaluated
#############################################################################

Policies = {
  'DT_OnGoing_Only' :
    { 'Description' : "Ongoing down-times",
      'Granularity' : [],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module' : 'DT_Policy',
      'commandIn' : ( 'GOCDBStatus_Command', 'GOCDBStatus_Command' ),
      'args' : None,

#      'Site_Panel' : [ {'WebLink': {'Command': 'DT_Link',
#                                    'args': None}}
#                      ],
#      'Resource_Panel' : [ {'WebLink': {'Command': 'DT_Link',
#                                        'args': None}}
#                      ]
     },
  'DT_Scheduled' :
    { 'Description' : "Ongoing and scheduled down-times",
      'Granularity' : ['Site', 'Resource'],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module' : 'DT_Policy',
      'commandInNewRes' : ( 'GOCDBStatus_Command', 'GOCDBStatus_Command' ),
      'commandIn' : ( 'GOCDBStatus_Command', 'DTCached_Command' ),
      'args' : ( pp["DTinHours"], ),
      'Site_Panel' : [ {'WebLink': {'CommandIn': ( 'GOCDBStatus_Command', 'DTInfo_Cached_Command' ),
                                    'args': None}},
                      ],
      'Resource_Panel' : [ {'WebLink': {'CommandIn': ( 'GOCDBStatus_Command', 'DTInfo_Cached_Command' ),
                                        'args': None}},
                      ]
     },
  'GGUSTickets' :
    { 'Description' : "Open GGUS tickets",
      'Granularity' : [],#['Site'],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module' : 'GGUSTickets_Policy',
      'commandIn' : ( 'GGUSTickets_Command', 'GGUSTickets_Open' ),
      'args' : None,
      'Site_Panel' : [ {'WebLink': {'CommandIn': ( 'GGUSTickets_Command', 'GGUSTickets_Link' ),
                                    'args': None}},
                       {'TextInfo': {'CommandIn': ( 'GGUSTickets_Command', 'GGUSTickets_Info' ),
                                    'args': None}},
                     ]
     },
  'SAM_CE' :
    { 'Description' : "Latest SAM results on the LCG Computing Element",
      'Granularity' : [],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['CE'],
      'module': 'SAMResults_Policy',
      'commandIn' : ( 'SAMResults_Command', 'SAMResults_Command' ),
      'args' : ( None, ['LHCb CE-lhcb-availability', 'LHCb CE-lhcb-install', 'LHCb CE-lhcb-job-Boole',
              'LHCb CE-lhcb-job-Brunel', 'LHCb CE-lhcb-job-DaVinci', 'LHCb CE-lhcb-job-Gauss', 'LHCb CE-lhcb-os',
              'LHCb CE-lhcb-queues', 'bi', 'csh', 'js', 'gfal', 'swdir', 'voms'] ),
      'Resource_Panel' : [ {'SAM': {'CommandIn':( 'SAMResults_Command', 'SAMResults_Command' ),
                                    'args': ( None, ['LHCb CE-lhcb-availability', 'LHCb CE-lhcb-install',
                                                     'LHCb CE-lhcb-job-Boole', 'LHCb CE-lhcb-job-Brunel',
                                                     'LHCb CE-lhcb-job-DaVinci', 'LHCb CE-lhcb-job-Gauss',
                                                     'LHCb CE-lhcb-os', 'LHCb CE-lhcb-queues',
                                                     'LHCb CE-lhcb-queues', 'bi', 'csh', 'js', 'gfal',
                                                     'swdir', 'voms'] ) }},
#                           {'WebLink': {'Command':'SAM_Link',
#                                        'args': None}}
                         ]
     },
  'SAM_CREAMCE' :
    { 'Description' : "Latest SAM results on the CREAM Computing Element",
      'Granularity' : [],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['CREAMCE'],
      'module': 'SAMResults_Policy',
      'commandIn' : ( 'SAMResults_Command', 'SAMResults_Command' ),
      'args' : ( None, ['bi', 'csh', 'gfal', 'swdir', 'creamvoms'] ),
      'Resource_Panel' : [ {'SAM': {'CommandIn':( 'SAMResults_Command', 'SAMResults_Command' ),
                                    'args': ( None, ['bi', 'csh', 'gfal', 'swdir', 'creamvoms'] ) }},
#                           {'WebLink': {'Command':'SAM_Link',
#                                        'args': None}}
                         ]
     },
  'SAM_SE' :
    { 'Description' : "Latest SAM results on the SRM nodes",
      'Granularity' : [],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['SE'],
      'module': 'SAMResults_Policy',
      'commandIn' : ( 'SAMResults_Command', 'SAMResults_Command' ),
      'args' : ( None, ['DiracTestUSER', 'FileAccessV2', 'LHCb-cr'] ),
      'Resource_Panel' : [ {'SAM': {'CommandIn':( 'SAMResults_Command', 'SAMResults_Command' ),
                                    'args': ( None, ['DiracTestUSER', 'FileAccessV2', 'LHCb-cr'] ) }},
#                           {'WebLink': {'Command':'SAM_Link',
#                                        'args': None}}
                         ]
     },
  'SAM_LFC_C' :
    { 'Description' : "Latest SAM results on the central LFC nodes",
      'Granularity' : [],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['LFC_C'],
      'module': 'SAMResults_Policy',
      'commandIn' : ( 'SAMResults_Command', 'SAMResults_Command' ),
      'args' : ( None, ['lfcwf', 'lfclr', 'lfcls', 'lfcping'] ),
      'Resource_Panel' : [ {'SAM': {'CommandIn':( 'SAMResults_Command', 'SAMResults_Command' ),
                                    'args': ( None, ['lfcwf', 'lfclr', 'lfcls', 'lfcping'] ) }},
#                           {'WebLink': {'Command':'SAM_Link',
#                                        'args': None}}
                          ]
     },
  'SAM_LFC_L' :
    { 'Description' : "Latest SAM results on the slave LFC nodes",
      'Granularity' : [],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['LFC_L'],
      'module': 'SAMResults_Policy',
      'commandIn' : ( 'SAMResults_Command', 'SAMResults_Command' ),
      'args' : ( None, ['lfcstreams', 'lfclr', 'lfcls', 'lfcping'] ),
      'Resource_Panel' : [ {'SAM': {'CommandIn':( 'SAMResults_Command', 'SAMResults_Command' ),
                                    'args': ( None, ['lfcstreams', 'lfclr', 'lfcls', 'lfcping'] ) }},
#                           {'WebLink': {'Command':'SAM_Link',
#                                        'args': None}}
                          ]
     },
  'SAM_FTS' :
    { 'Description' : "Latest SAM results on the FTS nodes",
      'Granularity' : ['Resource'],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['FTS'],
      'module': 'SAMResults_Policy',
      'commandIn' : ( 'SAMResults_Command', 'SAMResults_Command' ),
      'args' : ( None, ['ftschn', 'ftsinfo'] ),
      'Resource_Panel' : [ {'SAM': {'CommandIn':( 'SAMResults_Command', 'SAMResults_Command' ),
                                    'args': ( None, ['ftschn', 'ftsinfo'] ) }},
#                           {'WebLink': {'Command':'SAM_Link',
#                                        'args': None}}
                          ]
     },
#  'OnNodePropagation' :
#    { 'Description' : "How the site of the node is behaving in the RSS",
#      'Granularity' : ['Resource'],
#      'Status' : ValidStatus,
#      'FormerStatus' : ValidStatus,
#      'SiteType' : ValidSiteType,
#      'ServiceType' : ValidServiceType,
#      'ResourceType' : ValidResourceType,
#      'module': 'DownHillPropagation_Policy',
#      'commandIn' : ('RS_Command', 'MonitoredStatus_Command'),
#      'args' : ('Site', ),
#      'Resource_Panel' : [ {'RSS':'ResOfStorEl'}
#                    ]
#     },
  'JobsEfficiencySimple' :
    { 'Description' : "Simple jobs efficiency",
      'Granularity' : ['Service'],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Computing'],
      'ResourceType' : ValidResourceType,
      'module': 'JobsEfficiency_Simple_Policy',
      'commandInNewRes' : ( 'Jobs_Command', 'JobsEffSimple_Command' ),
      'commandIn' : ( 'Jobs_Command', 'JobsEffSimpleCached_Command' ),
      'args' : None,
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
    { 'Description' : "Simple pilots efficiency",
      'Granularity' : ['Service'],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Computing'],
      'ResourceType' : ValidResourceType,
      'module': 'PilotsEfficiency_Simple_Policy',
      'commandInNewRes' : ( 'Pilots_Command', 'PilotsEffSimple_Command' ),
      'commandIn' : ( 'Pilots_Command', 'PilotsEffSimpleCached_Command' ),
      'args' : None,
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
    { 'Description' : "Simple pilots efficiency",
      'Granularity' : [],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ['CE', 'CREAMCE'],
      'module': 'PilotsEfficiency_Simple_Policy',
      'commandIn' : ( 'Pilots_Command', 'PilotsEffSimple_Command' ),
      'args' : None,
      'Resource_Panel' : [ {'FillChart - Successfull pilots in the last 24 hours':
                            {'CommandIn': ( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                             'args':( 'Pilot', 'SuccessfullPilotsByCESplitted_24' ),
                             'CommandInNewRes': ( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                             'argsNewRes': ( 'Pilot', 'NumberOfPilots',
                                            {'Format': 'LastHours', 'hours': 24},
                                            'GridCE', {'GridStatus':'Done'} )}},
                            {'FillChart - Failed pilots in the last 24 hours':
                             {'CommandIn': ( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                              'args': ( 'Pilot', 'FailedPilotsByCESplitted_24' ),
                              'CommandInNewRes': ( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                              'argsNewRes': ( 'Pilot', 'NumberOfPilots',
                                             {'Format': 'LastHours', 'hours': 24},
                                             'GridCE', {'GridStatus':'Aborted'} )}},
                          ]
     },
  'OnSitePropagation' :
    { 'Description' : "How the site's services are behaving in the RSS",
      'Granularity' : ['Site'],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module' : 'Propagation_Policy',
      'commandIn' : ( 'RS_Command', 'ServiceStats_Command' ),
      'args' : ( 'Service', ),
      'Site_Panel' : [ {'RSS':'ServiceOfSite'}
                      ]
     },
  'OnComputingServicePropagation' :
    { 'Description' : "How the service's computing resources are behaving in the RSS",
      'Granularity' : ['Service'],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Computing'],
      'ResourceType' : ValidResourceType,
      'module' : 'Propagation_Policy',
      'commandIn' : ( 'RS_Command', 'ResourceStats_Command' ),
      'args' : ( 'Resource', ),
      'Service_Computing_Panel' : [ {'RSS':'ResOfCompService'}
                                   ]
     },
  'OnStorageServicePropagation_Res' :
    { 'Description' : "How the service's storage nodes are behaving in the RSS",
      'Granularity' : ['Service'],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Storage'],
      'ResourceType' : ValidResourceType,
      'module' : 'Propagation_Policy',
      'commandIn' : ( 'RS_Command', 'ResourceStats_Command' ),
      'args' : ( 'Resource', ),
      'Service_Storage_Panel' : [ {'RSS':'ResOfStorService'}
                                 ]
     },
  'OnStorageServicePropagation_SE' :
    { 'Description' : "How the service's storage elements are behaving in the RSS",
      'Granularity' : ['Service'],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['Storage'],
      'ResourceType' : ValidResourceType,
      'module' : 'Propagation_Policy',
      'commandIn' : ( 'RS_Command', 'StorageElementsStats_Command' ),
      'args' : ( 'StorageElement', ),
      'Service_Storage_Panel' : [{'RSS':'StorageElementsOfSite'},
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
    { 'Description' : "How the VO-Box is behaving in the SLS",
      'Granularity' : ['Service'],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['VO-BOX'],
      'ResourceType' : ValidResourceType,
      'module' : 'SLS_Policy',
      'commandIn' : ( 'SLS_Command', 'SLSStatus_Command' ),
      'args' : ( 'VO-BOX', ),
      'Service_VO-BOX_Panel' : [ {'WebLink': {'CommandIn':( 'SLS_Command', 'SLSLink_Command' ),
                                  'args': ( 'VO-BOX', )}},
                                ]
     },
  'VOMS-SLS' :
    { 'Description' : "How the VOMS service is behaving in the SLS",
      'Granularity' : ['Service'],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ['VOMS'],
      'ResourceType' : ValidResourceType,
      'module' : 'SLS_Policy',
      'commandIn' : ( 'SLS_Command', 'SLSStatus_Command' ),
      'args' : ( 'VOMS', ),
      'Service_VOMS_Panel' : [ {'WebLink': {'CommandIn':( 'SLS_Command', 'SLSLink_Command' ),
                                  'args': ( 'VOMS', )}},
                              ]
     },
#  'OnServicePropagation' :
#    { 'Granularity' : [],
#      'Status' : ValidStatus,
#      'FormerStatus' : ValidStatus,
#      'SiteType' : ValidSiteType,
#      'ServiceType' : ValidServiceType,
#      'ResourceType' : ValidResourceType,
#      'module' : 'OnServicePropagation_Policy',
#      'commandIn' : None,
#      'args' : None,
#     },
  'OnStorageElementPropagation' :
    { 'Description' : "How the storage element's nodes are behaving in the RSS",
      'Granularity' : ['StorageElementRead','StorageElementWrite'],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module': 'DownHillPropagation_Policy',
      'commandIn' : ( 'RS_Command', 'MonitoredStatus_Command' ),
      'args' : ( 'Resource', ),
      'SE_Panel' : [ {'RSS':'ResOfStorEl'}
                    ]
     },
#  'OnSENodePropagation' :
#    { 'Granularity' : [],
#      'Status' : ValidStatus,
#      'FormerStatus' : ValidStatus,
#      'SiteType' : ValidSiteType,
#      'ServiceType' : ValidServiceType,
#      'module': 'OnSENodePropagation_Policy',
#      'commandIn' : None,
#      'args' : None,
#      'ResourceType' : ['SE'],
#     },
  'TransferQuality' :
    { 'Description' : "SE transfer quality",
      'Granularity' : ['StorageElementRead','StorageElementWrite'],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module': 'TransferQuality_Policy',
      'commandInNewRes' : ( 'DIRACAccounting_Command', 'TransferQuality_Command' ),
      'argsNewRes':None,
#      'commandIn' : ('DIRACAccounting_Command', 'TransferQualityCached_Command'),
      'commandIn' : ( 'DIRACAccounting_Command', 'TransferQualityFromCachedPlot_Command' ),
      'args' : ( 'DataOperation', 'TransferQualityByDestSplitted' ),
      'SE_Panel' : [ {'FillChart - Transfer quality in the last 24 hours, incoming in the space token':
                      {'CommandIn':( 'DIRACAccounting_Command', 'CachedPlot_Command' ),
                       'args':( 'DataOperation', 'TransferQualityByDestSplitted_24' ),
                       'CommandInNewRes':( 'DIRACAccounting_Command', 'DIRACAccounting_Command' ),
                       'argsNewRes':( 'DataOperation', 'Quality',
                                     {'Format': 'LastHours', 'hours': 24},
                                     'Destination', {'OperationType':'putAndRegister'} )}},
                      ]
     },
  'SEOccupancy' :
    { 'Description' : "SE occupancy",
      'Granularity' : ['StorageElementRead','StorageElementWrite'],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module': 'SEOccupancy_Policy',
      'commandIn' : ( 'SLS_Command', 'SLSStatus_Command' ),
      'args' : None,
      'SE_Panel' : [ {'WebLink': {'CommandIn':( 'SLS_Command', 'SLSLink_Command' ),
                                  'args': None}},
                      ]
     },
  'SEQueuedTransfers' :
    { 'Description' : "Queued transfers on the SE",
      'Granularity' : ['StorageElementRead','StorageElementWrite'],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ['T0'],
      'ServiceType' : ValidServiceType,
      'ResourceType' : ValidResourceType,
      'module': 'SEQueuedTransfers_Policy',
      'commandIn' : ( 'SLS_Command', 'SLSServiceInfo_Command' ),
      'args' : ( ["Queued transfers"], ),
      'SE_Panel' : [ {'WebLink': {'CommandIn':( 'SLS_Command', 'SLSLink_Command' ),
                                  'args': None}},
                      ]
     },
#  'Fake' :
#    { 'Granularity' : ['Site'],
#      'Status' : ValidStatus,
#      'FormerStatus' : ValidStatus,
#      'SiteType' : ValidSiteType,
#      'ServiceType' : ValidServiceType,
#      'ConfirmationPolicy': 'Fake_Confirm',
#      'module': 'Fake_Policy',
#      'commandIn' : ('DoNothing_Command', 'DoNothing_Command'),
#      'args' : None,
#      'ResourceType' : ValidResourceType,
#     },
#  'Fake_Confirm' :
#    { 'Granularity' : [],
#      'Status' : ValidStatus,
#      'FormerStatus' : ValidStatus,
#      'SiteType' : ValidSiteType,
#      'ServiceType' : ValidServiceType,
#      'module': 'Fake_Confirm_Policy',
#      'commandIn' : ('DoNothing_Command', 'DoNothing_Command'),
#      'args' : None,
#      'ResourceType' : ValidResourceType,
#     },
  'AlwaysFalse' :
    { 'Granularity' : [],
      'Status' : ValidStatus,
      'FormerStatus' : ValidStatus,
      'SiteType' : ValidSiteType,
      'ServiceType' : ValidServiceType,
      'commandIn' : None,
      'args' : None,
      #'ResourceType' : ValidResourceType,
     }
}
