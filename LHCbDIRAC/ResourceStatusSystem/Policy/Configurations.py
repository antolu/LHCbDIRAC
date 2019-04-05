''' LHCbDIRAC.ResourceStatusSystem.Policy.Configurations

   POLICIESMETA_LHCB
   policies
   POLICIESMETA

'''

__RCSID__ = "$Id$"

from DIRAC.ResourceStatusSystem.Policy.Configurations import POLICIESMETA

# LHCb Policies

POLICIESMETA_LHCB = {'GGUSTickets': {'description': "Open GGUS tickets",
                                     'module': 'GGUSTicketsPolicy',
                                     'command': ('GGUSTicketsCommand', 'GGUSTicketsCommand'),
                                     'args': {'onlyCache': False}},
                     'TransferQualitySource': {'description': 'Transfers from element, quality measure',
                                               'module': 'TransferQualityPolicy',
                                               'command': ('TransferCommand', 'TransferCommand'),
                                               'args': {'direction': 'Source',
                                                        'metric': 'Quality',
                                                        'hours': 2}},
                     'TransferQualityDestination': {'description': 'Transfers to element, quality measure',
                                                    'module': 'TransferQualityPolicy',
                                                    'command': ('TransferCommand', 'TransferCommand'),
                                                    'args': {'direction': 'Destination', 'metric':
                                                              'Quality', 'hours': 2}},
                     }

################################################################################
# Old stuff

policies = {'DTScheduled': {'description': 'Ongoing and scheduled down-times',
                            'module': 'DTPolicy',
                            'commandInNewRes': ('GOCDBStatusCommand', 'GOCDBStatusCommand'),
                            # 'command'         : ( 'GOCDBStatusCommand', 'DTCachedCommand' ),
                            'command': ('GOCDBStatusCommand', 'GOCDBStatusCommand'),
                            'args': {'hours': 12},  # Fix to avoid querying the CS on load time, to be fixed
                            'Site_Panel': [{'WebLink': {'CommandIn': ('GOCDBStatusCommand',
                                                                      'DTInfoCachedCommand'),
                                                        'args': None}}],
                            'Resource_Panel': [{'WebLink': {'CommandIn': ('GOCDBStatusCommand',
                                                                          'DTInfoCachedCommand'),
                                                            'args': None}}]},
            'GGUSTickets': {'description': 'Open GGUS tickets',
                            'module': 'GGUSTicketsPolicy',
                            'command': ('GGUSTicketsCommand', 'GGUSTicketsOpen'),
                            'args': None,
                            'Site_Panel': [{'WebLink': {'CommandIn': ('GGUSTicketsCommand', 'GGUSTicketsLink'),
                                                        'args': None}},
                                           {'TextInfo': {'CommandIn': ('GGUSTicketsCommand', 'GGUSTicketsInfo'),
                                                         'args': None}}]},

            'OnStorageElementPropagation': {'description': 'How the storage element\'s nodes are behaving in the RSS',
                                            'module': 'DownHillPropagationPolicy',
                                            'command': ('RSCommand', 'MonitoredStatusCommand'),
                                            'args': ('Resource', ),
                                            'SE_Panel': [{'RSS': 'ResOfStorEl'}]},
            'TransferQuality': {'description': 'SE transfer quality',
                                'module': 'TransferQualityPolicy',
                                'commandInNewRes': ('DIRACAccountingCommand', 'TransferQualityCommand'),
                                'argsNewRes': None,
                                'command': ('DIRACAccountingCommand', 'TransferQualityFromCachedPlotCommand'),
                                'args': ('DataOperation', 'TransferQualityByDestSplitted_2'),

                                'SE_Panel': [{'FillChart - Transfer quality in the last 24 hours':
                                              {'CommandIn': ('DIRACAccountingCommand', 'CachedPlotCommand'),
                                               'args': ('DataOperation', 'TransferQualityByDestSplitted_24'),
                                               'CommandInNewRes': ('DIRACAccountingCommand', 'DIRACAccountingCommand'),
                                               'argsNewRes': ('DataOperation', 'Quality',
                                                              {'Format': 'LastHours', 'hours': 24},
                                                              'Destination',
                                                              {'OperationType': 'putAndRegister'})}}, ]}}

# Update DIRAC policies with LHCbDIRAC policies
POLICIESMETA.update(POLICIESMETA_LHCB)
