======================================
CERN centralized Elasticsearch service
======================================

This document contains all information needed to manage the ES. ES is provided by
CERN. 

----------------------
Communication channels:
----------------------

	1. open a ticket: ` snow ticket: <https://cern.service-now.com/service-portal/service-element.do?name=Elasticsearch-Service>_`.
	2. mattermost: LHCb ` specific channel <https://mattermost.web.cern.ch/it-dep/channels/es-for-lhcb>`_ or ` it general
	channel <https://mattermost.web.cern.ch/it-dep/channels/it-es-project>`_.
	
-----------------------
Elasticsearch instances
-----------------------

We are using three instances(host:username):

	1. es-lhcb-monitoring:lhcb for monitoring WMS and ComponentMonitoring
	2. es-lhcb-dirac-logs:lhcb-dirac-logs for centralized Monitoring
	3. es-lhcb-mcstats:lhcb-mcstats for MC statistics
	

------------------------------------
Elasticsearch performance monitoring
------------------------------------

IT/ES provides monitoring tool for monitoring ES instances. You can access `in the following link <https://es-perfmon-lhcb.cern.ch>`_. 

------
Kibana
------

Kibana is used for visualize the data. IT/ES provides a Kibana end point for each ES instance.
You can access using https://instance/kibana for example: https://es-lhcb-monitoring.cern.ch/kibana

---------------------
Managing ES templates
---------------------

Each ES instance has a dedicated template, what you can found in the `repository <https://gitlab.cern.ch/it-elasticsearch-project>`_ by
searching lhcb. For example: `https://gitlab.cern.ch/it-elasticsearch-project/endpoint-lhcb-dirac-logs-settings`.

-------
Curator
-------

Curator can be used for easily manage ES data. It can be used in different purpose. We
are using for deleting indexes, which are older a certain age. To setup Curator you need to
use the ES template repository (see Managing ES templates section.) and create `curator4.actions` file.
For example: `deleting indexes older a certain period <https://gitlab.cern.ch/it-elasticsearch-project/endpoint-lhcb-dirac-logs-settings/raw/master/curator4.actions>`_.

--------------------------
Re-indexing existing index
--------------------------

You may need to re-index indexes from one cluster to another cluster. You can
use the `following script to reindex <https://gitlab.cern.ch/lhcb-dirac/LHCbDIRACMgmt>`_.
