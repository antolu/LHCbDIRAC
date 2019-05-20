.. _installlhcbwebapp:

==========================
Installing LHCbWebAppDIRAC
==========================

The installation requires two steps:

	1. Install the machine:
	
	The machine should installed using the ``webportal`` puppet template. LHCbWepAppDIRAC is using ``Nginx`` for better performance, which is also puppetized. 
	The main configuration file used to install ``Nginx`` can be found `in this gitlab repository <https://gitlab.cern.ch/ai/it-puppet-hostgroup-volhcb/blob/qa/code/manifests/vobox/webportal/nginx.pp>`_ .
	The ``site.conf`` configuration file is used for handling the user requests and pass to the ``Tornado`` based LHCbWebAppDIRAC component. The configuration file can be 
	found in  `this repository <https://gitlab.cern.ch/ai/it-puppet-hostgroup-volhcb/blob/qa/code/templates/site.conf.erb>`_ .

	2. Installing LHCbWebAppDIRAC extension:
 