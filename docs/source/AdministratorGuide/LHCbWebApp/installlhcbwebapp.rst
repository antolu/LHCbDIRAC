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
	
	- cd /home/dirac
	
	- curl -O https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/install_site.sh
	
	- chmod +x install_site.sh
	
	- Edit install_site.sh add the Release = version
	
	- ./install_site.sh install.cfg
	
	Note: install.cfg file must exists in the /home/dirac directory
	
Most of the case you will have problem with the SELinux. If you see the following error message, you need to generate 
the correct role:
	
[dirac@lbvobox202 dirac]$ tail -200f data/log/nginx/error.log 
2019/05/06 16:57:07 [crit] 19494#0: *1 connect() to 127.0.0.1:8000 failed (13: Permission denied) while connecting to upstream, client: 128.141.212.123, server: lhcb-portal-dirac.cern.ch, request: "GET / HTTP/1.1", upstream: "http://127.0.0.1:8000/", host: "lbvobox202.cern.ch"

To do that please execute the following commands as root:

        grep nginx /var/log/audit/audit.log | audit2allow -M nginx
        semodule -i nginx.pp
        rferesh the page
	 
NOTE: Most probably, it will not work. Ask Joel to create the correct dirac.cfg file. The dirac.cfg file content 
must be the same as the existing web machine.