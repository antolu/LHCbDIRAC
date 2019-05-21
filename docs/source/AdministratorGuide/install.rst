=================
Setup a new vobox
=================

The configuration file is puppetized. If the machine correctly created, the configuration file
need to install the machine already must be exists.

The installation procedure:

	- cd /home/dirac
	
	- curl -O https://raw.githubusercontent.com/DIRACGrid/DIRAC/integration/Core/scripts/install_site.sh
	
	- chmod +x install_site.sh
	
	- Edit install_site.sh add the Release = version
	
	- ./install_site.sh install.cfg
	
	Note: install.cfg file must exists in the /home/dirac directory
	
Make sure the dirac.cfg file is correctly created in the machine.