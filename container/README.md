# LHCbDIRAC in containers and Mesos

This documentation explains how the DIRAC services are ran on Mesos, and the use of the different
folders and scripts in this folder.

Eventhough the setup of Mesos is as generic as possible, there are some conventions that are taken
to ease the management.

## lhcbdirac folder

This contains the necessary files to build an LHCbDIRAC image.

* Dockerfile.tpl: this is a template of a Dockerfile, in which variables are replaced by dirac-docker-mgmt to produce the final Dockerfile
* dockerEntrypoint.sh: entry point of docker to setup the DIRAC environment before executing the command
* dirac_self_ping.py: Marathon monitors containers by executing a command inside them. This script perform a DIPS ping on the service running inside the container

The host should mount into the container:
* the dirac.cfg in `/opt/dirac/etc/dirac.cfg`
* the certificates as `/opt/dirac/etc/grid-security`

If running a service, it is necessary to expose a port.

## monitoring folder

This contains all the necessary scripts to monitor the containers. They are deployed on all mesos slave with puppet.

* simpleParseCAdvisor.py: collects metrics from the local cAdvisor, and send them to an InfluxDB instance. The script is called by a cron job.
* testConfig.json: configuration file example for the database.

## pinger folder

The `pinger` is a container used to monitor the health of the DIRAC container from the outside.
It performs a DIPS ping. It is used by Consul to test the health of the services.
As long as the dips protocol or the underlying SSL does not change, it does not matter if it is
based on an old DIRAC version.

* Dockerfile: the Dockerfile to make it a container
* dirac_ping: simple web server that transforms an HTTP request into a DIPS ping

## marathon folder

This contains the tools to generate the marathon config files for our DIRAC services.

* generate-marathon-config.py: from basic info (component name, cpu, etc) or an input file containing these info, this script generates the Marathon JSON file properly configured for our environment.
* service_template.json: this is the template that will be followed by generate-marathon-config to generate the services.
* test-marathon-service.py: given a Marathon service config file, this script will run the container as Marathon would. It is a good way to test..
* pinger.json: the marathon json configuration file to run the pinger in mesos.
* certification_services.json: This file is compatible with generate-marathon-config, and lists all the services that are running for Certification on our cluster.

## dirac-docker-mgmt

This is the Swiss Army Knife to have a new release of LHCbDIRAC built into a container and running on the cluster. See the release documentation
and the help of the script for more details.
