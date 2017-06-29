# LHCbDIRAC in container

This document explains how LHCbDIRAC is packaged into docker containers, and some of its integration with Mesos

## lhcbdirac folder

This contains the necessary files to build an LHCbDIRAC image.

* Dockerfile: this is the Dockerfile used to build the image. It is necessary to modify the version by hand
* dockerEntrypoint.sh: entry point of docker to setup the DIRAC environment before executing the command
* dirac_self_ping.py: Marathon monitors containers by executing a command inside them. This script perform a DIPS ping on the service running inside the container

The host should mount into the container:
* the dirac.cfg in `/opt/dirac/etc/dirac.cfg`
* the certificates as `/opt/dirac/etc/grid-security`

If running a service, it is necessary to expose a port.

Some environment variables allow to specify alternative repositories in case it is necessary to run with a hotfix:
* `DIRAC_REPO`: git repository from which to checkout DIRAC
* `DIRAC_BRANCH`: branch to use. If unspecified, we use the default branch
* `LHCB_DIRAC_REPO`: git repository from which to checkout LHCbDIRAC
* `LHCB_DIRAC_BRANCH`: branch to use. If unspecified, we use the default branch

These variables can be passed to the container using `--env` options in `docker-run`



## monitoring folder

This contains all the necessary scripts to monitor the containers. They are deployed on all mesos slave with puppet. This should move to another repository eventually...

* simpleParseCAdvisor.py: collects metrics from the local cAdvisor, and send them to an InfluxDB instance. The script is called by a cron job.
* testConfig.json: configuration file example for the database.

## pinger folder

The `pinger` is a container used to monitor the health of the DIRAC container from the outside.
It performs a DIPS ping. It is used by Consul to test the health of the services.
As long as the dips protocol or the underlying SSL does not change, it does not matter if it is
based on an old DIRAC version.

* Dockerfile: the Dockerfile to make it a container
* dirac_ping: simple web server that transforms an HTTP request into a DIPS ping
