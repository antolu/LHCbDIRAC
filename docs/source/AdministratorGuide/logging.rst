===================
Centralized logging
===================

TL;DR
=====

All the logs (up to the DEBUG level) from the Agents and services are visible on `lblogs.cern.ch`, and are stored in the directory `/logdata/dirac`. They are aggregated per components.

More details
============

Each and every component send their logs at the DEBUG level in a message queue. This is configured using the `message queue backend <https://dirac.readthedocs.io/en/latest/DeveloperGuide/AddingNewComponents/Utilities/gLogger/Backends/index.html#messagequeuebackend>`_ , and the queue is described in the `MQServices resources <https://dirac.readthedocs.io/en/latest/AdministratorGuide/DIRACSites/MessageQueues/index.html?highlight=MQServices#message-queues>`_

The logs are then consumed by a logstash server, and dumped into the file. This is configured in the `ai-puppet-hostgroup-volhcb repository <https://gitlab.cern.ch/ai/it-puppet-hostgroup-volhcb>`_.
