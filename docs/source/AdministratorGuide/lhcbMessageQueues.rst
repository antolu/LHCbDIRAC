=======================
Message Queues for LHCb
=======================

The IT Message Queue service has setup two ActiveMQ brokers for us, configured the same way: `lhcb-test-mb` and `lhcb-mb`.

Queues and topics
=================

This is the destinations that we are allowed::

   /queue/lhcb.certification
   /queue/lhcb.dirac.{path}
   /queue/lhcb.jenkins
   /queue/lhcb.test.{name}
   /topic/lhcb.dirac.{path}

where `{path}` can be anything and `{name}` any word without dots.


Authentication & Authorizations
===============================

Authentication can be done via user name, or via host DN.

For convenience, we use a `dirac` user for every interaction.
The IT guys have a list of our machines, but we have to ask them to update it every time we add a machine, so it is not very convenient.



Monitoring
==========

We can see how are our queues doing on this `monitoring link <https://mig-graphite.cern.ch/grafana/dashboard/file/overview.json?orgId=1&from=now-15m&to=now&var-cluster=lhcb>`_

cluster can be either `lhcb` or `lhcb-test`
