:orphan:

=========================================
 ceph-crush-location - get CRUSH location
=========================================

.. program:: ceph-crush-location

Description
===========

:program:`ceph-crush-location` generates a CRUSH location for the given entity.

The  CRUSH location consists of a list of key=value pairs, separated by spaces,
all on a single line.  This describes where in CRUSH hierarhcy this entity
should be placed.

Options
=======

.. option:: --cluster <clustername>

   name of the cluster (see /etc/ceph/$cluster.conf)

.. option:: --type <osd|mds|client>

   daemon/entity type

.. option:: --id <id>

   id (osd number, mds name, client name)

Availability
============

:program:`ceph-crush-location` is part of Ceph, a massively scalable,
open-source, distributed storage system. Please refer to
the Ceph documentation at http://ceph.com/docs for more information.

See also
========

:doc:`ceph-conf <ceph-conf>`\(8),
