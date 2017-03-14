=================
Release 0.167-t
=================

Presto 0.167-t is equivalent to Presto release 0.167, with some additional features and patches.

**TIMESTAMP limitations**

Presto supports a granularity of milliseconds for the ``TIMESTAMP`` datatype, while Hive
supports microseconds.

**TIMESTAMP semantic changes**

Teradata distribution of Presto does introduce fix of semantic of ``TIMESTAMP`` type.

Previously ``TIMESTAMP`` was a type describing instance of time in Presto session time zone.
Currently Presto treats this - as specified SQL standard - as set of following fields representing wall time:

 * ``YEAR OF ERA``
 * ``MONTH OF YEAR``
 * ``DAY OF MONTH``
 * ``HOUR OF DAY``
 * ``MINUTE OF HOUR``
 * ``SECOND OF MINUTE`` - as decimal with precision 3

For that reason ``TIMESTAMP`` value is not linked with session time zone in any way until time zone is needed explicitly.
Those scenarios include casting to ``TIMESTAMP WITH TIME ZONE`` (and ``TIME WITH TIME ZONE``). In those scenarios
time zone offset of session time zone and session time is applied as specified in SQL standard.

For various compatibility reasons, when casting from data time type without time zone to one with time zone, fixed time
zone is used as opposed to named one that may be set for session.
eg. with ``-Duser.timezone="Asia/Kathmandu"`` on CLI

 * Query: ``SELECT CAST(TIMESTAMP '2000-01-01 10:00' AS TIMESTAMP WITH TIME ZONE);``
 * Previous result: ``2000-01-01 10:00:00.000 Asia/Kathmandu``
 * Current result: ``2000-01-01 10:00:00.000 +05:45``

**TIME semantic changes**

Similar changes as for ``TIMESTAMP`` types were applied to ``TIME`` type.

**TIME WITH TIME ZONE semantic changes**

Due to compatilibity changes having TIME WITH TIME ZONE completly aligned with SQL standard was not possible at this point.
For that reason, when calculating offset of time zone in which TIME WITH TIME ZONE is, Teradata distribution of Presto does
use session start date and time.

This can be visible in queries using ``TIME WITH TIME ZONE`` in time zone that had historical time zone policy changes or uses DST.
eg. With session start time on 1 March 2017

 * Query: ``SELECT TIME '10:00:00 Asia/Kathmandu' AT TIME ZONE 'UTC'``
 * Previous result: ``04:30:00.000 UTC``
 * Current result: ``04:15:00.000 UTC``

**Bugfixes**

 * ``current_time`` and ``localtime`` functions were fixed
