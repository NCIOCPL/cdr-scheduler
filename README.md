# CDR Scheduler

This repository contains the software for the service which replaced
the functionality formerly provided by the unreliable Windows
Scheduler, for running CDR jobs at repeated intervals according to
configured schedules. The implementation builds on the [Advanced
Python Scheduler package](https://apscheduler.readthedocs.io/).

## Installer

The [`install-cdr-scheduler-service.cmd`](install-cdr-sacheduler-service.cmd)
script registers the CDR Scheduler as a Windows service.

## Scheduler Service Script

The [`cdr_scheduler.py`](cdr_scheduler.py) script checks every few
seconds to see if there are any jobs due to be run and if so, it
launches them in separate threads.

## Scheduled Jobs

The [`jobs`](jobs) directory contains the base `Job` class, as well as
the modules implementing the derived classes for each of the jobs
managed by the scheduler.
