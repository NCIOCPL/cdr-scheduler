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
launches them in separate threads. For an overview of how the scheduler
works, start by reading the documentation comments at the top of this
scipt.

## Scheduled Jobs

The [`jobs`](jobs) directory contains the base `Job` class, as well as
the modules implementing the derived classes for each of the jobs
managed by the scheduler.

## Command-Line Testing

Most of the modules used to implement the scheduled jobs have a conditional
block at the bottom to allow the module to be run as a script from the
command-line. To use this feature, open a console window in the parent
of the `jobs` directory and use the `-m` flag to name the module, being
sure the include the package name. For example:

```bash
python -m jobs.file_sweeper_task --test --email kathy@example.com
```