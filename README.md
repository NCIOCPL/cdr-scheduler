# CDR Scheduler

This repository contains the software for the service which replaced
the functionality formerly provided by the unreliable Windows
Scheduler, for running CDR jobs at repeated intervals according to
configured schedules. The implementation builds on the [Advanced
Python Scheduler package](https://apscheduler.readthedocs.io/).

## Installer

The [`install-cdr-scheduler-service.cmd`](install-cdr-sacheduler-service.cmd)
script registers the CDR Scheduler as a Windows service and sets the options
for the service.

## Scheduler Service Script

The [`cdr_scheduler`](cdr_scheduler.py) module checks every few
seconds to see if there are any jobs due to be run and if so, it
launches them in separate threads. For an overview of how the scheduler
works, start by reading the documentation comments at the top of this
module.

## Scheduled Jobs

The [`jobs`](jobs) directory contains the base `Job` class, as well as
the modules implementing the derived classes for each of the jobs
managed by the scheduler. The base class provides common values and
utilities, such as a logger property and methods for manipulating HTML
reports and fetching email addresses for a named group.

The following jobs are available for scheduling.

### Batch Job Queue

Some processing jobs (such as complex reports) requested through the CDR administrative web interface take too long to process before the web server times out the request. To work around this limitation, such jobs are queued for batch processing. The [*Batch Job Queue*](jobs/batch_job_queue.py) task is scheduled to check this queue every two minutes for pending jobs and run them.

### Check Media Files

This job (implemented by the [`media`](jobs/media.py) module) compares the media files in the Akamai rsync directory with those generated from the blobs last published for the CDR Media documents. If there are any discrepancies or errors, an email message is sent to the developers. Akamai provides the content delivery network (CDN) which serves up the static files used by the cancer.gov web site. The following parameter options are all optional.

* `fix` - set to any non-empty string to cause the discrepancies to be corrected, so that the file system has the media files it should; by default the job only checks and reports the discrepancies
* `nolock` - set to any non-empty string to prevent the media directory from being renamed to media.lock during processing so that no other job can run which modifies the media files (should probably not be used except during development testing, as this option could introduce unpredictable behavior, or leave the file system in an incorrect state); ignored if the `fix` option is set
* `recip` - overrides the default recipient list for any email notification sent out; multiple recipients can be specified, separated by commas and/or spaces
* `rsync` - set to any non-empty string to cause `rsync` to be run to give any changes to Akamai
* `force` -  set to any non-empty string to cause `rsync` to be run even if there are no discrepancies detected between the file system and the repository
* `debug` - set to any non-empty string to cause each Media document checked to be logged

### Dictionary Loader

Two scheduled [tasks](jobs/dictionary_api_loader.py) are run nightly to load dictionary data to the ElasticSearch server which supports the cancer.gov digital platform. One of the tasks loads the cancer glossary dictionaries, and the other loads the cancer drugs dictionary. Email notification is sent to report successful completion of the loading jobs or any problems which may have occurred.

### Disk Space Check

This [task](jobs/disk_space.py) is run every hour on the CDR DEV server to check the available disk space on the CDR servers on all tiers. If the available space on any checked drive drops below the threshold configured for that drive, or if the check fails to get a response from any of the servers, an email alert is sent to the CDR developers.

### Dynamic Listing Pages Loader

A [nightly data-processing run](jobs/trial_info.py) producing a set of documents mapping EVS concept IDs to concept names and pretty-url name segments. These documents are used for looking up information to create one of the trial listing pages on cancer.gov. Two document types are stored in ElasticSearch:

* `ListingInfo` contains the metadata from combining EVS records with override data
* `LabelInformation` contains "identifier" and label for specific pretty-url names (typically — but not always — trial types)

For more information on the detailed logic implemented by the job, see the [requirements document](https://github.com/NCIOCPL/clinical-trials-listing-api/issues/2).

### Expiring Links

Drug Information Summaries contain links whose usefulness degrades over time (for example, links to older blog posts). There are two scheduled jobs which address this problem, [one](jobs/expiring_links.py) to use the global change harness to strip the links which have outlived their relevance, and the [other](jobs/expired_links.py) to report on the links whose removal by the first job is pending, in order to allow any links which should not be removed to be edited to prevent that removal. By default, links which are over three years old are removed. This can be overridden by supplying a specific date for the `cutoff` parameter. There is also a parameter to run the global change removal in test mode so the results can be reviewed without modifying the documents.

For more details on the job which removes older links, see ticket [OCECDR-5027](https://tracker.nci.nih.gov/browse/OCECDR-5027). For requirements of the report job, refer to ticket [OCECDR-5078](https://tracker.nci.nih.gov/browse/OCECDR-5078).

### Glossifier Refresh Service

This nightly [job](jobs/glossifier_refresh.py) pushes the data for the PDQ Glossary to each of the CMS servers registered for the current CDR tier. This data supports automated support for interactive glossification of content on those servers.

### Gov Delivery Weekly Reports

Two reports are sent via email by this [job](jobs/gov_delivery_reports.py) each week containing information about new and updated PDQ documents in English and Spanish, respectively. The information in the tables contained in these reports is used to populate the reports sent out to subscribers registered with the [govdelivery.com service](https://public.govdelivery.com/accounts/USNIHNCI/subscriber/topics).

### Hoover

This nightly [job](jobs/file_sweeper_task.py) manages disk space usage on the CDR servers by sweeping up obsolete directories and files based on instructions in a configuration file - deleting, truncating, or archiving files and directories when required. The configuration is stored in the CDR repository as a singleton document of type *SweepSpecifications*, though it is possible for testing to instruct the job to use configuration stored in a named document in the file system. A lock is used to prevent two simultaneous sweep jobs from running at the same time. If that lock is somehow left in place by a failed run, and you are certain that that failed job is no longer executing, you can break that lock using the CDR Admin web utility provided for that purpose.

### Notify PDQ Data Partners

This [job](jobs/data_partners.py), which is scheduled to run at 3:00 p.m. each Monday on the production server, sends notifications to the active PDQ data partners. Most partners will just get the notification that fresh PDQ data is ready to be retrieved. Test accounts which are nearing their expiration dates will also get warnings about the upcoming expiration. Test accounts which have passed their expiration dates will only get a notification that their accounts have expired.

The database table for the notification is updated to reflect dates of notification. Expiration of a test account results in a modification of the Licensee document. The actual disabling of login access to the s/FTP server is a separate step handled by CBIIT at our request.

### PCIB Report

A management [report](jobs/pcib_stats.py) sent out at the beginning of each month to list a variety of counts (typically run for the previous month) regarding the number of documents published, updated, etc. See ticket [OCECDR-3478](https://tracker.nci.nih.gov/browse/OCECDR-3478) for the original requirements for this report.

### PDQ Access Report

This [job](jobs/pdq_access_log.py) runs on the first day of each month on the development server to refresh the logs from the s/FTP server and to parse those logs, generating and emailing a report on the activity of the PDQ data parters, showing login and data retrieval requests submitted during the previous month by those partners.

### PDQ Partner List

An email tabular [report](jobs/licensee_list.py) sent out each month showing the active and test PDQ partner accounts, with columns for the CDR ID of the document for the partner account, the partner name, the partner status, and dates for the various statuses of each account.

### Production Server Health Check

A [job](jobs/health_check.py) is run every minute on the CDR development server to verify that the production server is available and responding to requests, and if it is not the job sends email notification to the *Developers Notification* distribution list including any information on the problems which it has been able to collect. Followup notifications are sent hourly until the problems have been resolved.

### Publishing Tasks

There are several scheduled jobs for managing publication events. A weekly [job](jobs/publishing_task.py) is run every Friday afternoon to do a full export of all publishable documents, push the new and changed documents to the cancer.gov CMS, provide a complete set for s/FTP retrieval by the PDQ data partners, and sync the media files to the Akamai CDN. A scaled-back version of that job, limited to a subset of the publishable documents, is run each evening from Monday through Thursday. A separate [publishing queue monitor](jobs/publishing_queue.py) is run every few minutes to identify and launch publishing jobs marked as "Ready," changing that status to "Started."

### Recent CT.gov Trials

A scheduled [job](jobs/recent_trials.py) runs daily to make sure we have information (NCT IDs, date received, trial title, phase information, other IDs, and sponsors) extracted from all the clinical trials recently recorded at [NLM's trial registry](https://clinicaltrials.gov/), so that this information can be used for maintaining drug termiinogy documents in the CDR.

### Refresh Sitemap

Given an input CSV file of CDRID, a key representing the glossary name or drug dictionary and language, this [job](jobs/sitemap.py) creates a sitemap file each week following the [sitemap protocol format](https://www.sitemaps.org/protocol.html) and uploads it to the Akamai netstorage location. The job is currently running only on the development tier, as it has not yet been tested or given approval for promotion to the production server.

### Restart Scheduler

There is a [job](jobs/stop_scheduler.py) which can be invoked on demand for stopping the scheduler process, which causes it to be restarted. This is useful when the scheduler has gotten stuck, is using cached values which are out of date, or is otherwise behaving incorrectly.

### Scheduled Job List

This [job](jobs/job_list.py) emails the list of jobs registered with the scheduler, with the jobs appearing in order of each job will have its next execution, and jobs which are disabled appearing at the end of the list. The CDR administrative interface also has a button on the [page for managing the scheduled jobs](https://cdr.cancer.gov/cgi-bin/cdr/Scheduler.py) which can be used to export a JSON list of registered jobs. It is a good idea to use at least one of these tools regularly so that the schedule can be easily restored in the event jobs are inadvertently removed from the database.

### Sync s/FTP Logs

This [job](jobs/sftp_logs.py) is run each day on the CDR development server to refresh its copy of the logs on the s/FTP server and to use those log entries to maintain a cumulative normalized log of login and PDQ document retrieval requests.

### Translation Job Reports

The CDR maintains separate translation job queues for Glossary, Media, and Summary documents. Weekly reports are sent out via email to the Spanish Translation Team leads for Glossary and Media documents in their respective queues with the status of *Ready for Translation*. In addition, a [nightly job](jobs/translation_jobs.py) runs for each of the three queues to send email reports to each user assigned one or more documents in that queue for translation. These email reports contain one table for each state in which jobs assigned to that user exist.

## Command-Line Testing

Most of the modules used to implement the scheduled jobs have a conditional
block at the bottom to allow the module to be run as a script from the
command-line. To use this feature, open a console window in the parent
of the `jobs` directory and use the `-m` flag to name the module, being
sure the include the package name. For example:

```bash
python -m jobs.file_sweeper_task --test --email kathy@example.com
```
