REM Install the new CDR Scheduler Service with robust recovery settings
SET NSSM=D:\cdr\Bin\nssm.exe
SET SERVICE=CDRNewScheduler
%NSSM% remove %SERVICE% confirm
%NSSM% install %SERVICE% D:\Python\python.exe
%NSSM% set %SERVICE% AppDirectory D:\cdr\NewScheduler
%NSSM% set %SERVICE% AppParameters D:\cdr\NewScheduler\cdr_scheduler.py
%NSSM% set %SERVICE% DisplayName CDR New Scheduler
%NSSM% set %SERVICE% Description Manages Scheduled CDR Jobs
%NSSM% set %SERVICE% AppNoConsole 1
%NSSM% set %SERVICE% AppExit Default Restart
%NSSM% set %SERVICE% AppThrottle 10000
%NSSM% set %SERVICE% Start SERVICE_AUTO_START
%NSSM% set %SERVICE% AppStdout D:\cdr\log\NewSchedulerService.log
%NSSM% set %SERVICE% AppStderr D:\cdr\log\NewSchedulerService.log
%NSSM% set %SERVICE% AppStdoutCreationDisposition 4
%NSSM% set %SERVICE% AppStderrCreationDisposition 4
%NSSM% set %SERVICE% AppRotateFiles 1
%NSSM% set %SERVICE% AppRotateOnline 0
%NSSM% set %SERVICE% AppRotateSeconds 2592000
%NSSM% set %SERVICE% AppRotateBytes 10000000
