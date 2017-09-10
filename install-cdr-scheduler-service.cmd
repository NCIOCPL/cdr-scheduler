REM Install the CDR Scheduler Service with robust recovery settings
SET NSSM=D:\cdr\Bin\nssm.exe
SET SERVICE=CDRScheduler
%NSSM% remove %SERVICE% confirm
%NSSM% install %SERVICE% D:\Python\python.exe
%NSSM% set %SERVICE% AppDirectory D:\cdr\Scheduler
%NSSM% set %SERVICE% AppParameters D:\cdr\Scheduler\cdr_scheduler.py
%NSSM% set %SERVICE% AppEnvironmentExtra PYTHONPATH=D:\cdr\lib\Python NDSCHEDULER_SETTINGS_MODULE=settings
%NSSM% set %SERVICE% DisplayName CDR Scheduler
%NSSM% set %SERVICE% Description Manages CDR tasks
%NSSM% set %SERVICE% AppNoConsole 1
%NSSM% set %SERVICE% AppExit Default Restart
%NSSM% set %SERVICE% AppThrottle 10000
%NSSM% set %SERVICE% Start SERVICE_AUTO_START
%NSSM% set %SERVICE% AppStdout D:\cdr\log\scheduler-service.log
%NSSM% set %SERVICE% AppStderr D:\cdr\log\scheduler-service.log
%NSSM% set %SERVICE% AppStdoutCreationDisposition 4
%NSSM% set %SERVICE% AppStderrCreationDisposition 4
%NSSM% set %SERVICE% AppRotateFiles 1
%NSSM% set %SERVICE% AppRotateOnline 0
%NSSM% set %SERVICE% AppRotateSeconds 2592000
%NSSM% set %SERVICE% AppRotateBytes 10000000
