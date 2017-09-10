"""
    Custom exceptions for the CDR Scheduler.
"""

class TaskException(Exception):
    "Custom exception for errors in a CDR task."
    pass

class JobException(Exception):
    "Custom exception for errors in a CDR job."
    pass
