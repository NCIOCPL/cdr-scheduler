"""Keep disk space usage under control.

Sweep up obsolete directories and files based on instructions in
a configuration file - deleting, truncating, or archiving files
and directories when required.
"""

import argparse
import datetime
import glob
import os
import os.path
import re
import shutil
import sys
import tarfile
import time
import traceback
import xml.dom.minidom
import lxml.etree as et
import cdr
from cdrapi import db
from cdrapi.settings import Tier
from .base_job import Job

# Filesweeper Logfile (separate from the scheduler's.)
FS_LOGGER = None  # supplied later by the FileSweeper object.

# Don't go wild creating output files
MAX_OUTPUT_FILES_WITH_ONE_NAME = 5

# Size for read/write
BLOCK_SIZE = 4096

# Date constants, YEARS_OLD is max time we'll look back, sanity check
DAY_SECS = 86400
YEAR_DAYS = 365.25
YEARS_OLD = 10
LONG_TIME = DAY_SECS * YEAR_DAYS * YEARS_OLD

# Where are we running?
TIER = Tier().name


class FileSweeper(Job):
    """
    Adapter to allow the overall file clean up task to be driven
    from the CDR scheduler.

    Required jobParam fields:
        ConfigFile  Full or relative path to configuration file.

    Optional jobParam fields:
        TestMode    Boolean value. Create output files but delete nothing.
                    (default False)
        Email       Alternate email list for fatal error msgs.
                    If more than one address, use '+' as separator, no spaces.
                    e.g, joe@nih.gov+bill@nih.gov+jane@somewhere.com
        OutputDir   Optional path to prepend to archive file output directory.
    """

    LOGNAME = "FileSweeper"
    SUPPORTED_PARAMETERS = {"ConfigFile", "TestMode", "Email", "OutputDir"}

    def run(self):

        global FS_LOGGER
        FS_LOGGER = self.logger

        # Gather parameters.
        configFile = self.opts.get('ConfigFile')
        testMode = self.opts.get('TestMode', False)
        email = self.opts.get('Email', '')
        outputDir = self.opts.get('OutputDir', '')

        sweepFiles(configFile, testMode, email, outputDir)


# ---------------------------------------------------------------------
# Class encapsulating actions on one file
# ---------------------------------------------------------------------
class qualFile:

    def __init__(self, fileName):
        # Default values
        self.fileName = fileName

        # Stat file
        fstat = os.stat(self.fileName)

        # Save info
        self.fsize = fstat.st_size
        self.mtime = fstat.st_mtime

        # Nothing done to this file yet
        self.archived = False
        self.truncated = False
        self.deleted = False


# ---------------------------------------------------------------------
# Class encapsulating the elements in a specification
# ---------------------------------------------------------------------
class SweepSpec:

    def __init__(self, specNode):
        """
        Constructor loads SweepSpec from a dom node.

        Pass:
            DOM node of a SweepSpec element in a configuration file.
        """

        # Initialize specification invalid values
        self.specName = "Unknown"  # Name for report
        self.action = None         # What to do with files
        self.root = None           # Where to look for files
        self.inFiles = []          # File paths to look for
        self.outFile = None        # Output file for archive
        self.oldSpec = None        # If at least one file older than this
        self.youngSpec = None      # Files must be older than this
        self.maxSizeSpec = None    # If file bigger than this
        self.truncSizeSpec = None  # Truncate file to this size
        self.customProc = None     # Name of custom sweep routine, if any

        # Start with the assumption that spec applies to all tiers.
        self.tiers = None

        # Set this flag to true when the archive is successfully saved
        self.okayToDelete = False

        # These fields track what actually matches the specification
        # Initialized to invalid values, filled in by self.statFiles()
        self.oldestDate = 0        # Date of oldest file found in root/inFiles
        self.youngestDate = 0      # Date of youngest found
        self.biggestSize = 0       # Size of biggest file found
        self.smallestSize = 0      # Size of smallest
        self.totalList = []        # All names of files found in root/inFiles
        self.qualifiedList = []    # qualFile objects qualified for action
        self.totalBytes = 0        # Total bytes in all files
        self.qualifiedBytes = 0    # Total bytes in qualified files
        self.archivedFiles = 0     # Number successfully archived
        self.archivedBytes = 0        # "        "          "
        self.truncFiles = 0        # Number successfully truncated
        self.truncBytes = 0           # "        "          "
        self.msgs = []             # Messages accrued during processing
        self.statted = False       # Info has been collected

        # All times relative to right now, normalized to previous midnight
        now = normTime(time.time())

        # Find out if this spec only applies to specific tiers.
        tiers = specNode.getAttribute("Tiers")
        if tiers:
            self.tiers = set(tiers.split())

        # Load all significant fields
        for node in specNode.childNodes:
            if node.nodeType == xml.dom.minidom.Node.ELEMENT_NODE:
                elem = node.nodeName
                if elem == 'Name':
                    self.specName = cdr.getTextContent(node)
                elif elem == 'Action':
                    self.action = cdr.getTextContent(node)
                elif elem == 'InputRoot':
                    self.root = cdr.getTextContent(node)
                elif elem == 'InputFiles':
                    for child in node.childNodes:
                        if child.nodeType == xml.dom.minidom.Node.ELEMENT_NODE:
                            if child.nodeName == 'File':
                                self.inFiles.append(
                                  cdr.getTextContent(child))
                            elif child.nodeName == 'Comment':
                                pass
                            else:
                                msg = "Unrecognized element %r in SweepSpec %r"
                                args = child.nodeName, self.specName
                                fatalError(msg.format(*args))
                elif elem == 'OutputFile':
                    self.outFile = cdr.getTextContent(node)
                elif elem == 'Oldest':
                    # Convert to UNIX time = seconds since epoch
                    days = int(cdr.getTextContent(node))
                    self.oldSpec = now - (days * DAY_SECS)
                elif elem == 'Youngest':
                    days = int(cdr.getTextContent(node))
                    self.youngSpec = now - (days * DAY_SECS)
                elif elem == 'Biggest':
                    self.maxSizeSpec = int(cdr.getTextContent(node))
                elif elem == 'Smallest':
                    self.truncSizeSpec = int(cdr.getTextContent(node))
                elif elem == 'CustomProc':
                    self.customProc = cdr.getTextContent(node)
                elif elem == 'Comment':
                    pass
                else:
                    msg = "Unrecognized element %r in SweepSpec %r"
                    fatalError(msg.format(elem, self.specName))

        # Validate
        if self.specName == "Unknown":
            fatalError("No Name subelement in one of the SweepSpec elements")
        if not self.action:
            fatalError(f"No Action in SweepSpec {self.specName!r}")
        if self.action not in ('Archive', 'Delete', 'TruncateArchive',
                               'TruncateDelete', 'Custom'):
            msg = "Invalid Action '{}' in SweepSpec '{}'"
            fatalError(msg.format(self.action, self.specName))
        if self.inFiles == []:
            if self.action != 'Custom':
                msg = "No File (or InputFiles?) in SweepSpec '{}'"
                fatalError(msg.format(self.specName))

        # Validate combinations of specs
        if not self.outFile and self.action in ('Archive', 'TruncateArchive'):
            msg = "No output file specified for SweepSpec {} with Action={}"
            fatalError(msg.format(self.specName, self.action))
        if not (self.oldSpec and self.youngSpec):
            if self.action == 'Archive':
                msg = "Must specify Oldest/Youngest for Archive SweepSpec {}"
                fatalError(msg.format(repr(self.specName)))
        if not (self.maxSizeSpec and self.truncSizeSpec):
            if self.action.startswith('Truncate'):
                msg = "Must specify Biggest/Smallest for Truncate "
                msg += "SweepSpec '{}'"
                fatalError(msg.format(self.specName))
        if self.customProc and self.customProc == 'expireMeetingRecordings':
            if not self.oldSpec:
                msg = "Must specify Oldest for Custom SweepSpec '{}'"
                fatalError(msg.format(self.specName))

        # Times should be reasonable e.g., now until 10 years before now
        if self.oldSpec:
            if self.oldSpec >= now or self.youngSpec and self.youngSpec >= now:
                fatalError('A date >= current date in SweepSpec "%s"' %
                           self.specName)
            longAgo = now - LONG_TIME

            if self.oldSpec and self.oldSpec < longAgo:
                fatalError(
                    f'"Oldest" date is older than {YEARS_OLD:d} years '
                    f'in SweepSpec "{self.specName}"')
            if self.youngSpec and self.youngSpec < longAgo:
                fatalError(
                    f'"Youngest" date is older than {YEARS_OLD:d} years '
                    f'in SweepSpec "{self.specName}"')

        if self.oldSpec and self.maxSizeSpec:
            fatalError("Can't specify both big/small and old/young")

    # ---------------------------------------------------------------------
    # Is this spec to be used on this tier?
    # ---------------------------------------------------------------------
    def active(self):
        """
        Check to make sure this spec is not intended only for other tiers.
        """
        if self.tiers is None:
            return True
        if TIER in self.tiers:
            return True
        return False

    # ---------------------------------------------------------------------
    # Find files matching a spec
    # ---------------------------------------------------------------------
    def statFiles(self):
        """
        Find all files matching a SweepSpec specification.
        Only finds those that qualify "Youngest" or "Smallest" limit.
        Also finds maximum oldest, youngest, biggest, smallest values
          so that caller can decide whether there is anything at all to
          do for the SweepSpec.

        Pass:
            Void

        Return:
            True  = There is at least one file matching the SweepSpec.
            False = There aren't any.
        """
        # Get a list of all files matching all input specs
        for fileSpec in self.inFiles:
            self.totalList.extend(glob.glob((fileSpec)))

        # Were there any files found at all?
        if len(self.totalList) == 0:
            return False

        # Stat each one
        for fileName in self.totalList:

            # Create a stat'ed object for it
            # Force string format in case name is all digits
            fileObj = qualFile(normPath(str(fileName)))

            # Shorthand names for last modified time and file size
            mtime = fileObj.mtime
            fsize = fileObj.fsize

            # Update number of bytes in directory we've examined
            self.totalBytes += fsize

            # Does this file qualify?
            if (self.youngSpec and mtime < self.youngSpec or
                    self.truncSizeSpec and fsize > self.truncSizeSpec):

                # Yes, remember it
                self.qualifiedList.append(fileObj)

                # Update summary dates for SweepSpec
                # XXX Should I erase these if required criterion not met?
                if self.oldestDate == 0 or mtime < self.oldestDate:
                    self.oldestDate = mtime
                if mtime > self.youngestDate:
                    self.youngestDate = mtime

                # Summary of sizes
                if self.smallestSize == 0 or fsize < self.smallestSize:
                    self.smallestSize = fsize
                if self.biggestSize == 0 or fsize > self.biggestSize:
                    self.biggestSize = fsize

                # And cumulate number of bytes we'll remove
                self.qualifiedBytes += fsize
                if self.truncSizeSpec:
                    # Already know file is bigger than max
                    self.qualifiedBytes -= self.truncSizeSpec

        # Signify completion of statFiles
        self.statted = True

        # Was at least one required criterion met?
        if self.oldSpec and self.oldestDate > self.oldSpec:
            return False
        elif self.maxSizeSpec and self.maxSizeSpec > self.biggestSize:
            return False

        # We should take action on this spec
        return True

    # ---------------------------------------------------------------------
    # Stringify entire spec
    # ---------------------------------------------------------------------
    def __str__(self):
        """
        Display spec for debugging purposes.
        """
        # Convert date times for display
        old = young = oldest = youngest = None
        if self.oldSpec:
            old = time.strftime("%Y-%m-%d",
                                time.localtime(self.oldSpec))
            young = time.strftime("%Y-%m-%d",
                                  time.localtime(self.youngSpec))
        if self.oldestDate > 0:
            oldest = time.strftime("%Y-%m-%d",
                                   time.localtime(float(self.oldestDate)))
            youngest = time.strftime("%Y-%m-%d",
                                     time.localtime(self.youngestDate))

        # Basics from config file
        tiers = self.tiers and " ".join(sorted(self.tiers)) or "ALL"
        specStr = f"""
SweepSpec: "{self.specName}"
    Action:     {self.action}
    Tiers:      {tiers}
    InputRoot:  {self.root}
    Files:      {self.inFiles}
    OutputFile: {self.outFile}
    Oldest:     {old}
    Youngest:   {young}
    Biggest:    {self.maxSizeSpec}
    Smallest:   {self.truncSizeSpec}
"""

        # If statFiles() called, report statistics
        if self.statted:
            specStr += f"""
  Statistics:
        oldest:          {oldest}
        youngest:        {youngest}
        biggest:         {self.biggestSize:d}
        smallest:        {self.smallestSize:d}
        total files:     {len(self.totalList):d}
        qualified files: {len(self.qualifiedList):d}
        total bytes:     {self.totalBytes:d}
        qualified bytes: {self.qualifiedBytes:d}
        message count:   {len(self.msgs):d}
"""

        # Messages
        if self.msgs:
            specStr += "Messages:\n"
            i = 1
            for msg in self.msgs:
                specStr += "%2d: %s\n" % (i, msg)
                i += 1

        return specStr

    # ---------------------------------------------------------------------
    # Archive files needing to be archived
    # ---------------------------------------------------------------------
    def archive(self, testMode):
        """
        Copy all qualified files into an archive file.

        If archive is successful, deletes the files.

        Pass:
            testMode
                True  = Don't actually delete file - for debugging
                False = Delete after archiving
        """
        # Don't do anything if there's nothing to do
        if len(self.qualifiedList) == 0:
            return

        # Create the output compressed tar archive
        try:
            tar = tarfile.open(self.outFile, "w:bz2")
        except Exception:
            fatalError(f'Could not open tarfile "{self.outFile}" for writing')

        # Process each qualifying file
        for fobj in self.qualifiedList:
            # Archive it
            try:
                tar.add(fobj.fileName)
            except Exception as info:
                self.addMsg("Tar error (1): %s" % info)
                self.addMsg("FileName: %s" % fobj.fileName)
                self.addMsg("Abandoning this SweepSpec")
                return

            # Stats
            self.archivedFiles += 1

        try:
            tar.close()
        except Exception:
            fatalError('Could not close tarfile "%s"' % self.outFile)

        # If here, everything okay, deletion can proceed
        self.delete(testMode)

    # ---------------------------------------------------------------------
    # Truncate files needing to be truncated
    # ---------------------------------------------------------------------
    def truncate(self, testMode):
        """
        Left truncate any files that exceed the specified maximum
        size, saving the truncated size in place of the original.  By
        "left truncate" we mean delete bytes from the beginning of
        the file, saving bytes that were at the end.

        For TruncateArchive, the removed data is archived to an output
        file, as with the archive() function.  For TruncateDelete, the
        removed data is lost.

        We'll move or copy the file to the output directory
        using a unique name generated by our name generator.

        Truncation is done by copying the "right" end of the
        file back to its original location, overwriting the
        longer original.

        If TruncateDelete'ing, we can delete the copy in the
        output directory.

        If TruncateArchive'ing, we retain it there, where it
        will eventually get compressed.

        Pass:
            Output directory
                Build output archive in this directory.

            testMode
                True  = Don't actually replace file - for debugging
                False = Replace the original file with truncated data
        """
        # Don't do anything if there's nothing to do
        if len(self.qualifiedList) == 0:
            return

        # Create the output compressed tar archive
        if self.action == "TruncateArchive":
            try:
                tar = tarfile.open(self.outFile, "w:bz2")
            except Exception:
                fatalError('Could not open tarfile "%s" for writing'
                           % self.outFile)

        # Process each qualifying file
        for fileObj in self.qualifiedList:

            # Name of the input file
            inFile = fileObj.fileName

            # Create a temporary file for output of the part to be saved
            tmpFile = inFile + ".Truncation"

            # If the temp file exists from a previous run, delete it
            if os.path.exists(tmpFile):
                self.addMsg('Warning: overwriting old temporary output "%s"'
                            % tmpFile)
                os.remove(tmpFile)

            # Truncation point is truncSizeSpec before end of file
            truncPoint = fileObj.fsize - self.truncSizeSpec

            # Copy the part we want to save into the new file
            # This creates the truncated file
            try:
                srcp = open(inFile, "rb")
                destp = open(tmpFile, "wb")
                srcp.seek(truncPoint)
                done = False
                while not done:
                    bytes = srcp.read(BLOCK_SIZE)
                    if bytes:
                        destp.write(bytes)
                    else:
                        done = True
                destp.close()
                srcp.close()
            except Exception as info:
                self.addMsg(f"""\
WARNING: Unable to create truncation of "{self.specName}::{inFile}":
   {info}
   Truncation was aborted""")
                continue

            # Sanity debug checks
            if not os.path.exists(tmpFile):
                fatalError('Temporary file "%s" not found - internal error'
                           % tmpFile)
            tmpstat = os.stat(tmpFile)
            if tmpstat.st_size != self.truncSizeSpec:
                self.addMsg(
                  'WARNING: Temp file "%s" size=%d, but truncsize=%d\n' %
                           (tmpFile, tmpstat.st_size, self.truncSizeSpec) +
                  '     Input file may have changed during processing')

            # If archiving, truncate and save the uncopied part
            #   of the temporary file
            if self.action == "TruncateArchive":

                # But don't truncate if in test mode
                if not testMode:
                    try:
                        srcp = open(inFile, "ab+")
                        srcp.truncate(truncPoint)
                        srcp.close()
                    except Exception as info:
                        self.addMsg('Unable to truncate "%s::%s": %s' %
                                    (self.specName, inFile, info))
                        continue

                    # Sanity debug checks
                    fstat = os.stat(inFile)
                    if fstat.st_size != truncPoint:
                        fatalError(
                         'Truncation file "%s" size=%d, truncsize=%d' %
                         (inFile, fstat.st_size, self.truncSizeSpec))
                    if (tmpstat.st_size + fstat.st_size) != fileObj.fsize:
                        fatalError(
                            f'File "{inFile}": Truncated and '
                            f"remaining sizes {tmpstat.st_size:d} + "
                            f"{fstat.st_size:d} != original size "
                            f"{fileObj.fsize:d}")

                # Archive the truncation
                try:
                    tar.add(inFile)
                except Exception as info:
                    self.addMsg("Tar error: %s" % info)
                    self.addMsg("Abandoning this SweepSpec")

            # If doing it for real, replace the original with the new
            #   truncated version
            if not testMode:
                try:
                    shutil.move(tmpFile, inFile)
                except Exception as info:
                    fatalError('Unable to replace original file "%s":\n   %s'
                               % (inFile, info))

            # If we got this far, the truncation has occurred
            fileObj.truncated = True

        # Close archive
        if self.action == "TruncateArchive":
            try:
                tar.close()
            except Exception:
                fatalError('Could not close tarfile "%s"' % self.outFile)

    # ---------------------------------------------------------------------
    # Delete files
    # ---------------------------------------------------------------------
    def delete(self, testMode):
        """
        Delete all files named in a specification.

        Pass:
            testMode
                True  = Just testing, don't delete anything.
                False = Delete them.
        """
        for fileObj in self.qualifiedList:
            nameIsDir = False
            try:
                if not testMode:
                    if os.path.isdir(fileObj.fileName):
                        # Recursivley remove directory if it's empty
                        nameIsDir = True
                        shutil.rmtree(fileObj.fileName)
                    else:
                        # Remove ordinary file
                        os.remove(fileObj.fileName)
                    fileObj.deleted = True
                else:
                    self.addMsg('Test mode, not deleting "%s"' %
                                fileObj.fileName)
            except Exception as info:
                if nameIsDir:
                    self.addMsg("Error removing directory %s: %s" %
                                (fileObj.fileName, info))
                else:
                    self.addMsg("Unable to remove file %s: %s" %
                                (fileObj.fileName, info))

    # ---------------------------------------------------------------------
    # Create a full output file name
    # ---------------------------------------------------------------------
    def makeOutFileName(self, outPath, testMode):
        """
        Create an absolute path name for an output file in zip or
        tar format.

        Path is stored in self.outFile

        Filename includes dates as follows:
            For Archived files, use the dates of the oldest and youngest
            files in the archive, i.e.:
                filename.YYYYMMDD-YYYYMMDD.tar

            For Archived truncations of files, use today's date, i.e.:
                filename.YYYYMMDD.{tar or zip}

        Pass:
            Output directory to prepend to filename stored in the Spec
                configuration.
        """
        # Construct output base name
        if not (self.outFile.startswith("/") or self.outFile[1:2] == ":/"):
            outFile = os.path.join(outPath, self.outFile)
        else:
            outFile = self.outFile

        # Strip off suffixes that get added later
        if outFile.endswith(".bz2") or outFile.endswith(".BZ2"):
            outFile = outFile[:-4]
        if outFile.endswith(".tar") or outFile.endswith(".TAR"):
            outFile = outFile[:-4]

        # Add appropriate date suffixes
        if self.action == 'Archive':
            # Add dates for oldest file destined for the archive
            outFile += time.strftime(".%Y%m%d",
                                     time.localtime(self.oldestDate))
            outFile += time.strftime("-%Y%m%d",
                                     time.localtime(self.youngestDate))
        else:
            outFile += time.strftime(".%Y%m%d", time.localtime())

        # If running in test mode, indicate that in the output filename
        # Allows us to easily find and delete such files
        if testMode:
            outFile += ".TEST"

        # Add conventional .tar.bz2 suffix
        outFile += ".tar.bz2"

        # Normalize slashes.  Cygwin tar likes forward slashes
        outBase = normPath(outFile)

        # Make sure we don't overwrite existing archive
        outFile = makeFileNameUnique(outBase, MAX_OUTPUT_FILES_WITH_ONE_NAME)

        # Sanity check
        if not outFile:
            fatalError(
                f"Too many output files with base name {outBase!r}, "
                f"in SweepSpec {self.specName!r}")

        self.outFile = outFile

    # ---------------------------------------------------------------------
    # Remove out of date Media recordings
    # ---------------------------------------------------------------------
    def expireMeetingRecordings(self, testMode):
        """
        This is a "Custom" routine that sweeps away MP3 format meeting
        recordings that have passed their useful life.  Implemented for
        JIRA Issue OCECDR-3886.

        Pass:
            testMode
                True  = Don't actually delete any blobs, just report
                False = Update docs and delete blobs.
        """
        cursor = session = None

        # Need a connection to the CDR Server
        session = cdr.login('FileSweeper', cdr.getpw('FileSweeper'))
        if not session:
            FS_LOGGER.error("FileSweeper login to CdrServer failed")
            # But no reason not to do the rest of the sweep
            return

        # And a read-only connection to the database
        try:
            conn = db.connect()
            cursor = conn.cursor()
        except Exception:
            FS_LOGGER.exception("attempting DB connect")

            # But continue with the sweep
            cleanSession(cursor, session)
            return

        # Today's SQL Server date
        try:
            cursor.execute("SELECT GETDATE()")
            now = cursor.fetchone()[0]
        except Exception:
            FS_LOGGER.exception("getting DB date")
            cleanSession(cursor, session)
            return

        # Only want YYYY-MM-DD, not HMS
        nowDate = str(now)[:10]

        # Locate all Media documents linked to meeting recordings that
        #  are older than Oldest days.
        # This is done by checking for any ADD DOCUMENT transaction in the
        #  audit trail for one of the qualifying documents.  If any ADD was
        #  performed before the Oldest value, then there was a version of
        #  the meeting recording from before that date.
        # The Media doc must also be found in one of the ...blob_usage tables.
        #  If not, then any blob associated with it has already been deleted.
        isoFmt = "%Y-%m-%d"
        earlyDate = \
            datetime.datetime.fromtimestamp(self.oldSpec).strftime(isoFmt)

        # DEBUG
        msg = "Looking for meeting recordings older than %s"
        FS_LOGGER.debug(msg, earlyDate)

        qry = """
        SELECT d.id, d.title
          FROM document d
          JOIN query_term qt
            ON qt.doc_id = d.id
          JOIN audit_trail at
            ON at.document = d.id
          JOIN action act
            ON act.id = at.action
         WHERE qt.path = '/Media/MediaContent/Categories/Category'
           AND qt.value = 'meeting recording'
           AND act.name = 'ADD DOCUMENT'
           AND at.dt <= '%s'
           AND (
              d.id IN ( SELECT doc_id FROM doc_blob_usage )
             OR
              d.id IN ( SELECT doc_id FROM version_blob_usage )
           )
        """ % earlyDate

        # Read the info into memory
        try:
            cursor.execute(qry)
            rows = cursor.fetchall()
        except Exception:
            FS_LOGGER.exception("attempting to locate old blobs")
            cleanSession(cursor, session)
            return

        # If there weren't any, that's normal and okay
        if len(rows) == 0:
            FS_LOGGER.info("No meeting recordings needed to be deleted")
            cleanSession(cursor, session)
            return

        # Do we need to lock and load the docs for update?
        checkOut = 'Y'
        if testMode:
            checkOut = 'N'

        # ------------------------------------------------------------------
        # We've got some to delete.
        # For each Media document:
        #  Send a transaction to the CDR Server to do the following:
        #   Add a ProcessingStatus to the Media document to say what happened
        #   Delete all of the blobs.
        # ------------------------------------------------------------------
        for row in rows:

            docId, title = row

            # Fetch the original document
            # We'll do this even in test mode to test the xml mods
            try:
                docObj = cdr.getDoc(session, docId, checkout=checkOut,
                                    getObject=True)
            except Exception:
                FS_LOGGER.exception("attempting to fetch doc %d", docId)
                cleanSession(cursor, session)
                return

            # Test for retrieval error, e.g., locked doc
            err = cdr.checkErr(docObj)
            if err:
                message = "Failed getDoc for CDR ID %s: %s, continuing"
                FS_LOGGER.error(message, docId, err)
                continue

            # Parse the xml preparatory to modifying it
            mediaRoot = et.fromstring(docObj.xml)

            # Create the new Comment field to record what we did
            # Make it the last subelement of the Media document element
            # It has to be there
            comment = et.SubElement(mediaRoot, 'Comment',
                                    audience='Internal',
                                    user='FileSweeper',
                                    date=nowDate)
            comment.text = "Removed meeting recording object after expiration"

            # Back to serial XML
            newXml = et.tostring(mediaRoot)

            # If we're testing, just log what we would have done
            if testMode:
                # For log file
                actionMsg = 'would delete'

            else:
                # Send the doc back to the database:
                #  Wrapped in CdrDoc wrapper
                #  With command to delete all blobs
                actionMsg = 'deleted'
                opts = dict(
                    doc=cdr.makeCdrDoc(newXml, 'Media', docObj.id),
                    comment='Removed meeting recording blobs',
                    delAllBlobVersions=True,
                    check_in=True,
                )
                response = cdr.repDoc(session, **opts)

                # Check response
                if not response[0]:
                    errors = cdr.getErrors(response[1], errorsExpected=True,
                                           asSequence=False)
                    message = "Saving Media xml for doc %s: %s"
                    FS_LOGGER.error(message, docObj.id, errors)
                    FS_LOGGER.info("Aborting expireMeetingRecords()")

                    # Stop doing this, but continue rest of file sweeps.
                    cleanSession(cursor, session)
                    return

            # Log results for this media recording
            args = actionMsg, docId, title
            msg = "FileSweeper %s blobs for cdrId: %s\n%s"
            FS_LOGGER.info(msg, *args)

        # Cleanup
        cleanSession(cursor, session)

    # ---------------------------------------------------------------------
    # Report results via HTML
    # ---------------------------------------------------------------------
    def reportHTML(self):
        """
        Construct HTML to summarize what happened with this SweepSpec

        Return:
            HTML string
        """
        html = f"""
<h3>{self.specName}</h3>
<table width='80%' align='center' border='1'>
 <tr><td>Action</td><td>{self.action}</td></tr>
 <tr><td>Num files examined</td><td>{len(self.totalList)}</td></tr>
 <tr><td>Num files processed</td><td>{len(self.qualifiedList)}</td></tr>
 <tr><td>Num bytes processed</td><td>{self.qualifiedBytes}</td></tr>
"""

        # Any errors or warnings?
        if len(self.msgs) > 0:
            for msg in (self.msgs):
                html += "<tr><td colspan='2'>%s</td></tr>\n" % msg

        html += "</table>\n"

        return html

    # ---------------------------------------------------------------------
    # Process a message
    # ---------------------------------------------------------------------
    def addMsg(self, msg):
        """
        This version appends messages to a list for this spec.
        Might do something else sometime.

        Pass:
            message
        """
        self.msgs.append(msg)


# ---------------------------------------------------------------------
# Load configuration file
# ---------------------------------------------------------------------
def loadConfigFile(fileName):
    """
    Load the configuration file.

    Change in Hawking release: pull configuration from the repository
    if possible, otherwise fall back on disk file.

    Later change: use file on disk if named, otherwise go to the database.

    Pass:
        Path to config file.

    Return:
        Sequence of SweepSpec objects.
        Each object represents one file sweeper specification.

    Fatal error if:
        Unable to find or load file.
        Unable to parse file.
        File contents invalid.
    """

    # Take the configuration from the disk if requested.
    if fileName:
        try:
            dom = xml.dom.minidom.parse(fileName)
            FS_LOGGER.info("loaded config from %s", fileName)
        except Exception as info:
            fatalError("Error loading config file %s: %s" % (fileName, info))

        # Otherwise pull the file from the repository.
    else:
        cursor = db.connect(user="CdrGuest").cursor()
        cursor.execute("""\
            SELECT v.id, MAX(v.num)
              FROM doc_version v
              JOIN doc_type t
                ON t.id = v.doc_type
             WHERE t.name = 'SweepSpecifications'
               AND v.val_status = 'V'
          GROUP BY v.id""")
        rows = cursor.fetchall()
        if len(rows) > 1:
            fatalError("More than on SweepSpecifications document found")
        elif len(rows) == 1:
            doc_id, version = rows[0]
            query = db.Query("doc_version", "xml")
            query.where(query.Condition("id", doc_id))
            query.where(query.Condition("num", version))
            row = query.execute(cursor).fetchone()
            args = doc_id, version
            try:
                dom = xml.dom.minidom.parseString(row[0].encode("utf-8"))
                msg = "loaded config from CDR%d version %d"
                FS_LOGGER.info(msg, *args)
            except Exception:
                msg = "Failure parsing config document CDR%d version %d"
                FS_LOGGER.exception(msg, *args)

    # List of loaded specifications
    spec = []

    # Load specifications
    docElem = dom.documentElement
    if dom.documentElement.nodeName != 'SweepSpecifications':
        fatalError("SweepSpecifications not found at root of config file %s"
                   % fileName)

    for node in docElem.childNodes:
        if node.nodeType == xml.dom.minidom.Node.ELEMENT_NODE:
            if node.nodeName == 'SweepSpec':
                ss = SweepSpec(node)
                if ss.active():
                    spec.append(ss)

    return spec


# ---------------------------------------------------------------------
# Normalize a path
# ---------------------------------------------------------------------
def normPath(path):
    return re.sub(r"\\", r"/", path)


# ---------------------------------------------------------------------
# Normalize a time value in seconds to the nearest previous midnight
# Converts UTC to local time and makes the change
# ---------------------------------------------------------------------
def normTime(timeVal):
    return (timeVal - (timeVal % DAY_SECS) + time.altzone - DAY_SECS)


# ---------------------------------------------------------------------
# Make a filename unique by adding a suffix if needed
# ---------------------------------------------------------------------
def makeFileNameUnique(inFile, maxSuffix=1):
    """
    Check if an input absolute or relative file name is unique.
    If so:
        Return it.
    Else:
        Try to make it unique by appending a suffix like .01, .02, etc.
        to it.  But don't exceed some reasonable number of tries.

    Pass:
        Input file name.
        Max allowable suffix number.

    Return:
        Filename.  Maybe be modified from original.
        None if no unique name could be generated within the passed
          constraint.
    """
    # Sanity check
    if maxSuffix < 1:
        fatalError('makeFileNameUnique maxSuffix value error\n' +
                   ' inFile="%s", maxSuffix=%d' % (inFile, maxSuffix))

    # Generate names until we create a unique one
    fileNum = 1
    outFile = inFile
    while os.path.exists(outFile):
        outFile = f"{inFile}.{fileNum:02d}"
        fileNum += 1

    # Did we get to a surprising number of files with the same name
    if fileNum > maxSuffix:
        # If they're test files, it's okay, otherwise let's complain
        if ".TEST" not in outFile:
            return None

    return outFile


# ---------------------------------------------------------------------
# Cleanup CdrServer and database connections
# ---------------------------------------------------------------------
def cleanSession(cursor, session):
    """
    If there is a session or cursor open, close it appropriately.  If
    operation fails, log failure and continue.

    Pass:
        session - Open CDR session or None.
        cursor  - Open database cursor or None.
    """
    if cursor:
        try:
            cursor.close()
        except Exception:
            FS_LOGGER.exception("failure closing db cursor")

    if session:
        try:
            cdr.logout(session)
        except Exception:
            FS_LOGGER.exception("failure logging out of session")


# ---------------------------------------------------------------------
# Fatal error
# ---------------------------------------------------------------------
def fatalError(msg):
    """
    Log and display error message.
    Then exit.

    Pass:
        Error message.
    """
    global recips, TIER

    # Add message to log file
    msg = f"FATAL error: {msg}\n"
    FS_LOGGER.error(msg)

    # Send mail to recipients from command line or registered group
    sender = 'FileSweeper-NoRepy@cdr.cancer.gov'
    if not recips:
        try:
            group = "FileSweeper Error Notification"
            recips = Job.get_group_email_addresses(group)
        except Exception:
            FS_LOGGER.exception("Getting email recipients from the CDR")

    # Message subject
    subject = "FileSweeper failed on %s tier" % TIER

    # Body
    errorBody = """
The CDR FileSweeper failed on %s at %s.

Error message was:
   %s
""" % (TIER, time.ctime(), msg)

    # Send it
    mailSent = False
    if recips:
        try:
            opts = dict(subject=subject, body=errorBody)
            cdr.EmailMessage(sender, recips, **opts).send()
            mailSent = True
        except Exception:
            FS_LOGGER.exception("Attempting to send mail for fatal error")

    if mailSent:
        FS_LOGGER.info("Mail sent to: %s", recips)
    else:
        FS_LOGGER.info("No mail sent")

    raise Exception(errorBody)


# ---------------------------------------------------------------------
# Beginning of common logic.  This script actually has two entry points,
# one at the bottom of the file (__name__ == '__main_' ) allowing the
# script to be run from the command line, and another via the FileSweeper
# task class.
# ---------------------------------------------------------------------
def sweepFiles(passedConfigFile, passedTestMode=False, passedRecips=None,
               passedOutputDir=""):

    # We want to work with the global versions of these variables.
    global configFile, testMode, recips, outputDir
    configFile = passedConfigFile
    testMode = passedTestMode
    recips = passedRecips
    outputDir = passedOutputDir

    # Don't allow two filesweepers to run at the same time
    lockFileName = f"{cdr.DEFAULT_LOGDIR}/FileSweeper.lockfile"
    needToReleaseLockFile = True
    try:
        if not cdr.createLockFile(lockFileName):
            needToReleaseLockFile = False
            fatalError("""
It appears that another copy of FileSweeper is currently running.

Only one copy may run at a time.

If you are _certain_ that no other copy is running, then manually
remove the file "%s" to enable FileSweeper to run.
""" % lockFileName)

        # Run separator in log file
        FS_LOGGER.info("  %s", " Beginning File Sweep ".center(46, "-"))
        FS_LOGGER.info("  Args:\n%s", sys.argv)

        # Load the configuration file, fatal if fails
        specList = loadConfigFile(configFile)

        # Current working directory
        cwd = os.getcwd()

        # Create absolute output directory path
        if outputDir:

            # Normalize path name
            outputDir = normPath(outputDir)

            # If output directory is relative, prepend cwd
            if not (outputDir[0:1] == "/" or outputDir[1:2] == ":/"):
                outputDir = normPath(os.path.join(cwd, outputDir))

        else:
            # Output directory not specifed.  Use the current working directory
            outputDir = normPath(cwd)

        # Output directory must exist
        if not os.path.exists(outputDir):
            # Try to make it
            try:
                os.makedirs(outputDir)
            except Exception as info:
                fatalError(
                  """Directory "%s" does not exist, can't create it: %s"""
                  % (outputDir, info))
        if not os.path.isdir(outputDir):
            fatalError('Command line output name "%s" is not a directory'
                       % outputDir)

        # DEBUG
        # fatalError("Aborting for test")

        # Process each archive specification
        try:
            for spec in specList:

                # Custom actions don't necessarily look at files
                if spec.action == 'Custom':
                    # Jump table for custom procs
                    if spec.customProc == 'expireMeetingRecordings':
                        spec.expireMeetingRecordings(testMode)

                    # elif: next goes here
                    else:
                        msg = "CustomSpec 'expireMeetingRecordings' unknown"
                        FS_LOGGER.error(msg)

                    # Custom routines don't use any standard facilities
                    continue

                # Change to input file root directory
                try:
                    os.chdir(spec.root)
                except Exception as info:
                    spec.addMsg("Unable to cd to root: %s" % info)
                    spec.addMsg('"%s" not processed' % spec.specName)
                    continue

                # Find files to process
                if spec.statFiles():

                    # If we're archiving files, process output filename
                    if spec.outFile and spec.outFile.find("Delete") == -1:

                        # Combine command line path with stored output path
                        spec.makeOutFileName(outputDir, testMode)

                        # Create the directory path if necessary
                        # Already created outputDir, but we may need more
                        (fileBase, fileName) = os.path.split(spec.outFile)
                        if not os.path.exists(fileBase):
                            try:
                                os.makedirs(fileBase)
                            except Exception as info:
                                fatalError('Error creating directory "%s": %s'
                                           % (fileBase, info))
                        if not os.path.isdir(fileBase):
                            msg = 'Config output name "{}" is not a directory'
                            fatalError(msg.format(fileBase))

                    # Perform action
                    if spec.action == "Archive":
                        spec.archive(testMode)
                    elif spec.action.startswith("Truncate"):
                        spec.truncate(testMode)
                    else:
                        spec.delete(testMode)

                    # Back to where we started
                    try:
                        os.chdir(cwd)
                    except Exception:
                        fatalError(f'SweepSpec "{spec.specName}" could '
                                   f'not return to directory "cwd"'
                                   " - can't happen")

                # Report results to log file
                FS_LOGGER.info(str(spec))

            # Print a finished separator in log file
            # If a single step takes a very long time (i.e. archiving the
            # Job???? directories) it might appear that nothing is written
            # to the file and therefore the job is done.
            FS_LOGGER.info("  %s\n", " Finished File Sweep ".center(46, "-"))

        except Exception as info:
            sys.stderr.write('Exception halted processing on: %s' % str(info))
            traceback.print_exc(file=sys.stderr)
            FS_LOGGER.exception("Failure")

    # The scheduler (ideally) never exits, so we must remove the
    # lock file explicitly. Don't remove it if the lock might belong
    # to another running process.
    finally:
        if needToReleaseLockFile:
            cdr.removeLockFile(lockFileName)


# ---------------------------------------------------------------------
#                           MAIN
# ---------------------------------------------------------------------
if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--test", "-t", action="store_true",
                        help="create output files but delete nothing")
    parser.add_argument("--email", "-e",
                        help="separate multiple addresses with + (no spaces)")
    parser.add_argument("--config-file", "-c", help="optional config file")
    parser.add_argument("--output-dir", "-o",
                        help="optional path to prepend to output directory")
    opts = parser.parse_args()
    recips = opts.email.split("+") if opts.email else None
    FS_LOGGER = cdr.Logging.get_logger(FileSweeper.LOGNAME)
    sweepFiles(opts.config_file, opts.test, recips, opts.output_dir or "")
