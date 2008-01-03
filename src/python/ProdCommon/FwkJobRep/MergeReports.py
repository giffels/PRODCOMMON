#!/usr/bin/env python
"""
_MergeReports_

Given two FrameworkJobReport XML files, concatenate them into
a single file.

"""
import os
from IMProv.IMProvDoc import IMProvDoc
from ProdCommon.FwkJobRep.ReportParser import readJobReport




def mergeReports(reportFile1, reportFile2):
    """
    _mergeReports_

    Load job reports from both files, and combine them into a
    single file.

    The output will be written to the first file provided.
    (IE JobReports from reportFile2 will be added to reportFile1)

    If reportFile1 does not exist, a new report will be created, containing
    the contents of reportFile2.
    
    If reportFile2 does not exist, then a RuntimeError is thrown.

    """
    if not os.path.exists(reportFile1):
        reports1 = []
    else:
        reports1 = readJobReport(reportFile1)

    if not os.path.exists(reportFile2):
        msg = "Report file to be merged does not exist:\n"
        msg += reportFile2
        raise RuntimeError, msg

    reports2 = readJobReport(reportFile2)
    reports1.extend(reports2)

    
    output = IMProvDoc("JobReports")
    for item in reports1:
        output.addNode(item.save())
    handle = open(reportFile1, 'w')
    handle.write(output.makeDOMDocument().toprettyxml())
    handle.close()
    return



def updateReport(reportFile, newReportInstance):
    """
    _updateReport_

    Given a file containing several reports: reportFile,
    find the report in there whose name matches the newReportInstance's
    name and replace that report with the new Report instance.

    Returns a boolean: True if report name was matched and updated,
    False if the report was not found and updated. (False may indicate that
    the new report file needs to be merged with the main report file)

    """
    if not os.path.exists(reportFile):
        existingReports = []
    else:
        existingReports = readJobReport(reportFile)

    updatedReport = False
    output = IMProvDoc("JobReports")
    for report in existingReports:
        if report.name == newReportInstance.name:
            output.addNode(newReportInstance.save())
            updatedReport = True
        else:
            output.addNode(report.save())

    
    handle = open(reportFile, 'w')
    handle.write(output.makeDOMDocument().toprettyxml())
    handle.close()
    return updatedReport


