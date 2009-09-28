#!/usr/bin/env python
"""
_ReportState_

Tools for checking details of a framework job report file


"""

from ProdCommon.FwkJobRep.ReportParser import readJobReport


def checkSuccess(jobReportFile):
    """
    _checkSuccess_

    Read a FrameworkJobReport XML file and verify that all
    reports evaulate as successful.

    If a report is not successful, False is returned (JobFailed)

    If all reports in the file are successful, True is returned (JobSuccess)

    """
    try:
        reports = readJobReport(jobReportFile)
    except:
        # exception indicates bad report file => Implies failure
        return False

    if len(reports) == 0:
        return False

    for report in reports:
        if report.wasSuccess():
            continue
        else:
            return False
    return True

    
