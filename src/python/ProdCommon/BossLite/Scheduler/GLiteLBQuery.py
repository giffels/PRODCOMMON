#!/usr/bin/env python
"""
_GLiteLBQuery_
GLite LB query functions
"""

__revision__ = "$Id: GLiteLBQuery.py,v 1.10 2008/04/02 12:23:50 gcodispo Exp $"
__version__ = "$Revision: 1.10 $"

import sys
import os
from socket import getfqdn
from glite_wmsui_LbWrapper import Status
import Job as lbJob


def config():
    """
    some basic env handling
    """
    try:
        import warnings
        warnings.simplefilter("ignore", RuntimeWarning)
        warnings.simplefilter("ignore", DeprecationWarning)
    except:
        pass
    
    try:
        gliteLocation = os.environ['GLITE_LOCATION']
        libPath = os.path.join(gliteLocation, "lib")
        sys.path.insert(0, libPath)
    except StandardError :
        print "GLITE_LOCATION variable not set. "

    try:
        gliteWmsLocation = os.environ['GLITE_WMS_LOCATION']
        libPath = os.path.join(gliteWmsLocation, "lib")
        sys.path.insert(0, libPath)
    except StandardError :
        print "Warning: the GLITE_LOCATION variable is not set.\n"


def getJobInfo( jobidInfo, states ):
    """
    fill job dictionary with LB informations
    """

    statusMap = {
        'Undefined':'UN',
        'Submitted':'SU',
        'Waiting':'SW',
        'Ready':'SR',
        'Scheduled':'SS',
        'Running':'R',
        'Done':'SD',
        'Cleared':'E',
        'Aborted':'A',
        'Cancelled':'K',
        'Unknown':'UN',
        'Done(failed)':'DA'   
        }
        
    result = {}
    jobid = str(jobidInfo[states.index('Jobid')])
    try:
        result['statusScheduler'] = str(jobidInfo[states.index('Status')])
    except StandardError :
        raise sys.exc_info()[1].__str__()
    
    try:
        result['statusReason'] = str(jobidInfo[states.index('Reason')])
    except StandardError :
        pass
    
    try:
        wms = str( jobidInfo[states.index('Network server')] )
        if wms != '' :
            wms = wms.replace( "https://", "" )
            tmp = wms.split(':')
            result['service'] = "https://" + getfqdn ( tmp[0] ) + ':' + tmp[1]
    except StandardError :
        pass
    
    try:
        dest_ce = str(jobidInfo[states.index( 'Destination' )])
        result['destination'] = dest_ce.replace("https://", "")
#        result['DEST_CE'] = dest_ce.split(':')[0].replace("https://", "")
    except StandardError :
        pass
    
    try:
        timestamp = str(jobidInfo[states.index('Stateentertimes')])
        pos = timestamp.find(result)
        result["lbTimestamp"] = timestamp[
            timestamp.find('=', pos)+1:timestamp.find(' ', pos)
            ]
    except StandardError :
        pass
    
    try:
        if result['statusScheduler'] == 'Done' \
               and jobidInfo[ states.index('Done code') ] != '0' :
            result['statusScheduler'] = 'Done(failed)'
    except StandardError :
        pass

    result['status'] = statusMap[result['statusScheduler']]

    return jobid, result


def checkJobs( job_list, userProxy='' ):
    """
    check a list of provided job id
    
    return: map with key=gridid and the value is the map of jobs attributes
    """

    st = 0
    jobs = []
    ret_map = {}
    if len( job_list ) == 0:
        return
    elif type( job_list ) == str :
        jobs = [ job_list ]
    elif type( job_list ) == list :
        jobs = job_list

    if userProxy != '':
        os.environ["X509_USER_PROXY"] = userProxy

    # instatiating status object
    status =   Status()

    # Loading dictionary with available parameters list    
    jobStatus = lbJob.JobStatus (status)
    states = jobStatus.states_names

    for jobid in jobs:
        try:
            jobid = str( jobid ).strip()
            if len(jobid) == 0 :
                continue
            status.getStatus(jobid, 0)
            err, apiMsg = status.get_error()
            if err :
                print jobid, apiMsg
                continue
            jobidInfo = status.loadStatus(st)
            jobid, values = getJobInfo(jobidInfo, states )
            ret_map[ jobid ] = values
            st = st + 1
        except StandardError :
            print jobid, sys.exc_info()[1].__str__() 
    return ret_map



def checkJobsBulk( job_list, userProxy='' ):
    """
    check a list of provided job parent ids
    
    return: map with key=gridid and the value is the map of jobs attributes
    """
    
    st = 0
    jobs = []
    ret_map = {}
    if len( job_list ) == 0:
        return
    elif type( job_list ) == str :
        jobs = [ job_list ]
    elif type( job_list ) == list :
        jobs = job_list

    if userProxy == '':
        userProxy = '/tmp/x509up_u' + str( os.getuid() )
    os.environ["X509_USER_PROXY"] = userProxy

    # instatiating status object
    status = Status()

    # Loading dictionary with available parameters list
    jobStatus = lbJob.JobStatus (status)

    states = jobStatus.states_names
    attrNumber = jobStatus.ATTR_MAX
    if attrNumber == 0 :
        print "Problem loading attrNumber"
        return

    for jobid in jobs:
        try:
            jobid = str( jobid ).strip()
            if len(jobid) == 0 :
                continue
            status.getStatus(jobid, 0)
            err, apiMsg = status.get_error()
            if err:
                print jobid, apiMsg
                continue
            jobidInfo = status.loadStatus(st)
            intervals = int ( len(jobidInfo) / attrNumber )
            for off in range ( 1, intervals ):
                offset = off*attrNumber
                jobid, values = getJobInfo(
                    jobidInfo[ offset : offset + attrNumber ], states
                    )
                ret_map[ jobid ] = values
            st = st + 1
        except StandardError :
            print jobid, sys.exc_info()[1].__str__() 

    del status
    del jobStatus
            
    return ret_map



def groupByWMS( job_list, userProxy='', id_type='node', status_list=None, allow=False ):
    """
    groups a list of jobid for submission WMS
    
    return: a map where for each wms there ia a list of the
    corresponding gridid
    """
    
    endpoints = {}
    states = {}

    if status_list == None :
        status_list = []

    if id_type == 'parent':
        states = checkJobsBulk( job_list, userProxy )
    else:
        states = checkJobs( job_list, userProxy )

    for jobid, attr in states.iteritems() :
        status = attr[ 'STATUS' ]
        if status in status_list and not allow:
            print "skipping ", jobid, " in status ", status
            continue
        elif status not in status_list and allow:
            print "skipping ", jobid, " in status ", status
            continue
        url = attr[ 'WMS' ]
        if not endpoints.has_key( url ) and len(url) != 0 :
            endpoints[ url ] = []
        endpoints[ url ].append( str( jobid ) )
    return endpoints




