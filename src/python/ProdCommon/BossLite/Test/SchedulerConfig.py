#!/usr/bin/env python
"""
__SchedulerConfig__

a set of possible choices for the TaskAPITest.py usage

"""

__version__ = "$Id: SchedulerConfig.py,v 1.0 2008/07/01 16:53:55 gcodispo Exp $"
__revision__ = "$Revision: 1.0 $"
__author__ = "Giuseppe.Codispoti@bo.infn.it"


def simplestScheduler() :
    return { 'name' : 'SchedulerGLiteAPI'}


def schedulerWtimeout() :
    return { 'name' : 'SchedulerGLiteAPI',
             'timeout' : 30  }


def oneWmsScheduler() :
    return {'name' : 'SchedulerGLiteAPI',
            'service' : 'https://wms107.cern.ch:7443/glite_wms_wmproxy_server'}

def complexScheduler() :
    return {'name' : 'SchedulerGLiteAPI',
            "config" : "/bohome/grandi/UI/3.1.13-0/glite/etc/cms/glite_wms.conf",
            'timeout' : 30,
            'service' : 'https://my-wms.cern.ch:7443/glite_wms_wmproxy_server',
            'user_proxy' : '/tmp/x509up_u' + str( os.getuid() )  }


schedulerConfig = simplestScheduler()

