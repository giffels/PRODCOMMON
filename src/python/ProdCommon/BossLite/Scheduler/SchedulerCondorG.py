#!/usr/bin/env python
"""
_SchedulerCondorG_
"""

__revision__ = "$Id: SchedulerCondorG.py,v 1.4 2008/09/23 13:07:37 ewv Exp $"
__version__  = "$Revision: 1.4 $"

import os
from ProdCommon.BossLite.Scheduler.SchedulerCondorCommon import SchedulerCondorCommon

class SchedulerCondorG(SchedulerCondorCommon) :
    """
    basic class to handle glite jobs through wmproxy API
    """

    def __init__( self, **args ):
        # call super class init method
        super(SchedulerCondorG, self).__init__(**args)


    def specificBulkJdl(self, job, requirements=''):
        # FIXME: This is very similar to SchedulerCondorCommon's version,
        # should be consolidated.
        """
        build a job jdl
        """
        rootName = os.path.splitext(job['standardError'])[0]

        jdl  = ''
#         jobId = int(job['jobId'])
#         # Massage arguments into space delimited form w/o backslashes
#         jobArgs = job['arguments']
#         jobArgs = jobArgs.replace(',',' ')
#         jobArgs = jobArgs.replace('\\ ',',')
#         jobArgs = jobArgs.replace('\\','')
#         jobArgs = jobArgs.replace('"','')
        jdl += 'Universe   = grid\n'
#         jdl += 'Universe  = vanilla\n'
#         jdl += 'environment = CONDOR_ID=$(Cluster).$(Process)\n'
#         jdl += 'Arguments  = %s\n' % jobArgs
#         if job['standardInput'] != '':
#             jdl += 'input = %s\n' % job['standardInput']
#         jdl += 'output  = %s\n' % job['standardOutput']
#         jdl += 'error   = %s\n' % job['standardError']
        jdl += 'log     = %s.log\n' % rootName # Same root as stderr
#         jdl += 'stream_output = false\n'
#         jdl += 'stream_error  = false\n'
#         jdl += 'notification  = never\n'
#         jdl += 'should_transfer_files   = YES\n'
#         jdl += 'when_to_transfer_output = ON_EXIT\n'
#         jdl += 'copy_to_spool           = false\n'
        if self.userRequirements:
            jdl += 'requirements = %s\n' % self.userRequirements

#         # Things in the requirements/jobType field
#         jdlLines = requirements.split(';')
#         for line in jdlLines:
#             [key, value] = line.split('=', 1)
#             if key.strip() == "schedulerList":
#                 CEs = value.split(',')
#                 ceSlot = (jobId-1) // self.batchSize
#                 ceNum = ceSlot % len(CEs)
#                 ce = CEs[ceNum]
#                 jdl += "globusscheduler = " + ce + '\n'
#             else:
#                 jdl += line.strip() + '\n'

#         x509 = None
#         x509tmp = '/tmp/x509up_u'+str(os.getuid())
#         if 'X509_USER_PROXY' in os.environ:
#             if os.path.isfile(os.environ['X509_USER_PROXY']):
#                 x509 = os.environ['X509_USER_PROXY']
#         elif os.path.isfile(x509tmp):
#             x509 = x509tmp
#
#         if x509:
#             jdl += 'x509userproxy = %s\n' % x509
#
#         outputFiles = job['outputFiles']
#         if outputFiles:
#             jdl += 'transfer_output_files   = ' + ','.join(outputFiles) + '\n'
#
#         filelist = ''
        return jdl
#     def singleApiJdl( self, job, requirements='' ):
#         """
#         build a job jdl easy to be handled by the wmproxy API interface
#         and gives back the list of input files for a better handling
#         """
#
#         jdl = 'Universe   = grid\n'
# #         superJdl, filelist, ce \
# #             = super(SchedulerCondorG, self).singleApiJdl(job, requirements)
#         superJdl = super(SchedulerCondorG, self).singleApiJdl(job, requirements)
#         jdl += superJdl
#         return jdl#, filelist, ce
