#!/usr/bin/env python
"""
_SchedulerGlidein_
"""

__revision__ = "$Id: SchedulerGlidein.py,v 1.9 2008/06/11 20:36:05 ewv Exp $"
__version__ = "$Revision: 1.9 $"

from ProdCommon.BossLite.Scheduler.SchedulerCondorCommon import SchedulerCondorCommon
import os

class SchedulerGlidein(SchedulerCondorCommon) :
  """
  basic class to handle glite jobs through wmproxy API
  """
  def __init__( self, **args ):

      # call super class init method
      super(SchedulerGlidein, self).__init__(**args)

  def singleApiJdl( self, job, requirements='' ):
      """
      build a job jdl easy to be handled by the wmproxy API interface
      and gives back the list of input files for a better handling
      """

      jdl = 'Universe = vanilla\n'
      superJdl, filelist, ce \
          = super(SchedulerGlidein, self).singleApiJdl(job, requirements)
      jdl += superJdl

      x509 = None
      x509tmp = '/tmp/x509up_u'+str(os.getuid())
      if 'X509_USER_PROXY' in os.environ:
          x509 = os.environ['X509_USER_PROXY']
      elif os.path.isfile(x509tmp):
          x509 = x509tmp

      if x509:
          jdl += 'x509userproxy = %s\n' % x509

      # Optional parameters that may be put in after the conversion from Perl to Python and escapes are fixed
      jdl += '+JOB_Site = "$$(GLIDEIN_Site:Unknown)" \n'
      jdl += '+JOB_VM = "$$(Name:Unknown)" \n'
      #jdl += '+JOB_Machine_KFlops = \"\$\$(KFlops:Unknown)\" \n");
      #jdl += '+JOB_Machine_Mips = \"\$\$(Mips:Unknown)\" \n");
      jdl += '+JOB_GLIDEIN_Schedd = "$$(GLIDEIN_Schedd:Unknown)" \n'
      jdl += '+JOB_GLIDEIN_ClusterId = "$$(GLIDEIN_ClusterId:Unknown)" \n'
      jdl += '+JOB_GLIDEIN_ProcId = "$$(GLIDEIN_ProcId:Unknown)" \n'
      jdl += '+JOB_GLIDEIN_Factory = "$$(GLIDEIN_Factory:Unknown)" \n'
      jdl += '+JOB_GLIDEIN_Name = "$$(GLIDEIN_Name:Unknown)" \n'
      jdl += '+JOB_GLIDEIN_Frontend = "$$(GLIDEIN_Client:Unknown)" \n'
      jdl += '+JOB_Gatekeeper = "$$(GLIDEIN_Gatekeeper:Unknown)" \n'
      jdl += '+JOB_GridType = "$$(GLIDEIN_GridType:Unknown)" \n'
      jdl += '+JOB_GlobusRSL = "$$(GLIDEIN_GlobusRSL:Unknown)" \n'
      jdl += 'Periodic_Remove = (((JobStatus == 2) && ((CurrentTime - JobCurrentStartDate) > (MaxWallTimeMins*60))) =?= True) \n'


      return jdl, filelist, 'Unknown'
