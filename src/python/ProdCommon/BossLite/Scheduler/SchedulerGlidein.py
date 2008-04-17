#!/usr/bin/env python
"""
_SchedulerGlidein_
"""

__revision__ = "$Id: SchedulerGlidein.py,v 1.1 2008/04/15 22:00:15 ewv Exp $"
__version__ = "$Revision: 1.1 $"

from ProdCommon.BossLite.Scheduler.SchedulerCondorCommon import SchedulerCondorCommon

class SchedulerGlidein(SchedulerCondorCommon) :
  """
  basic class to handle glite jobs through wmproxy API
  """
  def __init__( self, user_proxy = '' ):

    # call super class init method
    super(SchedulerGlidein, self).__init__(user_proxy)

  def singleApiJdl( self, job, requirements='' ):
      """
      build a job jdl easy to be handled by the wmproxy API interface
      and gives back the list of input files for a better handling
      """

      jdl = 'Universe   = vanilla\n'
      superJdl, filelist = super(SchedulerCondorG, self).singleApiJdl(job, requirements)
      jdl += superJdl

      glidein = 'UCSDT2'

      #jdl += '+DESIRED_Sites = "'+glidein+'\n'
      #jdl += '+DESIRED_Archs = "INTEL,X86_64"\n'
      #jdl += 'Requirements = stringListMember(GLIDEIN_Site,DESIRED_Sites) && stringListMember(Arch, DESIRED_Archs) \n'
      #jdl += '+JOB_Site = \"\$\$(GLIDEIN_Site:Unknown)\" \n");
      #jdl += '+JOB_VM = \"\$\$(Name:Unknown)\" \n");
      #jdl += '+JOB_Machine_KFlops = \"\$\$(KFlops:Unknown)\" \n");
      #jdl += '+JOB_Machine_Mips = \"\$\$(Mips:Unknown)\" \n");
      #jdl += '+JOB_GLIDEIN_Schedd = \"\$\$(GLIDEIN_Schedd:Unknown)\" \n");
      #jdl += '+JOB_GLIDEIN_ClusterId = \"\$\$(GLIDEIN_ClusterId:Unknown)\" \n");
      #jdl += '+JOB_GLIDEIN_ProcId = \"\$\$(GLIDEIN_ProcId:Unknown)\" \n");
      #jdl += '+JOB_GLIDEIN_Factory = \"\$\$(GLIDEIN_Factory:Unknown)\" \n");
      #jdl += '+JOB_GLIDEIN_Name = \"\$\$(GLIDEIN_Name:Unknown)\" \n");
      #jdl += '+JOB_GLIDEIN_Frontend = \"\$\$(GLIDEIN_Client:Unknown)\" \n");

      return jdl, filelist

