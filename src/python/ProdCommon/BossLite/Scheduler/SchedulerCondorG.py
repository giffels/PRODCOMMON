#!/usr/bin/env python
"""
_SchedulerCondorG_
"""

__revision__ = "$Id: SchedulerCondorGAPI.py,v 1.22 2008/04/15 17:01:25 ewv Exp $"
__version__ = "$Revision: 1.22 $"

from ProdCommon.BossLite.Scheduler.SchedulerCondorCommon import SchedulerCondorCommon

class SchedulerCondorG(SchedulerCondorCommon) :
  """
  basic class to handle glite jobs through wmproxy API
  """
  def __init__( self, user_proxy = '' ):

    # call super class init method
    super(SchedulerCondorG, self).__init__(user_proxy)
  

  def singleApiJdl( self, job, requirements='' ):
      """
      build a job jdl easy to be handled by the wmproxy API interface
      and gives back the list of input files for a better handling
      """

      jdl = 'Universe   = grid\n'
      superJdl, filelist = super(SchedulerCondorG, self).singleApiJdl(job, requirements)
      jdl += superJdl
      return jdl, filelist

