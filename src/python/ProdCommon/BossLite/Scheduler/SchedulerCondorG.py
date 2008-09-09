#!/usr/bin/env python
"""
_SchedulerCondorG_
"""

__revision__ = "$Id: SchedulerCondorG.py,v 1.2 2008/04/29 08:15:42 gcodispo Exp $"
__version__ = "$Revision: 1.2 $"

from ProdCommon.BossLite.Scheduler.SchedulerCondorCommon import SchedulerCondorCommon

class SchedulerCondorG(SchedulerCondorCommon) :
  """
  basic class to handle glite jobs through wmproxy API
  """
  def __init__( self, **args ):

    # call super class init method
    super(SchedulerCondorG, self).__init__(**args)


  def singleApiJdl( self, job, requirements='' ):
      """
      build a job jdl easy to be handled by the wmproxy API interface
      and gives back the list of input files for a better handling
      """

      jdl = 'Universe   = grid\n'
      superJdl, filelist, ce = super(SchedulerCondorG, self).singleApiJdl(job, requirements)
      jdl += superJdl
      return jdl, filelist, ce

