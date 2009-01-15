#!/usr/bin/env python
"""
_Monitoring_

"""

__version__ = "$Id: Monitoring.py,v 1.0 2008/10/02 14:30:01 gcodispo Exp $"
__revision__ = "$Revision: 1.0 $"
__author__ = "Giuseppe.Codispoti@bo.infn.it"


class Monitoring:
    """
    _Monitoring_
    """

    ##########################################################################

    def __init__(self, session):
        """
        __init__
        """

        self.bossSession = session


    ##########################################################################

    def getTimeLimits( self, span, time, stop=None ) :
        """
        build up time limit for SQL query
        """

        if span == 'hours' :
            interval = time * 3600
        elif span == 'days' :
            interval = time * 24 * 3600

        if stop is None :
            ret = \
                " and submission_time > DATE_SUB(Now(),INTERVAL %d SECOND)" \
                % interval

        else :
            starttime = stop - interval
            ret = " and submission_time > FROM_UNIXTIME(" \
                  + str(starttime) + \
                  ") and submission_time < FROM_UNIXTIME(" \
                  + str(stop) + ")"

        return ret


    ##########################################################################

    def destination(self, span, time, stop=None):
        """
        jobs per site and status distribution
        """

        query = """
        select destination,status_scheduler,count(*) from bl_runningjob  group
        by status_scheduler,destination
        """

        query += self.getTimeLimits( span, time, stop )
        rows = self.bossSession.select(query)

        return rows


    ##########################################################################

    def exitCodes(self, stop=None):
        """
        jobs per site and exit codes distribution
        """

        query = """
        select pr.site_name,pi.exit_code,count(*) from prodmon_Job_instance pi
        join prodmon_Resource pr on pi.resource_id=pr.resource_id group by
        pr.site_name,pi.exit_code
        """

        rows = self.bossSession.select(query)

        return rows


    ##########################################################################
    def enqueuingDelay(self, span, time, stop=None):
        """
        delay between job submission and scheduling at site
        """

        query = """
        select (scheduled_at_site-submission_time)/60 from bl_runningjob
        where scheduled_at_site!=0 and submission_time!=0
        """

        query += self.getTimeLimits( span, time, stop )
        rows = self.bossSession.select(query)

        return rows


    ##########################################################################
    def queueDelay(self, span, time, stop=None):
        """
        job latency in site queue
        """

        query = """
        select (start_time-scheduled_at_site)/60 from bl_runningjob
        where scheduled_at_site!=0 and start_time!=0
        """

        query += self.getTimeLimits( span, time, stop )
        rows = self.bossSession.select(query)

        return rows


    ##########################################################################
    def startDelay(self, span, time, stop=None):
        """
        delay between job submission and actual job execution
        """


        query = """
        select (start_time-submission_time)/60 from bl_runningjob
        where start_time!=0 and submission_time!=0
        """

        query += self.getTimeLimits( span, time, stop )
        rows = self.bossSession.select(query)

        return rows


    ##########################################################################
    def executionTime(self, span, time, stop=None):
        """
        job execution time, calculed as stop_time-start_time as reported by LB
        """

        query = """
        select (stop_time-start_time)/60 from bl_runningjob
        where start_time!=0 and stop_time!=0
        """

        query += self.getTimeLimits( span, time, stop )
        rows = self.bossSession.select(query)

        return rows


    ##########################################################################
    def getOutputDelay(self, span, time, stop=None):
        """
        delay between job stop and output retrieval
        """

        query = """
        select (getoutput_time-stop_time)/60 from bl_runningjob where
        getoutput_time!=0 and stop_time!=0
        """

        query += self.getTimeLimits( span, time, stop )
        rows = self.bossSession.select(query)

        return rows


    ##########################################################################
    def status(self, span, time, stop=None):
        """
        cumulative job status
        """

        query = """
        select status_scheduler,count(status_scheduler)
        from bl_runningjob group by status_scheduler
        """

        query += self.getTimeLimits( span, time, stop )
        rows = self.bossSession.select(query)

        return rows


    ##########################################################################
    def goStatus(self, span, time, stop=None):
        """
        diagnostic for JT/GO: watching at the processStatus it's possible
        to debug latency on the varios tracking/retrieval/reorganization
        of the job outputs
        """

        query = """
        select process_status,status_scheduler,count(status_scheduler)
        from bl_runningjob group by process_status,status_scheduler
        """

        query += self.getTimeLimits( span, time, stop )
        rows = self.bossSession.select(query)

        return rows


    ##########################################################################
    def activeStatus(self, span, time, stop=None):
        """
        diagnostic for closed running instances
        """
        
        query = """
        select status_scheduler,closed,count(status_scheduler)
        from bl_runningjob group by status,process_status,status_scheduler
        """

        query += self.getTimeLimits( span, time, stop )
        rows = self.bossSession.select(query)

        return rows


