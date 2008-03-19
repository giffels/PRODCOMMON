#!/usr/bin/env python
"""
_BossLiteAPI_

"""

__version__ = "$Id"
__revision__ = "$Revision"
__author__ = "Giuseppe.Codispoti@bo.infn.it"

import logging

# db interaction
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI
from ProdCommon.BossLite.API.BossLiteAPI import parseRange

# Scheduler interaction
from ProdCommon.BossLite.Scheduler import Scheduler

##########################################################################

class BossLiteAPISched(object):
    """
    High level API class for DBObjcets and Scheduler interaction.
    It allows load/operate/update jobs and taks using just id ranges
    
    """

    def __init__(self, bossLiteSession, schedulerConfig):
        """
        initialize the scheduler API instance

        - bossLiteSession is a BossLiteAPI instance
 
        - schedulerConfig is a dictionary with the format
           {'name' : 'SchedulerGLiteAPI',
            'user_proxy' : '/proxy/path',
            'service' : 'https://wms104.cern.ch:7443/glite_wms_wmproxy_server',
            'config' : '/etc/glite_wms.conf' }
            
        """

        # use bossLiteSession for DB interactions
        if type( bossLiteSession ) is not BossLiteAPI:
            raise TypeError( "first argument must be a BossLiteAPI object")
        
        self.bossLiteSession = bossLiteSession

        # update scheduler config
        self.schedConfig = {'user_proxy' : '', 'service' : '', 'config' : '' }
        self.schedConfig.update( schedulerConfig )

        # scheduler
        self.scheduler = Scheduler.Scheduler(
            schedulerConfig['name'], self.schedConfig
            )

    ##########################################################################


    def submit( self, task, jobRange='all', requirements='', jobAttributes=None ):
        """
        works for Task objects
        
        just create running instances and submit to the scheduler
        in case of first submission
        
        archive existing submission an create a new entry for the next
        submission (i.e. duplicate the entry with an incremented
        submission number)
        """

        # db connect
        if self.bossLiteSession.db is None :
            self.bossLiteSession.connect()

        taskId = task['id']
        task = self.bossLiteSession.load( taskId, jobRange )[0]

        # create or recreate running instances
        for job in task.jobs:
            self.bossLiteSession.getRunningInstance(
                job, { 'schedulerAttributes' : jobAttributes }
                )

        # scheduler submit
        self.scheduler.submit( task, requirements )

        # update
        task.update(self.bossLiteSession.db)
        self.bossLiteSession.session.commit()


    ##########################################################################


    def resubmit( self, taskId, jobRange='all', requirements='', jobAttributes=None ):
        """
        unlike previous method, works using taskId
        and load itself the Task object
        
        archive existing submission an create a new entry for the next
        submission (i.e. duplicate the entry with an incremented
        submission number)
        """

        # db connect
        if self.bossLiteSession.db is None :
            self.bossLiteSession.connect()

        # load and close running instances
        task = self.bossLiteSession.load( taskId, jobRange )[0]

        for job in task.jobs:
            job.closeRunningInstance( self.bossLiteSession.db )

        # update changes
        task.update(self.bossLiteSession.db)
        self.bossLiteSession.session.commit()

        # get new running instance
        for job in task.jobs:
            if jobRange != 'all' and job['id'] in jobRange:
                self.bossLiteSession.getRunningInstance(
                    job, { 'schedulerAttributes' : jobAttributes }
                    )

        # scheduler submit
        self.scheduler.submit( task, requirements )

        # update
        task.update(self.bossLiteSession.db)
        self.bossLiteSession.session.commit()

    ##########################################################################

    def query( self, taskId, jobRange='all', queryType='node' ):
        """
        query status and eventually other scheduler related information
        """

        # db connect
        if self.bossLiteSession.db is None :
            self.bossLiteSession.connect()

        task = self.bossLiteSession.load( taskId, jobRange )[0]

        # retrieve running instances
        for job in task.jobs:
            self.bossLiteSession.getRunningInstance( job )

        # scheduler query
        self.scheduler.query( task, queryType )

        # update
        task.update(self.bossLiteSession.db)
        self.bossLiteSession.session.commit()

    ##########################################################################


    def getOutput( self, taskId, jobRange='all', outdir='' ):
        """
        retrieve output or just put it in the destination directory
        """

        # db connect
        if self.bossLiteSession.db is None :
            self.bossLiteSession.connect()

        task = self.bossLiteSession.load( taskId, jobRange )[0]

        # retrieve running instances
        for job in task.jobs:
            if jobRange != 'all' and job['id'] in jobRange:
                self.bossLiteSession.getRunningInstance( job )

        # scheduler query
        self.scheduler.query( task, outdir )

        # update
        task.update(self.bossLiteSession.db)
        self.bossLiteSession.session.commit()

    ##########################################################################
    
    def kill( self, taskId, jobRange='all' ):
        """
        kill the job instance
        """

        task = self.bossLiteSession.load( taskId, jobRange )[0]

        # retrieve running instances
        for job in task.jobs:
            if jobRange != 'all' and job['id'] in jobRange:
                self.bossLiteSession.getRunningInstance( job )

        # scheduler query
        self.scheduler.kill( task )

        # update
        task.update(self.bossLiteSession.db)
        self.bossLiteSession.session.commit()

    ##########################################################################


    def matchResources( self, taskId, jobRange='all', requirements='', jobAttributes=None ) :
        """
        perform a resources discovery
        """

        # db connect
        if self.bossLiteSession.db is None :
            self.bossLiteSession.connect()

        # create a session and db access
        task = self.bossLiteSession.load( taskId, jobRange )[0]

        # retrieve running instances
        for job in task.jobs:
            if jobRange != 'all' and job['id'] in jobRange:
                self.bossLiteSession.getRunningInstance(
                    job, { 'schedulerAttributes' : jobAttributes }
                    )

        # scheduler query
        return self.scheduler.matchResources( task, requirements )

    ##########################################################################

    def lcgInfo(self, tags, seList=None, blacklist=None, whitelist=None, vo='cms'):
        """
        execute a resources discovery through bdii
        returns a list of resulting sites
        """

        return self.scheduler.lcgInfo( tags, seList, blacklist, whitelist, vo )


    ##########################################################################
        
    def jobDescription ( self, taskId, jobRange='all', requirements='', jobAttributes=None ):
        """
        query status and eventually other scheduler related information
        """

        # db connect
        if self.bossLiteSession.db is None :
            self.bossLiteSession.connect()

        task = self.bossLiteSession.load( taskId, jobRange )[0]

        for job in task.jobs:
            self.bossLiteSession.getRunningInstance(
                job, { 'schedulerAttributes' : jobAttributes }
                )

        return self.bossLiteSession.jobDescription ( task, requirements )

    ##########################################################################

    def purgeService( self, taskId, jobRange='all') :
        """
        purge the service used by the scheduler from job files
        not available for every scheduler
        """
        
        # db connect
        if self.bossLiteSession.db is None :
            self.bossLiteSession.connect()

        # retrieve and purge task
        task = self.bossLiteSession.load( taskId, jobRange )[0]
        self.bossLiteSession.purgeService( task )

        # update
        task.update(self.bossLiteSession.db)
        self.bossLiteSession.session.commit()

    ##########################################################################

    def postMortem ( self, taskId, jobId, outfile ) :
        """
        execute any post mortem command such as logging-info
        """

        # db connect
        if self.bossLiteSession.db is None :
            self.bossLiteSession.connect()

        # create a session and db access
        task = self.bossLiteSession.loadTask( taskId, {'id' : jobId} )

        # retrieve running instances
        for job in task.jobs:
            self.bossLiteSession.getRunningInstance( job )

        # scheduler query
        self.bossLiteSession.postMortem( task, outfile )

    ##########################################################################
