#!/usr/bin/env python
"""
_BossLiteAPI_

"""

__version__ = "$Id"
__revision__ = "$Revision"
__author__ = "Giuseppe.Codispoti@bo.infn.it"


# db interaction
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI

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


    def submit( self, taskId, jobRange='all', requirements='', jobAttributes=None ):
        """
        eventually creates running instances and submit to the scheduler
        
        - taskId can be both a Task object or the task id
        - jobRange can be of the form:
             'a,b:c,d,e'
             ['a',b','c']
             'all'
        - requirements are scheduler attributes affecting all jobs
        - jobAttributes is a map of running attributes
                        to be applyed at the job level
        """

        # load task
        task = self.bossLiteSession.load( taskId, jobRange )[0]

        # create or load running instances
        for job in task.jobs:
            self.bossLiteSession.getRunningInstance(
                job, { 'schedulerAttributes' : jobAttributes }
                )

        # scheduler submit
        self.scheduler.submit( task, requirements )

        # update
        self.bossLiteSession.updateDB(task)

        # return updated task
        return task

    ##########################################################################


    def resubmit( self, taskId, jobRange='all', requirements='', jobAttributes=None ):
        """
        archive existing submission an create a new entry for the next
        submission (i.e. duplicate the entry with an incremented
        submission number) instances and submit to the scheduler
        
        - taskId can be both a Task object or the task id
        - jobRange can be of the form:
             'a,b:c,d,e'
             ['a',b','c']
             'all'
        - requirements are scheduler attributes affecting all jobs
        - jobAttributes is a map of running attributes
        """

        # load task
        task = self.bossLiteSession.load( taskId, jobRange )[0]

        for job in task.jobs:
            job.closeRunningInstance( self.bossLiteSession.db )

        # update changes
        self.bossLiteSession.updateDB(task)

        # get new running instance
        for job in task.jobs:
            self.bossLiteSession.getRunningInstance(
                job, { 'schedulerAttributes' : jobAttributes }
                )

        # scheduler submit
        self.scheduler.submit( task, requirements )

        # update
        self.bossLiteSession.updateDB(task)


        # return task updated
        return task

    ##########################################################################

    def query( self, taskId, jobRange='all', queryType='node', runningAttrs=None ):
        """
        query status and eventually other scheduler related information
        
        - taskId can be both a Task object or the task id
        - jobRange can be of the form:
             'a,b:c,d,e'
             ['a',b','c']
             'all'
        - query type can be 'parent' if the status check is meant
                                  to be performed via bulk id
        - all: if False, only jobs with non closed running instances are loaded
        """

        # load task
        task = self.bossLiteSession.load( taskId, jobRange, runningAttrs)[0]

        # scheduler query
        self.scheduler.query( task, queryType )

        # update
        self.bossLiteSession.updateDB(task)

        # return task updated
        return task

    ##########################################################################


    def getOutput( self, taskId, jobRange='all', outdir='' ):
        """
        retrieve output or just put it in the destination directory
        
        - taskId can be both a Task object or the task id
        - jobRange can be of the form:
             'a,b:c,d,e'
             ['a',b','c']
             'all'
        - outdir is the output directory for files retrieved
        """

        # load task
        task = self.bossLiteSession.load( taskId, jobRange )[0]

        # scheduler query
        self.scheduler.getOutput( task, outdir )

        # update
        self.bossLiteSession.updateDB(task)

        # return task updated
        return task

    ##########################################################################
    
    def kill( self, taskId, jobRange='all' ):
        """
        kill the job instance
        
        - taskId can be both a Task object or the task id
        - jobRange can be of the form:
             'a,b:c,d,e'
             ['a',b','c']
             'all'
        """

        # load task
        task = self.bossLiteSession.load( taskId, jobRange )[0]

        # scheduler query
        self.scheduler.kill( task )

        # update
        self.bossLiteSession.updateDB(task)
        
        # return task updated
        return task

    ##########################################################################


    def matchResources( self, taskId, jobRange='all', requirements='', jobAttributes=None ) :
        """
        perform a resources discovery
        
        - taskId can be both a Task object or the task id
        - jobRange can be of the form:
             'a,b:c,d,e'
             ['a',b','c']
             'all'
        """

        # load task
        task = self.bossLiteSession.load( taskId, jobRange )[0]

        # retrieve running instances
        for job in task.jobs:
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
        
        - taskId can be both a Task object or the task id
        - jobRange can be of the form:
             'a,b:c,d,e'
             ['a',b','c']
             'all'
        - requirements are scheduler attributes affecting all jobs
        - jobAttributes is a map of running attributes
                        to be applyed at the job level
        """

        # load task
        task = self.bossLiteSession.load( taskId, jobRange )[0]

        # create or load running instances
        for job in task.jobs:
            self.bossLiteSession.getRunningInstance(
                job, { 'schedulerAttributes' : jobAttributes }
                )

        return self.scheduler.jobDescription ( task, requirements )

    ##########################################################################

    def purgeService( self, taskId, jobRange='all') :
        """
        purge the service used by the scheduler from job files
        not available for every scheduler
        
        - taskId can be both a Task object or the task id
        - jobRange can be of the form:
             'a,b:c,d,e'
             ['a',b','c']
             'all'
        """
        
        # load task
        task = self.bossLiteSession.load( taskId, jobRange )[0]

        # purge task
        self.bossLiteSession.purgeService( task )

        # update
        self.bossLiteSession.updateDB(task)

    ##########################################################################

    def postMortem ( self, taskId, jobId, outfile ) :
        """
        execute any post mortem command such as logging-info
        
        - taskId can be both a Task object or the task id
        - jobRange can be of the form:
             'a,b:c,d,e'
             ['a',b','c']
             'all'
        - outfile is the physical file where to log post mortem informations
        """

        # load task
        task = self.bossLiteSession.loadTask( taskId, {'id' : jobId} )

        # retrieve running instances
        for job in task.jobs:
            self.bossLiteSession.getRunningInstance( job )

        # scheduler query
        self.scheduler.postMortem( task, outfile )

    ##########################################################################
