#!/usr/bin/env python
"""
_BossLiteAPI_

"""


__version__ = "$Id: BossLiteAPISched.py,v 1.26 2008/07/22 13:55:01 gcodispo Exp $"
__revision__ = "$Revision: 1.26 $"
__author__ = "Giuseppe.Codispoti@bo.infn.it"


# db interaction
from ProdCommon.BossLite.API.BossLiteAPI import BossLiteAPI

# Scheduler interaction
from ProdCommon.BossLite.Scheduler import Scheduler
from ProdCommon.BossLite.Common.BossLiteLogger import BossLiteLogger
from ProdCommon.BossLite.Common.Exceptions import BossLiteError


##########################################################################

class BossLiteAPISched(object):
    """
    High level API class for DBObjects and Scheduler interaction.
    It allows load/operate/update jobs and taks using just id ranges

    """


    def __init__(self, bossLiteSession, schedulerConfig, task=None):
        """
        initialize the scheduler API instance

        - bossLiteSession is a BossLiteAPI instance

        - schedulerConfig is a dictionary with the format
           {'name' : 'SchedulerGLiteAPI',
            'user_proxy' : '/proxy/path',
            'service' : 'https://wms104.cern.ch:7443/glite_wms_wmproxy_server',
            'config' : '/etc/glite_wms.conf' }

        """

        # set BossLiteLogger
        self.bossLiteLogger = None

        # use bossLiteSession for DB interactions
        if type( bossLiteSession ) is not BossLiteAPI:
            raise TypeError( "first argument must be a BossLiteAPI object")

        self.bossLiteSession = bossLiteSession
        #global GlobalLogger

        # update scheduler config
        self.schedConfig = {'user_proxy' : '', 'service' : '', 'config' : '' }
        self.schedConfig.update( schedulerConfig )

        # something to be retrieved from task object?
        if task is not None :

            # retrieve scheduler
            # FIXME : to be replaced with a task specific field
            for job in task.jobs :
                if job.runningJob['scheduler'] is not None:
                    scheduler = job.runningJob['scheduler']
                    break
            if scheduler is not None :
                self.schedConfig['name'] = scheduler

            # retrieve proxy
            if task['user_proxy'] is not None:
                self.schedConfig['name'] = scheduler

        # scheduler
        self.scheduler = Scheduler.Scheduler(
            self.schedConfig['name'], self.schedConfig
            )


    ##########################################################################
    def getLogger(self):
        """
        returns the BossLiteLogger object
        """

        return self.bossLiteLogger


    ##########################################################################

    def submit( self, taskId, jobRange='all', requirements='', schedulerAttributes=None ):
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

        task = None

        try:
            # load task
            task = self.bossLiteSession.load( taskId, jobRange )[0]

            # create or load running instances
            for job in task.jobs:
                if job.runningJob is not None :
                    job.runningJob['schedulerAttributes'] = schedulerAttributes
                
            # scheduler submit
            self.scheduler.submit( task, requirements )

            # update & set logger
            self.bossLiteSession.updateDB( task )
            self.bossLiteLogger = BossLiteLogger( task )

        except BossLiteError, e:

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task, \
                                                         notSkipClosed=False )
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise e

        # return updated task
        return task

    ##########################################################################

    def resubmit( self, taskId, jobRange='all', requirements='', schedulerAttributes=None ):
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

        task = None

        try:

            # load task
            task = self.bossLiteSession.load( taskId, jobRange )[0]

            # get new running instance
            for job in task.jobs:
                self.bossLiteSession.getNewRunningInstance(
                    job, { 'schedulerAttributes' : schedulerAttributes }
                    )

            # scheduler submit
            self.scheduler.submit( task, requirements )

            # update & set logger
            self.bossLiteSession.updateDB( task )
            self.bossLiteLogger = BossLiteLogger( task )

        except BossLiteError, e:

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task, \
                                                         notSkipClosed=False )
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise e

        # return updated task
        return task

    ##########################################################################

    def query( self, taskId, jobRange='all', queryType='node', runningAttrs=None, strict=True ):
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

        task = None

        try:

            # load task
            task = self.bossLiteSession.load( taskId, jobRange, \
                                              runningAttrs=runningAttrs, \
                                              strict=strict )[0]

            # scheduler query
            self.scheduler.query( task, queryType )

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task, \
                                                         notSkipClosed=False )
            self.bossLiteLogger = BossLiteLogger( task )

        except BossLiteError, e:

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task, \
                                                         notSkipClosed=False )
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise e

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

        task = None

        try:

            # load task
            task = self.bossLiteSession.load( taskId, jobRange )[0]

            # scheduler query
            self.scheduler.getOutput( task, outdir )

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task, \
                                                         notSkipClosed=False )
            self.bossLiteLogger = BossLiteLogger( task )

        except BossLiteError, e:

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task, \
                                                         notSkipClosed=False )
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise e

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

        task = None

        try:

            # load task
            task = self.bossLiteSession.load( taskId, jobRange )[0]

            # scheduler query
            self.scheduler.kill( task )

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task, \
                                                         notSkipClosed=False )
            self.bossLiteLogger = BossLiteLogger( task )

        except BossLiteError, e:

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task, \
                                                         notSkipClosed=False )
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise e

        # return task updated
        return task

    ##########################################################################

    def matchResources( self, taskId, jobRange='all', requirements='', schedulerAttributes=None ) :
        """
        perform a resources discovery

        - taskId can be both a Task object or the task id
        - jobRange can be of the form:
             'a,b:c,d,e'
             ['a',b','c']
             'all'
        """

        task = None

        try:

            # load task
            task = self.bossLiteSession.load( taskId, jobRange )[0]

            # retrieve running instances
            for job in task.jobs:
                if job.runningJob is not None :
                    job.runningJob['schedulerAttributes'] = schedulerAttributes

            # scheduler matchResources
            resources = self.scheduler.matchResources( task, requirements )

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task, \
                                                         notSkipClosed=False )
            self.bossLiteLogger = BossLiteLogger( task )

        except BossLiteError, e:

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task, \
                                                         notSkipClosed=False )
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise e

        # return list of sites
        return resources


    ##########################################################################

    def lcgInfo(self, tags, seList=None, blacklist=None, whitelist=None, full=False):
        """
        execute a resources discovery through bdii
        returns a list of resulting sites
        """

        task = None

        try:

            # scheduler matchResources
            resources = self.scheduler.lcgInfo( tags, seList, \
                                                blacklist, whitelist, full )

        except BossLiteError, e:

            # set logger
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise e

        # return list of sites
        return resources

    ##########################################################################

    def jobDescription ( self, taskId, jobRange='all', requirements='', schedulerAttributes=None ):
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


        task = None

        try:
            # load task
            task = self.bossLiteSession.load( taskId, jobRange )[0]

            # retrieve running instances
            for job in task.jobs:
                if job.runningJob is not None :
                    job.runningJob['schedulerAttributes'] = schedulerAttributes

            jdString = self.scheduler.jobDescription ( task, requirements )

        except BossLiteError, e:

            # set logger
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise e

        # return updated task
        return jdString

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


        task = None

        try:
            # load task
            task = self.bossLiteSession.load( taskId, jobRange )[0]

            # purge task
            self.scheduler.purgeService( task )

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task, \
                                                         notSkipClosed=False )
            self.bossLiteLogger = BossLiteLogger( task )

        except BossLiteError, e:

            # update & set logger
            self.bossLiteSession.updateRunningInstances( task, \
                                                         notSkipClosed=False )
            self.bossLiteLogger = BossLiteLogger( task, e )

            # re-throw exception
            raise e

        # return updated task
        return task

    ##########################################################################

    def postMortem ( self, taskId, jobRange='all', outfile='loggingInfo.log') :
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
        task = self.bossLiteSession.load( taskId, jobRange )[0]

        # scheduler query
        self.scheduler.postMortem( task, outfile )

    ##########################################################################
