#!/usr/bin/env python
"""
_Scheduler_

"""
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob
from ProdCommon.BossLite.Common.Exceptions import SchedulerError

__version__ = "$Id: Scheduler.py,v 1.18 2008/04/02 12:23:28 gcodispo Exp $"
__revision__ = "$Revision: 1.18 $"


##########################################################################
class Scheduler(object):
    """
    Upper layer for scheduler interaction
    
    """

    def __init__(self, scheduler, parameters = None):
        """
        initialization
        """

        # define scheduler parameters
        self.scheduler = scheduler
        defaults = {'user_proxy' : '', 'service' : '', 'config' : '' }
        if parameters is not None :
            defaults.update( parameters )
        self.parameters = defaults

        # load scheduler plugin
        try:
            module =  __import__(
                'ProdCommon.BossLite.Scheduler.' + self.scheduler, globals(), locals(),
                [self.scheduler]
                )
            schedClass = vars(module)[self.scheduler]
            self.schedObj = schedClass( self.parameters['user_proxy'])
        except KeyError:
            msg = 'Scheduler interface' + self.scheduler + 'not found'
            raise SchedulerError('missing', msg)
        except ImportError, e:
            msg = 'Cannot create scheduler ' + self.scheduler + ' '
            raise SchedulerError(msg, str(e))

    ##########################################################################

    def submit ( self, obj, requirements='' ) :
        """
        set up submission parameters and submit
        """

        # delegate submission to scheduler plugin
        jobAttributes, bulkId, service = self.schedObj.submit(\
            obj, requirements, \
            self.parameters['config'], self.parameters['service']\
            )

        # update single job
        if type( obj ) == Job :
            obj.runningJob['schedulerId'] = jobAttributes[ obj['name'] ]
            obj.runningJob['status'] = 'S'
            obj.runningJob['schedulerParentId'] = obj.runningJob['schedulerId']
            obj.runningJob['scheduler'] = self.scheduler
            obj.runningJob['service'] = service
            
        # update multiple jobs of a task
        elif type( obj ) == Task :
            for job in obj.jobs :
                if job.runningJob is None or job.runningJob['closed'] == "Y" :
                    continue
                job.runningJob['schedulerId'] = jobAttributes[ job['name'] ]
                job.runningJob['status'] = 'S'
                job.runningJob['schedulerParentId'] = bulkId
                job.runningJob['scheduler'] = self.scheduler
                job.runningJob['service'] = service

        # unknown object type
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))
    
    ##########################################################################

    def jobDescription ( self, obj, requirements='' ) :
        """
        retrieve scheduler specific job description
        """

        return self.schedObj.jobDescription( obj, self.parameters['config'], \
                                             self.parameters['service'], \
                                             requirements )

    ##########################################################################
    
    def query(self, obj, objType='node') :
        """
        query status and eventually other scheduler related information
        """

        # the object passed is a runningJob: query and update
        if type(obj) == RunningJob :

            # check for the RunningJob integrity
            if not self.schedObj.valid( obj ):
                raise SchedulerError('invalid object', str( obj ))

            # query!
            jobAttributes = self.schedObj.query(
                obj['schedulerId'], self.parameters['service'], 'node'
                )

            # status association
            for key, value in jobAttributes[obj['schedulerId']].iteritems() :
                obj[key] = value

        # the object passed is a Job: query and update
        elif type(obj) == Job :

            # check for the RunningJob integrity
            if not self.schedObj.valid( obj.runningJob ):
                raise SchedulerError('invalid object', str( obj.runningJob ))

            # query!
            jobAttributes = self.schedObj.query (
                obj.runningJob['schedulerId'], self.parameters['service'], \
                'node' )

            # status association
            for key, value in jobAttributes[obj['schedulerId']].iteritems() :
                obj.runningJob[key] = value

        # the object passed is a Task:
        #   check whether parent id are provided, make a list of ids
        #     and check the status
        elif type(obj) == Task :
            schedIds = []

            # query performed through single job ids
            if objType == 'node' :
                for job in obj.jobs :
                    if self.schedObj.valid( job.runningJob ):
                        schedIds.append( job.runningJob['schedulerId'] )
                jobAttributes = self.schedObj.query( schedIds, \
                                               self.parameters['service'], \
                                               'node' )

            # query performed through a bulk id
            elif objType == 'parent' :
                for job in obj.jobs :
                    if job.runningJob['schedulerParentId'] not in schedIds :
                        schedIds.append( job.runningJob['schedulerParentId'] )
                jobAttributes = self.schedObj.query( schedIds, \
                                               self.parameters['service'],
                                               'parent' )

            # status association
            for job in obj.jobs :
                try:
                    valuesMap = jobAttributes[ job.runningJob['schedulerId'] ]
                except:
                    continue
                for key, value in valuesMap.iteritems() :
                    job.runningJob[key] = value

        # unknown object type
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))


    ##########################################################################
    
    def getOutput( self, obj, outdir ):
        """
        retrieve output or just put it in the destination directory
        """

        # the object passed is a runningJob
        self.schedObj.getOutput( obj, outdir, self.parameters['service']  )

        if type(obj) == RunningJob :

            # update object
            obj['status'] = 'E'
            obj['closed'] = 'Y'
            obj['statusScheduler'] = "Retrieved"

        # the object passed is a job
        elif type(obj) == Job :

            # update object
            obj.runningJob['status'] = 'E'
            obj.runningJob['closed'] = 'Y'
            obj.runningJob['statusScheduler'] = "Retrieved"

        # the object passed is a Task
        elif type(obj) == Task :
 
            # update objects: still missing handling for partial success
            for job in obj.jobs:
                job.runningJob['status'] = 'E'
                job.runningJob['closed'] = 'Y'
                job.runningJob['statusScheduler'] = "Retrieved"

        # unknown object type
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))


    ##########################################################################

    def kill( self, obj ):
        """
        kill the job instance
        """

        # the object passed is a runningJob
        if type(obj) == RunningJob :

            # check for the RunningJob integrity
            if not self.schedObj.valid( obj ):
                raise SchedulerError('invalid object', str( obj ))

            # kill job
            self.schedObj.kill( obj['schedulerId'], obj['service'] )

            # update object
            obj['status'] = 'K'
            obj['statusScheduler'] = "Killed"

        # the object passed is a job
        elif type(obj) == Job :

            # check for the RunningJob integrity
            if not self.schedObj.valid( obj.runningJob ):
                raise SchedulerError('invalid object', str( obj.runningJob ))

            # kill job
            self.schedObj.kill( obj.runningJob['schedulerId'], \
                                obj.runningJob['service'] )

            # update object
            obj.runningJob['status'] = 'K'
            obj.runningJob['statusScheduler'] = "Killed"

        # the object passed is a Task
        elif type(obj) == Task :

            # retrieve scheduler id list
            schedIdList = {}
            for job in obj.jobs:
                if self.schedObj.valid( job.runningJob ):
                    if not schedIdList.has_key( job.runningJob['service'] ) :
                        schedIdList[job.runningJob['service']] = []
                    schedIdList[job.runningJob['service']].append( job.runningJob['schedulerId'] )
                        
            # perform actual kill
            for service, idList in schedIdList.iteritems() :
                self.schedObj.kill( idList, service )

            # update objects: still missing handling for partial success
            for job in obj.jobs:
                job.runningJob['status'] = 'K'
                job.runningJob['statusScheduler'] = "Killed"

        # unknown object type
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))

    ##########################################################################

    def postMortem ( self, obj, outfile ) :
        """
        execute any post mortem command such as logging-info
        """

        if type(obj) == RunningJob :
            self.schedObj.postMortem(
                obj['schedulerId'], outfile, self.parameters['service']
                )

        # the object passed is a job
        elif type(obj) == Job :
            self.schedObj.postMortem( obj.runningJob['schedulerId'], \
                                      outfile, self.parameters['service']
                )

        # the object passed is a Task
        elif type(obj) == Task :
 #           out = ''
            for job in obj.jobs:
                if job.runningJob is None:
                    continue
                self.schedObj.postMortem( job.runningJob['schedulerId'], \
                                          outfile, self.parameters['service'] )
 #               outfile += out

        # unknown object type
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))

    ##########################################################################

    def matchResources ( self, obj, requirements='' ) :
        """
        perform a resources discovery
        """

        return self.schedObj.matchResources( obj, requirements, \
                                             self.parameters['config'], \
                                             self.parameters['service'] )

    ##########################################################################

    def lcgInfo(self, tags, seList=None, blacklist=None, whitelist=None, vo='cms'):
        """
        execute a resources discovery through bdii
        returns a list of resulting sites
        """

        return self.schedObj.lcgInfo( tags, seList, blacklist, whitelist, vo )


    ##########################################################################

    def purgeService( self, obj ) :
        """
        purge the service used by the scheduler from job files
        not available for every scheduler
        """

        # the object passed is a runningJob
        if type(obj) == RunningJob :

            # purge object if valid
            if self.schedObj.valid( obj ):
                self.schedObj.purgeService( obj['schedulerId'] )
            else :
                raise SchedulerError('invalid object', str( obj ))

            # update object
            obj['statusScheduler'] = "Purged"

        # the object passed is a job
        elif type(obj) == Job :

            # purge object if valid
            if not self.schedObj.valid( obj.runningJob ):
                self.schedObj.purgeService( obj.runningJob['schedulerId'] )
            else :
                raise SchedulerError('invalid object', str( obj.runningJob ))

            # update object
            obj.runningJob['statusScheduler'] = "Purged"

        # the object passed is a Task
        elif type(obj) == Task :

            # retrieve scheduler id list
            schedIdList = []
            for job in obj.jobs:
                if self.schedObj.valid( job.runningJob ):
                    schedIdList.append( job.runningJob['schedulerId'] )

            # perform actual kill
            self.schedObj.purgeService( schedIdList )

            # update objects
            for job in obj.jobs:
                job.runningJob['statusScheduler'] = "Purged"

        # unknown object type
        else:
            raise SchedulerError('wrong argument type', str( type(obj) ))



