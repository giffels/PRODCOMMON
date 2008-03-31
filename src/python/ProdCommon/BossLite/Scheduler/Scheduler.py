#!/usr/bin/env python
"""
_Scheduler_

"""
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob
from ProdCommon.BossLite.Common.Exceptions import SchedulerError

__version__ = "$Id: Scheduler.py,v 1.14 2008/03/31 07:36:31 gcodispo Exp $"
__revision__ = "$Revision: 1.14 $"

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
            run = obj.runningJob
            run['schedulerId'] = jobAttributes[ obj['name'] ]
            run['schedulerParentId'] = run['schedulerId']
            run['scheduler'] = self.scheduler
            run['service'] = service
            
        # update multiple jobs of a task
        elif type( obj ) == Task :
            for job in obj.jobs :
                run = job.runningJob
                if job.runningJob is None:
                    continue
                run['schedulerId'] = jobAttributes[ job['name'] ]
                run['schedulerParentId'] = bulkId
                run['scheduler'] = self.scheduler
                run['service'] = service

        # unknown object type
        else:
            raise SchedulerError('wrong argument type')

        # returns an updated object
        return obj
    
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
            schedId = obj['schedulerId']
            if obj['schedulerId'] is None:
                return
            jobAttributes = self.schedObj.query( schedId, \
                                           self.parameters['service'], 'node' )
            for key, value in jobAttributes[schedId].iteritems() :
                obj[key] = value

        # the object passed is a Job: query and update
        elif type(obj) == Job :
            if obj.runningJob is None \
                   or obj.runningJob['schedulerId'] is None:
                return
            schedId = obj.runningJob['schedulerId']
            jobAttributes = self.schedObj.query( schedId, \
                                           self.parameters['service'], 'node' )
            for key, value in jobAttributes[schedId].iteritems() :
                obj.runningJob[key] = value

        # the object passed is a Task:
        #   check whether parent id are provided, make a list of ids
        #     and check the status
        elif type(obj) == Task :
            schedIds = []

            # query performed through single job ids
            if objType == 'node' :
                for job in obj.jobs :
                    if job.runningJob is None\
                           or job.runningJob['schedulerId'] is None:
                        job.runningJob = None
                        continue
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

            # update
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

        # returns an updated object
        return obj

    ##########################################################################
    
    def getOutput( self, obj, outdir ):
        """
        retrieve output or just put it in the destination directory
        """

        # the object passed is a runningJob
        if type(obj) == RunningJob :
            self.schedObj.getOutput(
                obj['schedulerId'], outdir, obj['service']
                )
            obj['status'] = 'E'
            obj['closed'] = 'Y'
            obj['statusScheduler'] = "Retrieved"

        # the object passed is a job
        elif type(obj) == Job :
            self.schedObj.getOutput( obj.runningJob['schedulerId'], \
                                     outdir, obj.runningJob['service']  )
            obj.runningJob['status'] = 'E'
            obj.runningJob['closed'] = 'Y'
            obj.runningJob['statusScheduler'] = "Retrieved"

        # the object passed is a Task
        elif type(obj) == Task :

            if outdir == '' :
                outdir = obj['outputDirectory']

            # retrieve scheduler id list
            schedIdList = {}
            for job in obj.jobs:
                if job.runningJob is not None \
                       and job.runningJob['schedulerId'] is not  None:
                    if not schedIdList.has_key( job.runningJob['service'] ) :
                        schedIdList[job.runningJob['service']] = []
                    schedIdList[job.runningJob['service']].append( job.runningJob['schedulerId'] )

            # perform actual getoutput
            for service, idList in schedIdList.iteritems() :
                self.schedObj.getOutput( idList, outdir, service )
 
            #update objects
            for job in obj.jobs:
                job.runningJob['status'] = 'E'
                job.runningJob['closed'] = 'Y'
                job.runningJob['statusScheduler'] = "Retrieved"

        # unknown object type
        else:
            raise SchedulerError( 'getOutput', 'wrong argument type' )


    ##########################################################################

    def kill( self, obj ):
        """
        kill the job instance
        """

        # the object passed is a runningJob
        if type(obj) == RunningJob :
            self.schedObj.kill( obj['schedulerId'], obj['service'] )
            obj['status'] = 'K'
            obj['statusScheduler'] = "Killed"

        # the object passed is a job
        elif type(obj) == Job :
            self.schedObj.kill( obj.runningJob['schedulerId'], \
                                obj.runningJob['service'] )
            obj.runningJob['status'] = 'K'
            obj.runningJob['statusScheduler'] = "Killed"

        # the object passed is a Task
        elif type(obj) == Task :

            # retrieve scheduler id list
            schedIdList = []
            for job in obj.jobs:
                if job.runningJob is not None \
                       and job.runningJob['schedulerId'] is not  None:
                    if not schedIdList.has_key( job.runningJob['service'] ) :
                        schedIdList[job.runningJob['service']] = []
                    schedIdList[job.runningJob['service']].append( job.runningJob['schedulerId'] )
                        
            # perform actual kill
            for service, idList in schedIdList.iteritems() :
                self.schedObj.kill( schedIdList, service )

            #update objects
            for job in obj.jobs:
                job.runningJob['status'] = 'K'
                job.runningJob['statusScheduler'] = "Killed"

        # unknown object type
        else:
            raise SchedulerError( 'kill', 'wrong argument type')

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
            out = ''
            for job in obj.jobs:
                if job.runningJob is None:
                    continue
                self.schedObj.postMortem( job .runningJob['schedulerId'], \
                                          out, self.parameters['service'] )
                outfile += out

        # unknown object type
        else:
            raise SchedulerError('wrong argument type')

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
            self.schedObj.purgeService( obj['schedulerId'] )
            obj['statusScheduler'] = "Purged"

        # the object passed is a job
        elif type(obj) == Job :
            self.schedObj.purgeService( obj.runningJob['schedulerId'] )
            obj.runningJob['statusScheduler'] = "Purged"

        # the object passed is a Task
        elif type(obj) == Task :

            # retrieve scheduler id list
            schedIdList = []
            for job in obj.jobs:
                if job.runningJob is not None \
                       and job.runningJob['schedulerId'] is not  None:
                    schedIdList.append( job.runningJob['schedulerId'] )

            # perform actual kill
            self.schedObj.purgeService( schedIdList )

            #update objects
            for job in obj.jobs:
                job.runningJob['statusScheduler'] = "Purged"

        # unknown object type
        else:
            raise SchedulerError('wrong argument type')



