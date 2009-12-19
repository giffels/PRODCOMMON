#!/usr/bin/env python
"""
BossLite PBS/torque interface

dave.newbold@cern.ch, June 09

You will need libtorque.so in your LD_LIBRARY_PATH

Uses a wrapper script which assumes an env var PBS_JOBCOOKIE points to the local execution area

"""

__revision__ = "$Id: SchedulerPbs.py,v 1.1 2009/10/08 15:09:20 mcinquil Exp $"
__version__ = "$Revision: 1.1 $"

import re, os, time
import tempfile
import pbs

from ProdCommon.BossLite.Scheduler.SchedulerInterface import SchedulerInterface
from ProdCommon.BossLite.Common.Exceptions import SchedulerError
from ProdCommon.BossLite.DbObjects.Job import Job
from ProdCommon.BossLite.DbObjects.Task import Task
from ProdCommon.BossLite.DbObjects.RunningJob import RunningJob

class SchedulerPbs (SchedulerInterface) :
    """
    basic class to handle lsf jobs
    """
    def __init__( self, **args):
        super(SchedulerPbs, self).__init__(**args)
        self.jobScriptDir=args['jobScriptDir']
        self.jobResDir=args['jobResDir']
        self.queue=args['queue']

        self.res_dict={}
        for a in args['resources'].split(','):
            if len(a) > 0:
                if a.find("=") != -1:
                    res,val=a.split('=')
                    self.res_dict.update({res:val})
                else:
                    raise SchedulerError("PBS error", +\
                                         "Unkown resource format: " + a)
                
        env=[]
        for v in ('HOME', 'LANG', 'LOGNAME', 'MAIL', 'PATH', 'SHELL'):
            env.append('PBS_O_'+v+'='+os.environ[v])

        env.append('PBS_O_WORKDIR='+os.getcwd())
        env.append('PBS_O_HOST='+pbs.pbs_default())
        #if 'use_proxy' in args:
        #    if args['use_proxy'] == 1:
        #        proxy_location = ''
        #        try:
        #            proxy_location = os.environ['X509_USER_PROXY']
        #        except:
        #            proxy_location = '/tmp/x509up_u'+ repr(os.getuid())

        #        msg, ret = self.ExecuteCommand('cp ' + proxy_location + " " + self.cert)
        ##        proxy_path = self.getUserProxy()
        #        env.append('X509_USER_PROXY=' + self.cert)
        #        env.append('X509_USER_CERT=' + self.cert)
        #        env.append('X509_USER_KEY=' + self.cert)
        #    else:
        #        raise SchedulerError(str(args), self.cert)
        
        self.pbs_env=','.join(env)

        self.status_map={'E':'R',
                         'H':'SS',
                         'Q':'SS',
                         'R':'R',
                         'S':'R',
                         'T':'R',
                         'W':'SS',
                         'Done':'SD'}

    def jobDescription ( self, obj, requirements='', config='', service = '' ):
        """
        retrieve scheduler specific job description
        return it as a string
        """
        raise NotImplementedError

    def submit ( self, obj, requirements='', config='', service = '' ) :
        """
        set up submission parameters and submit

        return jobAttributes, bulkId, service

        - jobAttributs is a map of the format
              jobAttributes[ 'name' : 'schedulerId' ]
        - bulkId is an eventual bulk submission identifier
        - service is a endpoit to connect withs (such as the WMS)
        """
        
        conn=self.pbs_conn()
            
        if type(obj) == RunningJob or type(obj) == Job:
            map, taskId, queue = self.submitJob(conn, obj, requirements)
        elif type(obj) == Task :
            map, taskId, queue = self.submitTask (conn, obj, requirements ) 

        self.pbs_disconn(conn)

        return map, taskId, queue

    def submitTask ( self, conn, task, requirements=''):

        ret_map={}
        for job in task.getJobs() :
            map, taskId, queue = self.submitJob(conn, job, task, requirements)
            ret_map.update(map)

        return ret_map, taskId, queue

    def submitJob ( self, conn, job, task=None, requirements=''):
        """ Need to copy the inputsandbox to WN before submitting a job"""

        # Write a temporary submit script
        # NB: we assume an env var PBS_JOBCOOKIE points to the exec dir on the batch host

        ifiles=task['globalSandbox'].split(',')

        f=tempfile.NamedTemporaryFile()
        s=[]
        s.append('#!/bin/sh');
        s.append('if [ ! -d $PBS_JOBCOOKIE ] ; then mkdir -p $PBS_JOBCOOKIE ; fi')
        s.append('cd $PBS_JOBCOOKIE')
        for ifile in task['globalSandbox'].split(','):
            s.append('cp '+ifile+' .')
        s.append(self.jobScriptDir + job['executable']+' '+ job['arguments'] +\
                 ' >' + job['standardOutput'] + ' 2>' + job['standardError'])
        s.append('cd $PBS_O_WORKDIR')
        s.append('rm -fr $PBS_JOBCOOKIE')
        f.write('\n'.join(s))
        f.flush()

        attr_dict={'Job_Name':'CRAB_PBS',
                   'Variable_List':self.pbs_env,
                   'Output_Path':self.jobResDir+'wrapper_'+str(job['standardOutput']),
                   'Error_Path':self.jobResDir+'wrapper_'+str(job['standardError'])
                   }

        attropl=pbs.new_attropl(len(attr_dict)+len(self.res_dict))
        i_attr=0
        for k in attr_dict.keys():
            attropl[i_attr].name=k
            attropl[i_attr].value=attr_dict[k]
            i_attr+=1
        for k in self.res_dict.keys():
            attropl[i_attr].name='Resource_List'
            attropl[i_attr].resource=k
            attropl[i_attr].value=self.res_dict[k]
            i_attr+=1

        jobid = pbs.pbs_submit(conn, attropl, f.name, self.queue, 'NULL')
        #raise Exception (str(s))
        f.close()

        if not jobid:
            err, err_text=pbs.error()
            self.logging.error('Error in job submission')
            self.logging.error('PBS error code '+str(err)+': '+err_text)
            self.pbs_disconn(conn)
            raise SchedulerError('PBS error', str(err)+': '+err_text)
        
        return {job['name']:jobid}, None, None 

    def query(self, obj, service='', objType='node') :
        """
        query status and eventually other scheduler related information
        It may use single 'node' scheduler id or bulk id for association
        """
        if type(obj) != Task :
            raise SchedulerError('wrong argument type', str( type(obj) ))

        jobids=[]

        conn=self.pbs_conn()
        attrl=pbs.new_attrl(2)
        attrl[0].name='job_state'
        attrl[1].name='exec_host'

        for job in obj.jobs :
            if not self.valid( job.runningJob ): continue
            id=str(job.runningJob['schedulerId']).strip()
            jobstat=pbs.pbs_statjob(conn, id, attrl, 'Null')

            if not jobstat:
                err, err_text=pbs.error()
                if err!=15001: # unknown job (probably finished)
                    self.logging.error('Error in job query for '+id)
                    self.logging.error('PBS error code '+str(err)+': '+err_text)
                    self.pbs_disconn(conn)
                    raise SchedulerError('PBS error', str(err)+': '+err_text)
        
            host=''
            if len(jobstat)==0:
                pbs_stat='Done'
            else:
                pbs_stat=jobstat[0].attribs[0].value
                if len(jobstat[0].attribs)>1: host=jobstat[0].attribs[1].value
            job.runningJob['statusScheduler']=pbs_stat
            job.runningJob['status'] = self.status_map[pbs_stat]
            job.runningJob['destination']=host
            
        self.pbs_disconn(conn)

    def kill(self, obj):

        conn=self.pbs_conn()

        for job in obj.jobs :
            if not self.valid( job.runningJob ): continue
            id=str(job.runningJob['schedulerId']).strip()
            res=pbs.pbs_deljob(conn, id, '')

            if res!=0:
                err, err_text=pbs.error()
                self.logging.error('Error in job kill for '+id)
                self.logging.error('PBS error code '+str(err)+': '+err_text)
                self.pbs_disconn(conn)
                raise SchedulerError('PBS error', str(err)+': '+err_text)
                    
        self.pbs_disconn(conn)
        
    def getOutput( self, obj, outdir='' ):
        """
        retrieve output or just put it in the destination directory

        does not return
        """

    def pbs_conn(self):
        conn=pbs.pbs_connect(pbs.pbs_default())
        if(conn<0):
            err, err_text = pbs.error()
            self.logging.error('Error in PBS server conncet')
            self.logging.error('PBS error code '+str(err)+': '+err_text)
            raise SchedulerError('PBS error', str(err)+': '+err_text)
        return conn

    def pbs_disconn(self, conn):
        pbs.pbs_disconnect(conn)
