#!/usr/bin/env python


"""
 Provides a convinient wrapper around database connection
 Can be used in pool, if it'll be ever written
 
"""

from ProdCommon.Database.Connect import connect as dbConnect

dbsession_counter=0

class DbSession:
    def __init__(self,db_params):
        self.db_params=db_params
        self.db=None
        self.db=dbConnect(**self.db_params)
        global dbsession_counter
        self.sessionId="dbsession_"+str(dbsession_counter)
        dbsession_counter=dbsession_counter+1
        self.cursor=self.db.cursor()
        self.pool=None
        self.status="free"


    def begin(self):
        self.db.begin()

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()

    def execute(self,sql,positional_parameters=None,**keyword_parameters):
        if(positional_parameters):
            self.cursor.execute(sql,positional_parameters)
        else:
            self.cursor.execute(sql,**keyword_parameters)


    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()


    def wakeupFromPool(self,pool):
        self.pool=pool
        if(not self.cursor):
            self.cursor=self.db.cursor()


    def release(self):
        if(self.db):
            self.db.rollback()
            self.cursor.close()
            self.cursor=None
            self.status="free"

    def __del__(self):
        print "Deleted session ["+self.sessionId+"]"


class SesWrap:
    def __init__(self,s):
        self.ses=s

    def __getattr__(self,name):
        print name
        if(name=='ses'):
            return ses
        else:
            return getattr(self.ses,name)

    def __setattr__(self,name,val):
        if(name=='ses'):
            self.__dict__['ses']=val
        else:
            setattr(self.ses,name,val)

    def __del__(self):
        self.ses.release()

    def __str__(self):
        ret="DbSession "+self.ses.sessionId
        return ret


class Pool:
    def __init__(self):
        self.sesmap={}

    def getSession(self,db_params):
        dbtype=db_params['dbType']
        dbname=db_params['dbName']
        dbhost=db_params['host']
        dbuser=db_params['user']
        dbpass=db_params['passwd']
        dbsock=db_params['socketFileLocation']
        dbport=db_params['portNr']
        sep="]_["
        key="dbKey_"+sep.join((dbtype,dbname,dbhost,dbuser,dbsock,dbport))
        if(not self.sesmap.has_key(key)):
            self.sesmap[key]=[]
        ses_list=self.sesmap[key]
        for i in ses_list:
            if(i.status=="free"):
                i.status="busy"
                i.wakeupFromPool(self)
                print "Got session ["+i.sessionId+"] from pool"
                wrap=SesWrap(i)
                return wrap
        newses=DbSession(db_params)
        newses.status="busy"
        ses_list.append(newses)
        newses.wakeupFromPool(self)
        print "New session ["+newses.sessionId+"] has been created, total "+str(len(ses_list))+" for the db "+key
        wrap=SesWrap(newses)
        return wrap

pool=Pool()

def getSession(db_params):
    return pool.getSession(db_params)


if __name__ == '__main__':
    cfg={'dbName':'CMSCALD',
               'host':'cmscald',
               'user':'REPACK_DEV',
               'passwd':'REPACK_DEV_CMS2007',
               'socketFileLocation':'',
               'portNr':'',
               'refreshPeriod' : 4*3600 ,
               'maxConnectionAttempts' : 5,
               'dbWaitingTime' : 10,
               'dbType' : 'oracle',
              }
    ses=getSession(cfg)
    print "Session",ses
    del ses
    ses=getSession(cfg)
    print "Session",ses
    del ses
    ses=getSession(cfg)
    print "Session",ses
    ses=getSession(cfg)
    print "Session",ses
    ses.execute("SELECT 1 from DUAL")
    data=ses.fetchall()
    print data
    



