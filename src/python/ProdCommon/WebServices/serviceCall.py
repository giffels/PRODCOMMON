import base64
import cPickle
import logging
import time

from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Database.Connect import connect
from ProdCommon.Core.Initialize import db_config

#NOTE: we want to implement this different
#NOTE: as the connection is not the same as
#NOTE: the connection used to execute the call.


def log(client_id,service_call,service_parameters,service_result,client_tag='0'):
    global db_config

    logging.debug('logging service call')
    try:
       conn=connect(**db_config)
       dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")
       # NOTE: this has to be done different, we do this to keep the log time unique
       sqlStr="""INSERT INTO ws_last_call_server(client_id,service_call,service_parameters,service_result,client_tag) 
           VALUES("%s","%s","%s","%s","%s") ON DUPLICATE KEY UPDATE 
           service_parameters="%s", service_result="%s",client_tag="%s";
           """ %(str(client_id),str(service_call),base64.encodestring(cPickle.dumps(service_parameters)),base64.encodestring(cPickle.dumps(service_result)),str(client_tag),base64.encodestring(cPickle.dumps(service_parameters)),base64.encodestring(cPickle.dumps(service_result)),str(client_tag))
       dbCur.execute(sqlStr)
       dbCur.execute("COMMIT")
       dbCur.close()
       conn.close()
    except Exception,ex:
       dbCur.execute("ROLLBACK")
       dbCur.close()
       conn.close()
       raise ProdException(str(ex),1001)


def retrieve(client_id,client_tag='0',service_call=None):
    global db_config

    logging.debug('retrieving logged service call '+str(client_id)+','+str(client_tag)+','+str(service_call))
    try:
       conn=connect(**db_config)
       dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")
       if service_call==None:
           sqlStr=""" SELECT service_result, service_call,service_parameters  
               FROM ws_last_call_server 
               WHERE id in (
               SELECT max(id)
               FROM ws_last_call_server
               WHERE client_id="%s" AND client_tag="%s" AND log_time in (
               SELECT max(log_time) 
               FROM ws_last_call_server 
               WHERE client_id="%s" AND client_tag="%s" GROUP BY client_id) GROUP BY client_id);
               """ %(str(client_id),str(client_tag),str(client_id),str(client_tag))
           dbCur.execute(sqlStr)
           rows=dbCur.fetchall()
           if len(rows)!=1:
               raise ProdException("No entries found for client ID: "+str(client_id)+" and tag "+str(client_tag),1002)
           service_results=cPickle.loads(base64.decodestring(rows[0][0]))
           service_parameters=cPickle.loads(base64.decodestring(rows[0][2]))
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
           return [str(rows[0][1]),service_parameters,service_results]

       sqlStr="""SELECT service_result,service_parameters FROM ws_last_call_server WHERE client_id="%s" AND client_tag="%s"
           AND service_call="%s";""" %(client_id,str(client_tag),service_call)
       dbCur.execute(sqlStr)
       rows=dbCur.fetchall()
       if len(rows)!=1:
           raise ProdException("No entries found for client ID: "+str(client_id)+" and tag "+str(client_tag),1002)
       service_results=cPickle.loads(base64.decodestring(rows[0][0]))
       service_parameters=cPickle.loads(base64.decodestring(rows[0][1]))
       dbCur.execute("COMMIT")
       dbCur.close()
       conn.close()
       return [service_results,service_parameters]
    except Exception,ex:
       dbCur.execute("ROLLBACK")
       dbCur.close()
       conn.close()
       raise ProdException(str(ex),1001)

def remove(client_id,service_call):
    global db_config

    logging.debug('removing logged service call')
    try:
       conn=connect(**db_config)
       dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")
       sqlStr=""" DELETE FROM ws_last_call_server WHERE client_id="%s"
           AND service_call="%s"; """ %(client_id,service_call)
       dbCur.execute(sqlStr)
       dbCur.execute("COMMIT")
       dbCur.close()
       conn.close()
    except Exception,ex:
       dbCur.execute("ROLLBACK")
       dbCur.close()
       conn.close()
       raise ProdException(str(ex),1001)

def client_component_id(args_length,args):
   if len(args)<args_length:
       component="defaultComponent"
       client_tag="0"
   else:
       component=str(args[args_length-2])
       client_tag=str(args[args_length-1])
   return (component,client_tag)

