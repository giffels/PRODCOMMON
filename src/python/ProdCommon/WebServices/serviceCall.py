import base64
import cPickle
import logging

from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Database.Connect import connect
from ProdCommon.Core.Initialize import db_config

#NOTE: we want to implement this different
#NOTE: as the connection is not the same as
#NOTE: the connection used to execute the call.


def log(client_id,service_call,service_parameters,service_result):
    global db_config

    logging.debug('logging service call')
    try:
       conn=connect(**db_config)
       dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")
       sqlStr="""INSERT INTO ws_last_call_server(client_id,service_call,service_parameters,service_result) 
           VALUES("%s","%s","%s","%s") ON DUPLICATE KEY UPDATE 
           service_parameters="%s", service_result="%s";
           """ %(str(client_id),str(service_call),base64.encodestring(cPickle.dumps(service_parameters)),base64.encodestring(cPickle.dumps(service_result)),base64.encodestring(cPickle.dumps(service_parameters)),base64.encodestring(cPickle.dumps(service_result)))
       dbCur.execute(sqlStr)
       dbCur.execute("COMMIT")
       dbCur.close()
       conn.close()
    except Exception,ex:
       dbCur.execute("ROLLBACK")
       dbCur.close()
       conn.close()
       raise ProdException(str(ex))



def retrieve(client_id,service_call=None):
    global db_config

    logging.debug('retrieving logged service call')
    try:
       conn=connect(**db_config)
       dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")
       if service_call==None:
           sqlStr=""" SELECT service_result, service_call,service_parameters  
               FROM ws_last_call_server 
               WHERE log_time in (
               SELECT max(log_time) 
               FROM ws_last_call_server 
               WHERE client_id="%s" GROUP BY client_id);
               """ %(client_id)
           dbCur.execute(sqlStr)
           rows=dbCur.fetchall()
           service_results=cPickle.loads(base64.decodestring(rows[0][0]))
           service_parameters=cPickle.loads(base64.decodestring(rows[0][2]))
           dbCur.execute("COMMIT")
           dbCur.close()
           conn.close()
           return [str(rows[0][1]),service_parameters,service_results]

       sqlStr="""SELECT service_result,service_parameters FROM ws_last_call_server WHERE client_id="%s"
           AND service_call="%s";""" %(client_id,service_call)
       dbCur.execute(sqlStr)
       rows=dbCur.fetchall()
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
       raise ProdException(str(ex))

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
       raise ProdException(str(ex))

def client_component_id(args_length,args):
   if len(args)<args_length:
       component="defaultComponent"
   else:
       component=str(args[args_length-1])
   return component

