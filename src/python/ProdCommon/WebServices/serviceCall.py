import base64
import cPickle
import logging
import time

from ProdCommon.Core.Codes import exceptions
from ProdCommon.Core.Initialize import db_config
from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Database import Session
from ProdCommon.Database.Dialect import Dialect

#NOTE: we want to implement this different
#NOTE: as the connection is not the same as
#NOTE: the connection used to execute the call.


def log(client_id,service_call,service_parameters,service_result,client_tag='0'):
    global db_config

    logging.debug('logging service call')
    try:
       sqlStr=Dialect.buildQuery("ProdCom.Query1",{'client_id':client_id,\
           'service_call':service_call,'service_parameters':service_parameters,\
           'service_result':service_result,'client_tag':client_tag,})
       # NOTE: this has to be done different, we do this to keep the log time unique
       Session.execute(sqlStr)
       logging.debug('service call logged')
    except Exception,ex:
       logging.debug('ERROR in logging call: '+str(ex))
       raise ProdException(str(ex),1001)


def retrieve(client_id,client_tag='0',service_call=None):
    global db_config

    logging.debug('retrieving logged service call '+str(client_id)+','+str(client_tag)+','+str(service_call))
    try:
       if service_call==None:
           sqlStr=Dialect.buildQuery("ProdCom.Query2",{'client_id':client_id,\
               'client_tag':client_tag})
           Session.execute(sqlStr)
           rows=Session.fetchall()
           if len(rows)!=1:
               raise ProdException("No entries found for client ID: "+str(client_id)+" and tag "+str(client_tag),1002)
           service_results=cPickle.loads(base64.decodestring(rows[0][0]))
           service_parameters=cPickle.loads(base64.decodestring(rows[0][2]))
           return [str(rows[0][1]),service_parameters,service_results]

       sqlStr=Dialect.buildQuery("ProdCom.Query3",{'client_id':client_id,\
           'client_tag':client_tag,'service_call':service_call})
       Session.execute(sqlStr)
       rows=Session.fetchall()
       if len(rows)!=1:
           raise ProdException("No entries found for client ID: "+str(client_id)+" and tag "+str(client_tag),1002)
       service_results=cPickle.loads(base64.decodestring(rows[0][0]))
       service_parameters=cPickle.loads(base64.decodestring(rows[0][1]))
       return [service_results,service_parameters]
    except Exception,ex:
       raise ProdException(exceptions[4001]+str(ex),4001)

def remove(client_id,service_call):
    global db_config

    logging.debug('removing logged service call')
    try:
       sqlStr=Dialect.buildQuery("ProdCom.Query4",{'client_id':client_id,\
           'service_call':service_call})
       Session.execute(sqlStr)
    except Exception,ex:
       raise ProdException(str(ex),1001)

def client_component_id(args_length,args):
   if len(args)<args_length:
       component="defaultComponent"
       client_tag="0"
   else:
       component=str(args[args_length-2])
       client_tag=str(args[args_length-1])
   return (component,client_tag)

