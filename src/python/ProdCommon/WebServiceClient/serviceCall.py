import base64
import cPickle
import logging

from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Database import Session
from ProdCommon.Core.Initialize import db_config


def log(serverUrl,method_name,args,componentID="defaultComponent"):
   try:
       sqlStr="""INSERT INTO ws_last_call(server_url,component_id,service_call,service_parameters,call_state)
           VALUES("%s","%s","%s","%s","%s") ON DUPLICATE KEY UPDATE
           service_parameters="%s", call_state="%s";
           """ %(serverUrl,componentID,method_name,base64.encodestring(cPickle.dumps(args)),"call_placed",base64.encodestring(cPickle.dumps(args)),"call_placed")
       Session.execute(sqlStr)
       lastCall=(serverUrl,method_name,componentID)
   except Exception,ex:
       raise ProdAgentException("Service logging Error: "+str(ex))

def commit(serverUrl=None,method_name=None,componentID=None):
   global lastCall

   try:
       if (serverUrl==None) or (method_name==None) or (componentID==None):
           serverUrl,method_name,componentID=lastCall
       sqlStr="""UPDATE ws_last_call SET call_state="result_retrieved" WHERE
           server_url="%s" AND component_id="%s" AND service_call="%s";
           """ %(serverUrl,componentID,method_name)
       Session.execute(sqlStr)
   except Exception,ex:
       raise ProdAgentException("Service commit Error: "+str(ex))

def retrieve(serverURL=None,method_name=None,componentID=None):

   try:
       if serverURL==None and method_name==None and componentID==None:
           sqlStr="""SELECT server_url,service_call,component_id, max(log_time) FROM ws_last_call
               WHERE call_state="call_placed" GROUP BY server_url;
               """ 
       elif serverURL==None and method_name==None and componentID!=None:
           sqlStr="""SELECT server_url,service_call,component_id, max(log_time) FROM ws_last_call
               WHERE component_id="%s" AND call_state="call_placed" GROUP BY server_url;
               """ %(componentID)
       elif serverURL==None and method_name!=None and componentID!=None:
           sqlStr="""SELECT server_url,service_call,component_id, max(log_time) FROM ws_last_call
               WHERE component_id="%s" AND service_call="%s" AND call_state="call_placed" GROUP BY server_url;
               """ %(componentID,method_name)
       elif serverURL!=None and method_name==None and componentID!=None:
           sqlStr="""SELECT server_url,service_call,component_id, max(log_time) FROM ws_last_call
               WHERE component_id="%s" AND server_url="%s" AND call_state="call_placed" GROUP BY server_url;
               """ %(componentID,serverURL)
       Session.execute(sqlStr)
       rows=Session.fetchall()
       if len(rows)==0:
           raise ProdException("No result in local last service call table with componentID :"+\
               str(componentID),1000)
       server_url=rows[0][0]
       service_call=rows[0][1]
       component_id=rows[0][2]
       return [server_url,service_call,component_id]
   except Exception,ex:
       raise ProdAgentException("Service commit Error: "+str(ex))
