import base64
import cPickle
import logging

from ProdCommon.Core.ProdException import ProdException
from ProdCommon.Database.Connect import connect
from ProdCommon.Core.Initialize import db_config


def log(serverUrl,method_name,args,componentID="defaultComponent"):
   try:
       conn=connect(**db_config)
       dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")
       sqlStr="""INSERT INTO ws_last_call(server_url,component_id,service_call,service_parameters,call_state)
           VALUES("%s","%s","%s","%s","%s") ON DUPLICATE KEY UPDATE
           service_parameters="%s", call_state="%s";
           """ %(serverUrl,componentID,method_name,base64.encodestring(cPickle.dumps(args)),"call_placed",base64.encodestring(cPickle.dumps(args)),"call_placed")
       dbCur.execute(sqlStr)
       dbCur.execute("COMMIT")
       lastCall=(serverUrl,method_name,componentID)
       dbCur.close()
       conn.close()
   except Exception,ex:
       dbCur.execute("ROLLBACK")
       dbCur.close()
       conn.close()
       raise ProdAgentException("Service logging Error: "+str(ex))

def commit(serverUrl=None,method_name=None,componentID=None):
   global lastCall

   try:
       conn=connect(**db_config)
       dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")
       if (serverUrl==None) or (method_name==None) or (componentID==None):
           serverUrl,method_name,componentID=lastCall
       sqlStr="""UPDATE ws_last_call SET call_state="result_retrieved" WHERE
           server_url="%s" AND component_id="%s" AND service_call="%s";
           """ %(serverUrl,componentID,method_name)
       dbCur.execute(sqlStr)
       dbCur.execute("COMMIT")
       dbCur.close()
       conn.close()
   except Exception,ex:
       dbCur.execute("ROLLBACK")
       dbCur.close()
       conn.close()
       raise ProdAgentException("Service commit Error: "+str(ex))

def retrieve(serverURL=None,method_name=None,componentID=None):

   try:
       conn=connect(**db_config)
       dbCur=conn.cursor()
       dbCur.execute("START TRANSACTION")
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
       dbCur.execute(sqlStr)
       rows=dbCur.fetchall()
       if len(rows)==0:
           raise ProdException("No result in local last service call table with componentID :"+\
               str(componentID),1000)
       server_url=rows[0][0]
       service_call=rows[0][1]
       component_id=rows[0][2]
       dbCur.execute("COMMIT")
       dbCur.close()
       conn.close()
       return [server_url,service_call,component_id]
   except Exception,ex:
       dbCur.execute("ROLLBACK")
       dbCur.close()
       conn.close()
       raise ProdAgentException("Service commit Error: "+str(ex))
