import base64
import cPickle

from ProdCommon.Core.GlobalRegistry import registerHandler
from ProdCommon.Database.Dialect.Query import Interface

class Query2(Interface):

   def build(self,args={}):
       sqlStr=""" SELECT service_result, service_call,service_parameters
       FROM ws_last_call_server
       WHERE id in (
       SELECT max(id)
       FROM ws_last_call_server
       WHERE client_id="%s" AND client_tag="%s" AND log_time in (
       SELECT max(log_time)
       FROM ws_last_call_server
       WHERE client_id="%s" AND client_tag="%s" GROUP BY client_id) 
       GROUP BY client_id);
       """ %(str(args['client_id']),str(args['client_tag']),\
       str(args['client_id']),str(args['client_tag']))
       return sqlStr 

registerHandler(Query2(),"ProdCom.Query2","MySQL")
