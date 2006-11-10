import base64
import cPickle

from ProdCommon.Core.GlobalRegistry import registerHandler
from ProdCommon.Database.Dialect.Query import Interface

class Query1(Interface):

   def build(self,args={}):
      sqlStr="""INSERT INTO 
      ws_last_call_server(client_id,service_call,service_parameters,service_result,client_tag)
      VALUES("%s","%s","%s","%s","%s") ON DUPLICATE KEY UPDATE
      service_parameters="%s", service_result="%s",client_tag="%s";
      """ %(str(args['client_id']),str(args['service_call']),\
      base64.encodestring(cPickle.dumps(args['service_parameters'])),\
      base64.encodestring(cPickle.dumps(args['service_result'])),\
      str(args['client_tag']),\
      base64.encodestring(cPickle.dumps(args['service_parameters'])),\
      base64.encodestring(cPickle.dumps(args['service_result'])),\
      str(args['client_tag']))
      return sqlStr 

registerHandler(Query1(),"ProdCom.Query1","MySQL")
