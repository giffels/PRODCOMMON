from ProdCommon.Core.GlobalRegistry import registerHandler
from ProdCommon.Database.Dialect.Query import Interface

class Query3(Interface):

   def build(self,args={}):

      sqlStr="""SELECT service_result,service_parameters 
      FROM ws_last_call_server WHERE client_id="%s" AND client_tag="%s"
      AND service_call="%s";
      """ %(args['client_id'],str(args['client_tag']),args['service_call'])
      return sqlStr 

registerHandler(Query3(),"ProdCom.Query3","MySQL")
