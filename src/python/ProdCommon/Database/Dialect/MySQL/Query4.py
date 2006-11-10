from ProdCommon.Core.GlobalRegistry import registerHandler
from ProdCommon.Database.Dialect.Query import Interface

class Query4(Interface):

   def build(self,args={}):
      sqlStr=""" DELETE FROM ws_last_call_server WHERE client_id="%s"
      AND service_call="%s"; 
      """ %(args['client_id'],args['service_call'])
      return sqlStr 

registerHandler(Query4(),"ProdCom.Query4","MySQL")
