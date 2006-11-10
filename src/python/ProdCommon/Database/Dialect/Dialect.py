
from ProdCommon.Core.GlobalRegistry import retrieveHandler
from ProdCommon.Core.Initialize import configuration
from ProdCommon.Database.Dialect.MySQL import *


def buildQuery(queryType,parameters):
   if configuration.get('DB')['dbType']=='mysql':
       query=retrieveHandler(queryType,"MySQL")
   if configuration.get('DB')['dbType']=='oracle':
       query=retrieveHandler(queryType,"Oracle")
   return query.build(parameters)  
