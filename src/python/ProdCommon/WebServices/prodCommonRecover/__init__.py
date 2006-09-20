from ProdCommon.Core.ProdException import ProdException
from ProdCommon.WebServices import serviceCall

def lastCall(req,method_name,args):
  try:
     component,client_tag=serviceCall.client_component_id(2,args)
     result=serviceCall.retrieve(str(req.clarens_dn)+"-"+component,client_tag,None)
     return result
  except ProdException, ex:
        raise RuntimeError(ex['ErrorNr'],str(ex))
  except Exception,ex:
        raise RuntimeError(0,str(ex))


def lastServiceCall(req,method_name,args):
  try:
     component,client_tag=serviceCall.client_component_id(3,args)
     result=serviceCall.retrieve(str(req.clarens_dn)+"-"+component,client_tag,args[0])
     return result
  except ProdException, ex:
        raise RuntimeError(ex['ErrorNr'],str(ex))
  except Exception,ex:
        raise RuntimeError(0,str(ex))


# ---------------------------------------------------------------------------------
# Method description data
import ProdCommon.Core.Initialize

methods_list={'lastCall'         :lastCall,
              'lastServiceCall'  :lastServiceCall }

major_version=1
minor_version=0

init_priority=0
