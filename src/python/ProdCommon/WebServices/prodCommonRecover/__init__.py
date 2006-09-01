from ProdCommon.WebServices import serviceCall

def lastCall(req,method_name,args):
  component=serviceCall.client_component_id(1,args)
  result=serviceCall.retrieve(str(req.clarens_dn)+"-"+component,None)
  return result

def lastServiceCall(req,method_name,args):
  component=serviceCall.client_component_id(2,args)
  result=serviceCall.retrieve(str(req.clarens_dn)+"-"+component,args[0])
  return result


# ---------------------------------------------------------------------------------
# Method description data
import ProdCommon.Core.Initialize

methods_list={'lastCall'         :lastCall,
              'lastServiceCall'  :lastServiceCall }

major_version=1
minor_version=0

init_priority=0
