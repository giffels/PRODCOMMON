from mod_python import apache
import clarens_util

from ProdCommon.Core.ProdException import ProdException
from ProdCommon.WebServices import serviceCall
from ProdCommon.Database import DB

def lastCall(req,method_name,args):
  try:
     DB.setProdCommonDB()
     component,client_tag=serviceCall.client_component_id(2,args)
     result=serviceCall.retrieve(str(req.clarens_dn)+"-"+component,client_tag,None)
     DB.finish()
     response=clarens_util.build_response(req,method_name,result)
  except ProdException, ex:
        DB.fail()
        response=clarens_util.build_fault(req,method_name,ex['ErrorNr'],str(ex))
  except Exception,ex:
        DB.fail()
        response=clarens_util.build_fault(req,method_name,0,str(ex))
  clarens_util.write_response(req,response)
  return apache.OK

def lastServiceCall(req,method_name,args):
  try:
     DB.setProdCommonDB()
     component,client_tag=serviceCall.client_component_id(3,args)
     result=serviceCall.retrieve(str(req.clarens_dn)+"-"+component,client_tag,args[0])
     DB.finish()
     response=clarens_util.build_response(req,method_name,result)
  except ProdException, ex:
        DB.fail()
        response=clarens_util.build_fault(req,method_name,ex['ErrorNr'],str(ex))
  except Exception,ex:
        DB.fail()
        response=clarens_util.build_fault(req,method_name,0,str(ex))
  clarens_util.write_response(req,response)
  return apache.OK


# ---------------------------------------------------------------------------------
# Method description data
import ProdCommon.Core.Initialize

methods_list={'lastCall'         :lastCall,
              'lastServiceCall'  :lastServiceCall }

major_version=1
minor_version=0

init_priority=0
