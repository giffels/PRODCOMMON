
from ProdCommon.Core.GlobalRegistry import registerHandler

class HandlerInterface:

   def __init__(self):
       pass

   def handlerMethod():
       raise Exception("If you see this something is wrong")

   def __call__(self):
       self.handlerMethod()

class TestHandler1(HandlerInterface):


   def __init__(self):
       pass

   def handlerMethod(self):
       return "TestHandler1"

class TestHandler2(HandlerInterface):


   def __init__(self):
       pass

   def handlerMethod(self):
       return "TestHandler2"

class TestHandler3(HandlerInterface):


   def __init__(self):
       pass

   def handlerMethod(self):
       return "TestHandler3"

