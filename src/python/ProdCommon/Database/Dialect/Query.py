
class Interface:

   def __init__(self):
       pass

   def build(self,parameters):

       return {}

   def __call__(self,parameters):
       self.build(parameters)

