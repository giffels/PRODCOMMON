#!/usr/bin/env python
"""
__Operation__

A set of generic methods for performing basic operations such
as insert, remove and update on simple tables. The assumption is
that the tables have a key column of type id
"""

import base64
import cPickle
import logging
from ProdCommon.Database.Connection import Connection

# this is a cache that stores items that 
# have been retrieved it is used to 
# check which of values that are updated
# are changed so only to update changed values
# of the objects.
#dataCache = {}


class Operation:
    
    def __init__(self, connection_parameters):
        if not connection_parameters.has_key('dbType'):
           connection_parameters['dbType'] =  'mysql'
                           
        self.connection = Connection(connection_parameters)
        
    
    def __getattr__ (self,name):
        
         
        if (name in ['commit','rollback','close']):        
          return getattr(self.connection,name)
        else:
          raise AttributeError()
        
    def __setattr__ (self,name,val):
        
        if name == 'connection':
           self.__dict__['connection'] = val     

    def insert(self,table_name, rows , columns =[], encode = [], encodeb64 = [], \
        duplicate = False):
        """
        _insert_
      
        Generic way of inserting rows into a table. The column names
        are based on the keys of the rows dictionary or list.
    
        Argument:
         
        table_name: Table name in which data will be inserted
        rows:
          if type(rows)== dict, THEN keys will be the column id's and rows.values() will be the values against those id's    
          if type(rows)==list, Then column will be the collumn id list and rows will be the data values against those id's
           

        """
             
        if  ((type(rows) not in [dict,list,tuple])  or (type(columns) not in [list,tuple])):
           raise ValueError("Invalid Argument type provided")


        # make sure everything is in the right format
        #if type(rows) == dict:
        rows = [rows]
        
        # if nothing to do return
        if len(rows) == 0:
            return
        if type(rows[0]) == dict: 
            column_names = rows[0].keys()
        if type(rows[0]) == list:
            column_names = columns
        logging.debug("Inserting objects of type "+table_name)
        sqlStr = "INSERT INTO "+table_name+"("
        comma = False
        for column_name in column_names:
            if comma:
                sqlStr += ', '
            comma = True
            sqlStr += column_name
        sqlStr += ')'
        sqlStr += ' VALUES '
        row_comma = False
        for row in rows:
            if row_comma:
                sqlStr += ', '
            row_comma = True
            sqlStr += '('
            entry_comma = False
            if type(rows[0]) == dict:
                for column_name in column_names:
                    if column_name in encode:
                        entry = base64.encodestring(cPickle.dumps(row[column_name]))
                    elif column_name in encodeb64:
                        entry = base64.encodestring(row[column_name])
                    else:
                        entry = row[column_name]
                    if entry_comma:
                        sqlStr += ', '
                    entry_comma = True
                    if entry == "LAST_INSERT_ID()":
                        sqlStr += entry
                    else:
                        sqlStr += '"'+str(entry)+'"'
                sqlStr += ')'
            else:
                i=0
                for column_name in column_names:
                     
                    if column_name in encode:
                        entry = base64.encodestring(cPickle.dumps(row[i]))
                    elif column_name in encodeb64:
                        entry = base64.encodestring(row[i])
                    else:
                        entry = row[i]
                    if entry_comma:
                        sqlStr += ', '
                    entry_comma = True
                    if entry == "LAST_INSERT_ID()":
                        sqlStr += entry
                    else:
                        sqlStr += '"'+str(entry)+'"'
                    i += 1
                sqlStr += ')'
        if duplicate:
            sqlStr+=" ON DUPLICATE KEY UPDATE "
            comma=False
            i = 0
            for column_name in column_names:
                if comma:
                   sqlStr+=','
                elif not comma :
                   comma=True
                if type(row) == dict:
                   sqlStr+= column_name + '="'+ str(row[column_name]) + '"'
                else:
                   sqlStr+= column_name + '="'+ str(row[i]) + '"'
                i += 1        
        
        self.connection.execute(sqlStr)
      
    def insert2update(self, table_name, rows , columns =[], encode = [], encodeb64 = []):
        self.insert(table_name, rows, columns, encode, encodeb64, duplicate = True)

  
  
    def retrieve(self, table_name, values, columns=[], decode=[], decodeb64=[]):
       """
       __retrieve__

       Generic retrieve table rows as dictionaries, where it is
       assumed that the key of tabe is labeled "id"
      
       Argument:

       table_name : Name of table from which data would be fetched 
       values : dictionary containing key,val pairs comprising where clause of select statement
       columns : List of column id's to be fetched   

       Return:
       List containing rows as dictionaries   

       """
       
       if  ((type(values) not in [dict])  or (type(columns) not in [list])):
           raise ValueError("Invalid Argument type provided")


       # if nothing to do return
       if len(values) == 0:
          return

       logging.debug("Retrieving objects of type %s" % table_name)

       sqlStr = 'SELECT '
       if ( len(columns) == 0 ):
          sqlStr+= '*'
       else:
          comma = False
          for column_name in columns:
             if comma:
                sqlStr += ', '
             comma = True
             sqlStr += column_name
       sqlStr += ' FROM %s WHERE ' % table_name

       entry_and=False
       for value_name in values.keys():
          if entry_and:
             sqlStr += 'and '
          entry_and=True
          sqlStr += value_name
          if type(values[value_name]) == list:
             sqlStr += " in %s" % str(tuple(values[value_name]))
          else:
             sqlStr += " = '%s'" % values[value_name]      
       

       self.connection.execute(sqlStr)
         
       return self.connection.convert(rows=self.connection.fetchall(),oneItem=False,decode=[],decode64=decodeb64)
  
  
  
    
    def remove(self, table_name, column_name, keys = []):
        """
        __remove__
    
        Generic remove from table where it is assumed that the 
        the key of the table is labeled "id"
        """
    
        if(type(keys)!=list):
            keys=[str(keys)]
        if len(keys)==0:
            return []

        if len(keys)==1:
            sqlStr="""DELETE FROM %s WHERE %s="%s"
            """ % (table_name, str(column_name), str(keys[0]))
        else:
            sqlStr="""DELETE FROM %s WHERE %s IN 
            %s """ % (table_name, str(column_name), str(tuple(keys)))
        self.connection.execute(sqlStr)
    
    def update(self, table_name, keys, row, columns = [], encode = [], encodeb64 = []):
        """
        update method to update the table rows based on the primary key 'id' labled as keys

        Arguments:

        table_name: Table Name on which update query will run
        keys: 'WHERE CLUASE' An id or list of id's for use in 'IN' clause, i.e id = 1, id IN [1,2]  

        row: If dict, Collumn id's against values to update
             If list, data values  

        columns: if type(row) == list, then columns contain column id's to update
        """
        # make sure everything is in the right format
        if type(row) == dict: 
            column_names = row.keys()
        if type(row) == list:
            column_names = columns
        logging.debug("Updating objects of type "+table_name)
        sqlStr = "UPDATE "+table_name+" SET "
        row_comma = False
        if row_comma:
            sqlStr += ', '
        row_comma = True
        entry_comma = False
        if type(row) == dict:
            for column_name in column_names:
                if column_name in encode:
                    entry = base64.encodestring(cPickle.dumps(row[column_name]))
                elif column_name in encodeb64:
                    entry = base64.encodestring(row[column_name])
                else:
                    entry = row[column_name]
                if entry_comma:
                    sqlStr += ', '
                entry_comma = True
                if entry == "LAST_INSERT_ID()":
                    sqlStr += column_name + "=\"" + entry + "\""
                else:
                    sqlStr += column_name + "=\"" +str(entry)+ '"'
        else:
            i=0
            for column_name in column_names:
                if column_name in encode:
                    entry = base64.encodestring(cPickle.dumps(row[i]))
                elif column_name in encodeb64:
                    entry = base64.encodestring(row[i])
                else:
                    entry = row[i]
                if entry_comma:
                    sqlStr += ', '
                entry_comma = True
                if entry == "LAST_INSERT_ID()":
                    sqlStr += column_name + "=\"" + entry + "\""
                else:
                    sqlStr += column_name + "=\"" +str(entry)+ '"'
                i += 1
        if type(keys) == list:
            sqlStr +=  """ WHERE id IN %s """ % (str(tuple(keys)))
        else:
            sqlStr +=  """ WHERE id="%s" """ % (str(keys))
       
        
        self.connection.execute(sqlStr)
