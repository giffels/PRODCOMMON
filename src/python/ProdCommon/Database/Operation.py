#!/usr/bin/env python
"""
__Operation__

A set of generic methods for performing basic operations such
as insert, remove and update on simple tables. The assumption is
that the tables have a key column of type id
Operation class itself instantiate the Connection instance in its constructor. One who uses
Operation class doesn't need to worry about Connection stuff.
"""

import base64
import cPickle
import logging
from ProdCommon.Database.Connection import Connection
from sqlalchemy import MetaData, Table
from sqlalchemy.sql import text


class Operation:
    
    def __init__(self, connection_parameters):
        if not connection_parameters.has_key('dbType'):
           connection_parameters['dbType'] =  'mysql'
                           
        self.connection = Connection(**connection_parameters)

        self.metaData = MetaData()
        self.metaData.bind = self.connection.engine

    def __del__ (self):

        del self.connection
        
        
    
    def __getattr__ (self,name):
        
        #  //
        # // Giving the access to the following method of Connection class through Operation instance.
        #//
          
        if (name in ['commit','rollback','close','execute']):        
          return getattr(self.connection,name)

        else:
          raise AttributeError()
        
    def __setattr__ (self,name,val):
        
        if name == 'connection':
           self.__dict__['connection'] = val     

        if name == 'metaData':
           self.__dict__['metaData'] = val


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
         
        #  //
        # // CHECK VALIDITY OF ARGUMENTS PASSED
        #//
             
        if  ((type(rows) not in [dict,list,tuple])  or (type(columns) not in [list,tuple])):
           raise ValueError("Invalid Argument type provided")


        # make sure everything is in the right format
        if type(rows) == dict:
            rows = [rows]
        elif type(rows) == list:           
           if type(rows[0]) not in  [list, dict]:
              rows = [rows]  

        # if nothing to do return
        if len(rows) == 0:
            return
        column_names = None  
        if type(rows[0]) in  [dict,list]: 
            if self.connection.connection_parameters['dbType'] == 'oracle':
              if len(rows) > 1:
                raise RuntimeError('Operations not permitted: Please use insertBulk for inserting muliple rows in Oracle DB',1)
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
                        sqlStr += '\''+str(entry)+'\''
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
                        sqlStr += '\''+str(entry)+'\''
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
                   sqlStr+= column_name + '=\''+ str(row[column_name]) + '\''
                else:
                   sqlStr+= column_name + '=\''+ str(row[i]) + '\''
                i += 1
       
        
        rowsModified = self.connection.execute(sqlStr)
         
        return rowsModified
      
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
  
  
  
  
    def remove(self, table_name, rows):
        """
        __remove__
    
        Generic remove method
        rows : Dictionary that contains the key value pair forming the where clause of remove command

        """
    
        
        if  (type(rows) not in [dict]):
           raise ValueError("Invalid Argument type provided")

        if len(rows)==0:
            return []
          
        if rows is not None :
           sqlStr = "DELETE FROM %s WHERE " % table_name
           sqlStr += "  "

           entry_and=False
           for key_name in rows.keys():
             if entry_and:
                sqlStr += 'and '
             entry_and=True
             sqlStr += key_name

             if type(rows[key_name]) == list:
                sqlStr += " in %s" % str(tuple(rows[key_name]))
             else:
                sqlStr += " = '%s'" % rows[key_name]   
        
        
        rowsModified = self.connection.execute(sqlStr)
        return rowsModified
    
    def update(self, table_name, rows, columns = [], keys = {}, encode = [], encodeb64 = []):
        """
        update method to update the table rows based on the primary keys provided

        Arguments:

        table_name: Table Name on which update query will run
        keys: Dictionary containing the key/value pair forming the where clause.
              It corresponds to the rows on which update will be called
              If value is a list, a 'in (a,b,c,d)' is used in the query

        rows: If dict, Collumn id's against values to update
             If list, data values  

        columns: if type(rows) == list, then columns contain column id's to update
        """


        # make sure everything is in the right format
        if  (type(keys) not in [dict]):
           raise ValueError("Invalid Argument type provided")

        if len(keys) == 0:
           logging.info('Nothing to update')     
           return

        if type(rows) == dict: 
            column_names = rows.keys()
        if type(rows) == list:
            column_names = columns
        logging.debug("Updating objects of type "+table_name)
        sqlStr = "UPDATE "+table_name+" SET "
        row_comma = False
        if row_comma:
            sqlStr += ', '
        row_comma = True
        entry_comma = False
        if type(rows) == dict:
            for column_name in column_names:
                if column_name in encode:
                    entry = base64.encodestring(cPickle.dumps(rows[column_name]))
                elif column_name in encodeb64:
                    entry = base64.encodestring(rows[column_name])
                else:
                    entry = rows[column_name]
                if entry_comma:
                    sqlStr += ', '
                entry_comma = True
                if entry == "LAST_INSERT_ID()":
                    sqlStr += column_name + "=\'" + entry + "\'"
                else:
                    sqlStr += column_name + "=\'" +str(entry)+ '\''
        else:
            i=0
            for column_name in column_names:
                if column_name in encode:
                    entry = base64.encodestring(cPickle.dumps(rows[i]))
                elif column_name in encodeb64:
                    entry = base64.encodestring(rows[i])
                else:
                    entry = rows[i]
                if entry_comma:
                    sqlStr += ', '
                entry_comma = True
                if entry == "LAST_INSERT_ID()":
                    sqlStr += column_name + "=\'" + entry + "\'"
                else:
                    sqlStr += column_name + "=\'" +str(entry)+ '\''
                i += 1
         
        if keys is not None :            
           sqlStr += " WHERE "

           entry_and=False
           for key_name in keys.keys():
             if entry_and:
                sqlStr += ' and '
             entry_and=True
             sqlStr += key_name

             values = keys[key_name]

             if type(values) != list:
                 values = [values]
             if len(values) > 1:
                 sqlStr += " in ("
                 for value in values:
                     if key_name in encode:
                         entry = base64.encodestring(cPickle.dumps(value))
                     elif key_name in encodeb64:
                         entry = base64.encodestring(value)
                     else:
                         entry = value
                     sqlStr += "'%s'," % entry
                 sqlStr = sqlStr.rstrip(',') + ')'
             else:
                 value = values[0]
                 if key_name in encode:
                     entry = base64.encodestring(cPickle.dumps(value))
                 elif key_name in encodeb64:
                     entry = base64.encodestring(value)
                 else:
                     entry = value
                 sqlStr += " = '%s'" % entry

        rowsModified=self.connection.execute(sqlStr)
        return rowsModified

  
    def arrayInsert(self, table_name, rows, key = None, encode = [], encodeb64 = []):
        """
        _arrayInsert_

        Method that inserts muliple rows in one go.
        Argument:

        table_name: Table name in which data will be inserted
        rows: List of dictionaries in which each dictionary represents each row to be inserted
        key: Dictionary of atmost element where key will be collumn id and value will be sequence name attached to it 
        """

        if key != None:
            if type(key) == dict:
                if len(key) > 1:
                    raise ValueError('Invalid parameter: key, it must be a dictionary containing at most one element')
            else:
                raise ValueError('Invalid parameter: key, it must be a dictionary containing at most one element')

        if type(rows) == dict:
           rows = [rows]

        if type(rows) != list:
           raise ValueError ("Invalid row format provided. Please provide list of dictionaries")

        table = Table(table_name, self.metaData, autoload = True)

        for row in rows:

           if type(row) != dict:
              raise ValueError("Invalid row: Expecting Dictionary")
           self.__lowerCaseFormat(row)
           for column_name in encode:
              if row.has_key(column_name):
                 row[column_name] = base64.encodestring(cPickle.dumps(row[column_name]))

           for column_name in encodeb64:
              if row.has_key(column_name):
                 row[column_name] = base64.encodestring(row[column_name])

        resultSet = None
        
        try:
            if key != None:

                self.__lowerCaseFormat(key)                
                id = key.keys()[0]
                resultSet = self.connection.session.execute(table.insert(values = {id:text(key[id]+'.nextval')}), rows)
            else:
                resultSet = self.connection.session.execute(table.insert(), rows)
        except Exception, e:
            msg = 'Exception caught while inserting data: \n'
            msg += str(e)
            raise RuntimeError(1, msg)  
        cursor = resultSet.cursor
        rowsModified = cursor.rowcount 

        return rowsModified
 
  
  
    def insertWithBind (self, table_name, rows, columns = [], key =  None, encode = [], encodeb64 = []):
        """
        _insertWithBind_
        Method that use bind params for inserting data in order to make efficient use of oracle shared pool. Let Oracle do 
        soft parse rather than hard Parse

        Argument:

        table_name: Table name in which data will be inserted
        rows:
          if type(rows)== dict, THEN dict keys will be the column id's and dict values will be the values against those id's
          if type(rows)==list, Then column will be the collumn id list and rows will be the data values against those id's

        columns:
          if type(rows)== dict, THEN columns will be a list containing name of columns to be inserted   

        key: Dictionary of atmost one element where key will be collumn id and value will be sequence name attached to it
        
        Four Input Formats supported:
           rows: dict containing one row data in the form of key/value pair         column : [] 
           rows: list of dict where each dict show one particular row               column : []
           rows: list of lists where each list show any particular row data         column: list of column names
           rows: list contating one row data                                        column: list of column names        
        """

        #// Make sure that sequence is provided in correct format 
        if key != None:

            if type(key) == dict:

                if len(key) > 1:
                    raise ValueError('Invalid parameter: key, it must be a dictionary containing at most one element')
            else:
                raise ValueError('Invalid parameter: key, it must be a dictionary containing at most one element')


        #//Assure correct format If only one row data is provided in dict format   
        if type(rows) == dict:
           
           rows = [rows]

        #//Assure correct format If only one row data is provided in a list 
        elif type(rows) == list:
           
           if type(rows[0]) not in  [list, dict]:

              rows = [rows]
 

        if type(rows) != list:
           raise ValueError ("Invalid row format provided. Please provide list of dictionaries")

        #// if no row data then do nothing
        if len(rows) == 0:
           return

        column_names = None
        
        #// Assiging to be inserted column names to separate list
        if type(rows[0]) == dict:
           column_names = rows[0].keys()
          
        else:
           column_names = columns 
           
        sqlStr = "Insert into " + table_name + " ( "
        
        column = " "
        bindParams = " " 
        comma = False

        #// Constructing comma separated string of column name 
        #// Constructing bindparams of format :column         
          
        for item in column_names:

           if comma:
              column += ", "
              bindParams += ", "
           comma = True

           column += item
 
           #// preparing bind sql statement for both mysql, oracle dbtypes              

           if self.connection.connection_parameters['dbType'] == 'oracle':
              bindParams += ":" + item
           elif self.connection.connection_parameters['dbType'] == 'mysql':
              bindParams += "%s"
           else:
              raise RuntimeError ('dbType not supported',1)    

        #// Adding sequence to raw sql incase of oracle
        #// No need to add code for mysql as mysql picks auto incremented value automatically

        if key !=None:

          if self.connection.connection_parameters['dbType'] == 'oracle':

             id = key.keys()[0]
             if id not in column_names:
                column += ", " + id
                bindParams += "," + key[id] +".nextval "

        sqlStr += column + " ) values ( " + bindParams + " ) "
       

        #// List containing bind parameters values to be passed to execute method in addition to bind param statment 
        rowBind = []        

               
        #//Looping over multi-row inserts
        for row in rows:
           
           #//If input row is provided as dict  

           if type(row) == dict:              

              for column_name in encode:
                 if row.has_key(column_name):
                    row[column_name] = base64.encodestring(cPickle.dumps(row[column_name]))

              for column_name in encodeb64:
                 if row.has_key(column_name):
                    row[column_name] = base64.encodestring(row[column_name])

              #// Separate bind param values format for oracle and mysql. 
              #// Oracle supports list of dict where dict contains key/value pair of one row.
              #// Mysql supports list of lists where each list show a unique row
                           
              if self.connection.connection_parameters['dbType'] == 'oracle':

                 rowBind.append(row) 
              elif self.connection.connection_parameters['dbType'] == 'mysql':
                 rowBind.append(row.values())


           elif type(row) == list:

              tempOracle = {}
              tempMysql = []
              i = 0              
              oraCheck = False
              for column_name in column_names:


                    if column_name in encode:
                        entry = base64.encodestring(cPickle.dumps(row[i]))
                    elif column_name in encodeb64:
                        entry = base64.encodestring(row[i])
                    else:
                        entry = row[i]
                    if self.connection.connection_parameters['dbType'] == 'oracle':
                       oraCheck = True
                       tempOracle[column_name] =  entry

                    elif self.connection.connection_parameters['dbType'] == 'mysql':
                       tempMysql.append(entry) 
 
                    i = i+1

              if oraCheck:
                 rowBind.append(tempOracle)
              else:
                 rowBind.append(tempMysql)   
                    
                    
        
        resultSet = self.connection.session.execute(sqlStr, rowBind)
        cursor = resultSet.cursor
        rowsModified = cursor.rowcount
        return rowsModified


    def __lowerCaseFormat (self, input):
        """
        __lowerCaseDict__
        Method to convert dictionary keys to lowercase

        HACK to make queries sqlalchemy compatible because sqlalchemy only bind params if column names are provided in
        lowercase
        """

        if type(input) == dict:

           tempDict = input
           for k,v in input.items():
              
              if not k.islower():
                 tempDict[k.lower()] = v
                 del tempDict[k]


 
  
        


