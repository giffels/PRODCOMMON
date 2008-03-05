#!/usr/bin/env python
"""
_TrackingDB_

"""

__version__ = "$Id: TrackingDB.py,v 1.2 2008/03/04 15:40:41 spiga Exp $"
__revision__ = "$Revision: 1.2 $"
__author__ = "Carlos.Kavka@ts.infn.it"

from copy import deepcopy
from ProdCommon.BossLite.Common.Exceptions import DbError

class TrackingDB:
    """
    _TrackingDB_
    """

    ##########################################################################

    def __init__(self, session):
        """
        __init__
        """

        self.session = session

    ##########################################################################

    def insert(self, obj):
        """
        _insert_

        Uses default values for non specified parameters. Note that all
        parameters can be default, a useful method to book an ID.
        """

        # check for valid type insertion
        #if type(obj) not in self.__class__.validObjects:
        #    raise DbError("insertJob: cannot insert an object of type %s." % \
        #                   str(type(obj)))

        # get field information
        fields = self.getFields(obj)
        fieldList = ','.join([x[0] for x in fields])
        valueList = '","'.join([x[1] for x in fields])

        # add missing quotes to value list if not empty
        if valueList.strip() != "":
            valueList = '"' + valueList + '"'

        # prepare query 
        query = 'insert into ' + obj.tableName + '(' + fieldList + ') ' + \
                       'values(' + valueList + ')'

        # execute query
        try:
            rows = self.session.execute(query)
        except Exception, msg:
            raise DbError(str(msg))

        # done, return number of updated rows
        return rows

    ##########################################################################

    def select(self, template, strict = True):
        """
        _select_
        """

        # get template information
        mapping = template.__class__.fields.items()
        tableName = template.__class__.tableName

        # get field mapping in order
        fieldMapping = [(key, value) for key, value in mapping]
        objectFields = [key[0] for key in fieldMapping]
        dbFields = [key[1] for key in fieldMapping]

        # get matching information from template
        fields = self.getFields(template)

        # determine if comparison is strict or not
        if strict:
            operator = '='
        else:
            operator = ' like '
        listOfFields = ' and '.join([('%s'+ operator +'"%s"') % (key, value)
                                     for key, value in fields
                                ])

        # check for general query for all objects
        if listOfFields != "":
            listOfFields = " where " + listOfFields

        # prepare query
        query = 'select ' + ', '.join(dbFields) + ' from ' +  tableName + \
                ' ' + listOfFields

        # execute query
        try:
            self.session.execute(query)
        except Exception, msg:
            raise DbError(msg)

        # get all information
        results = self.session.fetchall()
        
        # build objects
        theList = []
        for row in results:

            # create a single object
            template = deepcopy(template)
            obj = type(template)()

            # fill fields
            for key, value in zip(objectFields, row):

                # check for NULLs
                if value is None:
                    obj[key] = template.defaults[key]

                # check for lists
                elif type(template.defaults[key]) == list:
                    obj[key] = eval(value)

                # other objects get casted automatically
                else:
                    obj[key] = value

                # mark them as existing in database
                obj.existsInDataBase = True

            # add to list 
            theList.append(obj)

        # return the list
        return theList

    ##########################################################################

    def update(self, template):
        """
        _update_
        """
        # get template information
        tableName = template.__class__.tableName
        tableIndex = template.__class__.tableIndex

        # get specification for keys (if any)
        keys = [(template.mapping[key], template.data[key])
                             for key in tableIndex 
                             if template.data[key] is not None]
        keysSpec = " and ".join(['%s="%s"' % (key, value)
                                     for key, value in keys
                                ])
        if keysSpec != "":
            keysSpec = ' where ' + keysSpec

        # define update list (does not include keys)
        fields = self.getFields(template)

        #listOfFields = ','.join(['%s="%s"' % (key, value)
        ## Added replace DanieleS.
        listOfFields = ','.join(['%s="%s"' % (key, value.replace('"','""'))
                                     for key, value in fields
                                     if key not in tableIndex
                                ])
        
        # return if there are no fields to update
        if listOfFields == "":
            return 0
         
        # prepare query
        query = 'update ' + tableName + ' set  ' + listOfFields + \
                keysSpec
        # execute query
        try:
            rows = self.session.execute(query)
        except Exception, msg:
            raise DbError(msg)

        # return number of modified rows
        return rows

    ##########################################################################

    def delete(self, template):
        """
        _delete_
        """

        # get template information
        tableName = template.__class__.tableName

        # get matching information from template
        fields = self.getFields(template)
        listOfFields = ' and '.join(['%s="%s"' % (key, value)
                                     for key, value in fields
                                ])

        # check for general query for all objects
        if listOfFields != "":
            listOfFields = " where " + listOfFields

        # prepare query
        query = 'delete from ' +  tableName + \
                ' ' + listOfFields

        # execute query
        try:
            rows = self.session.execute(query)
        except Exception, msg:
            raise DbError(msg)

        # return number of rows removed
        return rows

    def getFields(self, obj):
        """
        prepare field sections in query
        """

        # get access to default values and mappings
        defaults = obj.__class__.defaults
        mapping = obj.__class__.fields

        # build list of fields and values with non default values
        fields = [(mapping[key], str(value)) \
                     for key, value in obj.data.items()
                     if value != defaults[key]
                 ]

        # return it
        return fields

    ### DanieleS 
    def distinct(self, template, valueMMM , strict = True):
        """
        _select_
        """
        # get template information
        mapping = template.__class__.fields.items()
        tableName = template.__class__.tableName

        # get field mapping in order
        fieldMapping = [(key, value) for key, value in mapping]
       # objectFields = [key[0] for key in fieldMapping]
       # dbFields = [key[1] for key in fieldMapping]

        #DanieleS
        for key, val in fieldMapping:
            if key == valueMMM:
                dbFields = [val]
                objectFields = [key]
                break
        # get matching information from template
        fields = self.getFields(template)

        # determine if comparison is strict or not
        if strict:
            operator = '='
        else:
            operator = ' like '
        listOfFields = ' and '.join([('%s'+ operator +'"%s"') % (key, value)
                                     for key, value in fields
                                ])

        # check for general query for all objects
        if listOfFields != "":
            listOfFields = " where " + listOfFields

        # DanieleS.
        # prepare query
        query = 'select distinct (' + ', '.join(dbFields) + ') from ' +  tableName + \
                ' ' + listOfFields
        # execute query
        try:
            self.session.execute(query)
        except Exception, msg:
            raise DbError(msg)

        # get all information
        results = self.session.fetchall()
        
        # build objects
        theList = []
        for row in results:

            # create a single object
            template = deepcopy(template)
            obj = type(template)()

            # fill fields
            for key, value in zip(objectFields, row):

                # check for NULLs
                if value is None:
                    obj[key] = template.defaults[key]

                # check for lists
                elif type(template.defaults[key]) == list:
                    obj[key] = eval(value)

                # other objects get casted automatically
                else:
                    obj[key] = value

                # mark them as existing in database
                obj.existsInDataBase = True

            # add to list 
            theList.append(obj)

        # return the list
        return theList

    ##########################################################################

