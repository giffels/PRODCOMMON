#!/usr/bin/env python
"""
_TrackingDB_

"""

__version__ = "$Id: TrackingDB.py,v 1.27 2009/09/17 13:09:13 gcodispo Exp $"
__revision__ = "$Revision: 1.27 $"
__author__ = "Carlos.Kavka@ts.infn.it"

from copy import deepcopy
from ProdCommon.BossLite.Common.Exceptions import DbError
from ProdCommon.BossLite.Common.System import evalCustomList

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
        valueList = ','.join([x[1] for x in fields])

        # prepare query
        query = 'insert into ' + obj.tableName + '(' + fieldList + ') ' + \
                       'values(' + valueList + ')'

        # execute query
        try:
            rows = self.session.modify(query)
        except Exception, msg:
            raise DbError(msg)

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
        listOfFields = ' and '.join([('%s'+ operator +'%s') % (key, value)
                                     for key, value in fields
                                ])

        # check for general query for all objects
        if listOfFields != "":
            listOfFields = " where " + listOfFields

        # prepare query
        query = 'select ' + ', '.join(dbFields) + ' from ' +  tableName + \
                ' ' + listOfFields

        # execute query
        results = None
        theList = []
        try:
            results = self.session.select(query)
        except Exception, msg:
            raise DbError(msg)

        if results is None :
            return theList

        # get all information and build objects
        for row in results:

            # fill object
            obj = self.fillObject( row, template, objectFields )

            # add to list
            theList.append(obj)

        # return the list
        return theList

    ##########################################################################

    def selectDistinct(self, template, distinctAttr, strict = True):
        """
        _select_
        """

        # get template information
        mapping = template.__class__.fields.items()
        tableName = template.__class__.tableName

        # get field mapping in order
        fieldMapping = [(key, value) for key, value in mapping]
        objectFields = [key[0] for key in fieldMapping]
        distFields = [key[1] for key in fieldMapping if key[0] in distinctAttr]

        # get matching information from template
        fields = self.getFields(template)

        # determine if comparison is strict or not
        if strict:
            operator = '='
        else:
            operator = ' like '
        listOfFields = ' and '.join([('%s'+ operator +'%s') % (key, value)
                                     for key, value in fields
                                ])

        # check for general query for all objects
        if listOfFields != "":
            listOfFields = " where " + listOfFields

        # prepare query
        query = 'select distinct (' + ', '.join(distFields) + ')' + \
                ' from ' +  tableName + \
                ' ' + listOfFields

        # execute query
        results = None
        theList = []
        try:
            results = self.session.select(query)
        except Exception, msg:
            raise DbError(msg)

        if results is None :
            return theList

        # get all information and build objects
        for row in results:

            # fill object
            obj = self.fillObject( row, template, objectFields )

            # add to list
            theList.append(obj)

        # return the list
        return theList

    ##########################################################################

    def selectJoin(self, template, jTemplate, jMap=None, less=None, more=None, options=None):
        """
        _selectJoin_

        select from template and jTemplate, using join condition from jMap
        """

        # evaluate options
        opt = { 'strict' : True,
                'jType'  : '',
                'limit'  : None,
                'offset' : None,
                'inList' : None }
        if options is not None :
            opt.update( options )

        if more is None :
            more = {}

        if less is None :
            less = {}

        # get template information
        dbMap = template.__class__.fields
        mapping = template.__class__.fields.items()
        tableName = template.__class__.tableName

        # get template information
        jDbMap = jTemplate.__class__.fields
        jMapping = jTemplate.__class__.fields.items()
        jTableName = jTemplate.__class__.tableName

        # get field mapping in order
        fieldMapping = [(key, value) for key, value in mapping]
        objectFields = [key[0] for key in fieldMapping]
        dbFields = [key[1] for key in fieldMapping]

        # get field mapping in order for join table
        jFieldMapping = [(key, value) for key, value in jMapping]
        jObjectFields = [key[0] for key in jFieldMapping]
        jDbFields = [key[1] for key in jFieldMapping]

        # get matching information from template
        fields = self.getFields(template)

        # get matching information from join template
        jFields = self.getFields(jTemplate)

        # evaluate eventual lists
        if opt['inList'] is None :
            listOfFields = ''
        else :
            for key in opt['inList'].keys() :
                k = template.__class__.fields[key]
                listOfFields = 't1.' + k + ' in (' + \
                                ','.join( str(val ) for val in opt['inList'][key]) + ') ' 
        if listOfFields != "" :
            listOfFields += ' and '

        # determine if comparison is strict or not
        if opt['strict']:
            operator = '='
        else:
            operator = ' like '

        # is there a set of field for more/less comparison?
        listOfFields += ' and '.join([('t1.%s'+ operator +'%s') % (key, value)
                                      for key, value in fields
                                      if key not in more and key not in less
                                ])
        jListOfFields = ' and '.join([('t2.%s'+ operator +'%s') \
                                      % (key, value)
                                      for key, value in jFields
                                      if key not in more and key not in less
                                ])

        # check for general query for all objects
        if listOfFields != "" and  jListOfFields != "":
            listOfFields = " where " + listOfFields + " and " + jListOfFields

        elif listOfFields != "":
            listOfFields = " where " + listOfFields

        elif jListOfFields != "":
            listOfFields = " where " + jListOfFields

        # evaluate more
        for key, val in more.iteritems():
            print key, jDbMap[key], val
            if key in objectFields :
                listOfFields += ' and t1.%s>%s ' % ( dbMap[key], val )
            elif key in jObjectFields :
                listOfFields += ' and t2.%s>%s ' % ( jDbMap[key], val )

        # evaluate less
        for key, val in less.iteritems():
            if key in objectFields :
                listOfFields += ' and t1.%s<%s ' % ( dbMap[key], val )
            elif key in jObjectFields :
                listOfFields += ' and t2.%s<%s ' % ( jDbMap[key], val )

        # evaluate join conditions
        jLFields = ''
        if jMap is not None :
            jLFields = ' and '.join([('t1.%s=t2.%s') % ( \
                template.__class__.fields[key], \
                jTemplate.__class__.fields[value])
                                     for key, value in jMap.iteritems()
                                     ])

        if jLFields != '':
            jLFields = ' on (' + jLFields + ') '

        # what kind of join?
        if opt['jType'] == '' :
            qJoin = ' inner join '
        elif opt['jType'] == 'left' :
            qJoin = ' left join '
        elif opt['jType'] == 'right' :
            qJoin = ' right join '

        # prepare query
        query = 'select ' + ', '.join( ['t1.'+ key for key in dbFields] ) + \
                ', ' + ', '.join( ['t2.'+ key for key in jDbFields] ) + \
                ' from ' +  tableName + ' t1 ' + qJoin + \
                jTableName + ' t2 ' + jLFields + listOfFields

        # limit?
        if opt['limit'] is not None :
            if opt['offset'] is None or int(opt['offset']) == 0 :
                query += ' limit %s' % opt['limit']
            else  :
                query += ' limit %s,%s' % (opt['offset'], opt['limit'])


        # execute query
        results = None
        theList = []
        try:
            results = self.session.select(query)
        except Exception, msg:
            raise DbError(msg)

        if results is None :
            return theList

        # get all information and build objects
        size =  len( mapping )
        for row in results:
        
            # fill objects
            obj = self.fillObject( row, template, objectFields )
            jObj = self.fillObject( row[size:], jTemplate, jObjectFields )
        
            # add to list
            theList.append((obj, jObj))

        # return the list
        return theList


    ##########################################################################

    def update(self, template, skipAttributes = None):
        """
        _update_
        """
        # get template information
        tableName = template.__class__.tableName
        tableIndex = template.__class__.tableIndex
        tableIndexRes = [ template.mapping[key]
                          for key in template.__class__.tableIndex ]

        if skipAttributes is None :
            skipAttributes = {}

        # get specification for keys (if any)
        keys = [(template.mapping[key], template.data[key])
                             for key in tableIndex
                             if template.data[key] is not None]
        keysSpec = " and ".join(['%s="%s"' % (key, value)
                                 for key, value in keys
                                ])

        unlikeSpec = " and ".join(['%s!="%s"' % (key, value)
                                   for key, value in skipAttributes.iteritems()
                                   ])

        if keysSpec != "" and unlikeSpec != "" :
            keysSpec += ' and ' + unlikeSpec
        elif unlikeSpec != "" :
            keysSpec = unlikeSpec

        if keysSpec != "" :
            keysSpec = ' where ' + keysSpec

        # define update list (does not include keys)
        fields = self.getFields(template)

        listOfFields = ','.join(['%s=%s' % (key, value)
                                     for key, value in fields
                                     if key not in tableIndexRes
                                ])

        # return if there are no fields to update
        if listOfFields == "":
            return 0

        # prepare query
        query = 'update ' + tableName + ' set  ' + listOfFields + \
                keysSpec
        # execute query
        try:
            rows = self.session.modify(query)
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
        listOfFields = ' and '.join(['%s=%s' % (key, value)
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
            rows = self.session.modify(query)
        except Exception, msg:
            raise DbError(msg)

        # return number of rows removed
        return rows

    ##########################################################################

    def getFields(self, obj):
        """
        prepare field sections in query
        """

        # get access to default values and mappings
        defaults = obj.__class__.defaults
        mapping = obj.__class__.fields

        # build list of fields and values with non default values
        fields = [(mapping[key], '"' + str(value).replace('"','""') + '"') \
                  for key, value in obj.data.items()
                  if value != defaults[key]
        ]

        # return it
        return fields


    ##########################################################################

    def fillObject( self, dbRow, template, objectFields ):

        # create a single object
        obj = type(template)()
 
        # fill fields
        for key, value in zip(objectFields, dbRow):
 
            # check for NULLs
            if value is None:
                obj[key] = deepcopy( template.defaults[key] )
 
            # check for lists
            elif type(template.defaults[key]) == list:
                try :
                    obj[key] = evalCustomList(value)
                except SyntaxError:
                    obj[key] = [ value ]
 
            # other objects get casted automatically
            else:
                obj[key] = value
 
            # mark them as existing in database
            obj.existsInDataBase = True
 
        return obj

    ##########################################################################
    
    ##DanieleS NOTE: ToBeRevisited
    def distinctAttr(self, template, value_1 , value_2, alist ,  strict = True):
        """
        _distinctAttr_
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
            if key == value_1:
                dbFields = [val]
                objectFields = [key]
            if key == value_2:
                field = val
        #        break
        # get matching information from template
     #   fields = self.getFields(template)
        # determine if comparison is strict or not
        if strict:
            operator = '='
        else:
            operator = ' like '
        listOfFields = ' or '.join([('%s'+ operator +'%s') % (field, value)
                                     for value in alist
                                ])
        # check for general query for all objects
        if listOfFields != "":
            listOfFields = " where " + listOfFields

        # DanieleS.
        # prepare query
        query = 'select distinct (' + ', '.join(dbFields) + ') from ' +  tableName + \
                ' ' + listOfFields

        # execute query
        results = None
        theList = []
        try:
            results = self.session.select(query)
        except Exception, msg:
            raise DbError(msg)

        if results is None :
            return theList

        # get all information and build objects
        for row in results:

            # create a single object
            # template = deepcopy(template)
            obj = type(template)()

            # fill fields
            for key, value in zip(objectFields, row):

                # check for NULLs
                if value is None:
                    obj[key] = deepcopy(template.defaults[key])

                # check for lists
                elif type(template.defaults[key]) == list:
                    try :
                        obj[key] = evalCustomList(value)
                    except SyntaxError:
                        obj[key] = [ value ]

                # other objects get casted automatically
                else:
                    obj[key] = value

                # mark them as existing in database
                obj.existsInDataBase = True

            # add to list
            theList.append(obj)

        # return the list
        return theList



    ### DanieleS
    def distinct(self, template, value_1 , strict = True):
        """
        _distinct_
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
            if key == value_1:
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
        listOfFields = ' and '.join([('%s'+ operator +'%s') % (key, value)
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
        results = None
        theList = []
        try:
            results = self.session.select(query)
        except Exception, msg:
            raise DbError(msg)

        if results is None :
            return theList

        # get all information and build objects
        for row in results:

            # create a single object
            # template = deepcopy(template)
            obj = type(template)()

            # fill fields
            for key, value in zip(objectFields, row):

                # check for NULLs
                if value is None:
                    obj[key] = deepcopy(template.defaults[key])

                # check for lists
                elif type(template.defaults[key]) == list:
                    try :
                        obj[key] = evalCustomList(value)
                    except SyntaxError:
                        obj[key] = [ value ]

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

