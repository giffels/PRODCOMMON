#!/usr/bin/env python
"""
_RunResDBAccess_

Library providing query based access to a RunResDB that can
be used directly by other python modules, or wrapped to create
command line tools

"""
import re
from RunRes.RunResDB import RunResDB

_RunResURLMatch = re.compile("^runresdb://", re.IGNORECASE)

def loadRunResDB(dbFileURL):
    """
    _loadRunResDB_

    Create a RunResDB instance and load the url provided
    returns the loaded DB instance.
    TODO:throws a RunResError if there is a problem

    """
    
    rrdb = RunResDB()
    rrdb.addInputURL(dbFileURL)
    rrdb.load()
    return rrdb



def queryRunResDB(dbInstance, queryString):
    """
    _queryRunResDB_

    Execute the queryString as a Query on the RunResDB instance
    provided, return the resulting list of values
    """
    if isQueryURL(queryString):
        queryString = parseQueryURL(queryString)
    return dbInstance.query(queryString)


def isQueryURL(queryURL):
    """
    _isQueryURL_

    return True if the queryURL has the form runresdb://<query>
    """
    if _RunResURLMatch.match(queryURL):
        return True
    return False


def parseQueryURL(queryURL):
    """
    _parseQueryURL_

    RunResDB query urls are of the form:
    runresdb://QueryPath
    This method parses the url and removes the leading url terms from
    the query

    """
    if _RunResURLMatch.match(queryURL):
        query = _RunResURLMatch.sub("", queryURL, 1)
    else:
        # TODO: Raise invalid url exception
        query = queryURL
    return query




    
def executeQueryURL(dbFileURL, queryURL):
    """
    _executeQueryURL_

    Load the db provided, evaluate the query url and return the
    results of the query
    """
    rrdb = loadRunResDB(dbFileURL)
    queryStr = parseQueryURL(queryURL)
    queryResult = queryRunResDB(rrdb, queryStr)
    return queryResult


def executeQueryURLList(dbFileURL, *queryURLs):
    """
    _executeQueryURLList_

    Execute a list of queries on the DB URL provided.
    The DB is loaded only once, so it is more efficient to use this
    if you have several queries to run.

    """
    results = {}
    rrdb = loadRunResDB(dbFileURL)
    for queryURL in queryURLs:
        queryStr = parseQueryURL(queryURL)
        queryResult = queryRunResDB(rrdb, queryStr)
        results[queryURL] = queryResult
    return results


