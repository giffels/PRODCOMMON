#!/usr/bin/env python
"""
_AppVersionTools_

Tools for collecting Application version information from a tree of PayloadNode
instances

"""




def getAppVersion(payloadNode):
    """
    _getAppVersion_

    Get the application['Version'] field from the payloadNode
    provided. Convert empty string values to None

    """
    appString = payloadNode.application['Version']
    if appString == None:
        return None
    appString = appString.strip()
    if len(appString) == 0:
        return None
    return appString



class AppVersionAccumulator:
    """
    _AppVersionAccumulator_

    record unique application versions for a tree of PayloadNodes

    """
    def __init__(self):
        self.results = []


    def __call__(self, payloadNode):
        """
        _operator()_

        Act on PayloadNode instance to extract application string
        and append it to the results list if it isnt in there already

        """
        appVersion = getAppVersion(payloadNode)
        if appVersion == None:
            return
        if appVersion in self.results:
            return
        self.results.append(appVersion)
        return

def getApplicationVersions(payloadNode):
    """
    _getApplicationVersions_

    Get a list of Application Versions for all nodes in the
    payload node tree provided

    """
    accum = AppVersionAccumulator()
    payloadNode.operate(accum)
    return accum.results

    
    
