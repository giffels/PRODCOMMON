#!/usr/bin/env python
"""
_RunResComponent_

IMProvNode based object that makes it easy to
publish and retrieve information from a RunRes node structure.

A set of RunResComponents can be used to generate a multi file
RunResDB structure

A RunResDB node should contain data in a path based manner,
with only the bottom of the path containing actual
data, the rest of the node structure being for organisational
grouping only.

Providing data to a RunResComponent uses a path to specify where the
data will go and the data element itself.

For example:  /path1/path2/path3, data
will be added to the node as
<RunResDB>
  <path1>
    <path2>
      <path3>data</path3>

Adding two of these entries:  /path1/path2/path3, data2
results in

<RunResDB>
  <path1>
    <path2>
      <path3>data</path3>
      <path3>data2</path3>

Once a data element contains a value it cannot be used as part of a path,
and once an element has children (ie is a path element), data cannot be added
to it. 

"""

from IMProv.IMProvNode import IMProvNode
from IMProv.IMProvQuery import IMProvQuery

from RunRes.RunResError import RunResError


class RunResComponent(IMProvNode):
    """
    _RunResComponent_


    """
    def __init__(self):
        IMProvNode.__init__(self, "RunResDB")


    def addPath(self, newPath):
        """
        _addPath_

        Create a Path structure in this node without populating
        it with any data.
        The path provided is converted into Path nodes and added
        to the instance.
        Note that this adds Path nodes, so it should not include the
        names of any nodes that will be data nodes.

        """
        while newPath.startswith("/"):
            newPath = newPath[1:]
        while newPath.endswith("/"):
            newPath = newPath[:-1]
            

        pathList = newPath.split("/")
        attachToNode = self
        for item in pathList:
            matchingNode = self.getChildNode(attachToNode, item )
            if matchingNode == None:
                newNode = IMProvNode(item, None, Type = "Path")
                attachToNode.addNode(newNode)
                attachToNode = newNode
            else:                
                if self.isDataNode(matchingNode):
                    msg = "Attempting to add path node to data node:\n"
                    msg += "Node %s:\n" % matchingNode.name
                    msg += "Is a data node and cannot be extended with\n"
                    msg += "more child nodes\n"
                    msg += "Path: %s \n Data: %s\n" % (newPath, data)
                    raise RunResError(msg, ClassInstance = self)
                
                attachToNode = matchingNode
        return attachToNode
    
    

    def addData(self, locationPath, data):
        """
        _addData_

        Add a data entry to this component.
        The locationPath specifies where in this component the
        data will be stored, the data will be converted to a
        string and inserted as chardata into that path

        The last entry in the path is added as a Data node containing
        the data provided.

        """
        while locationPath.startswith("/"):
            locationPath = locationPath[1:]
        while locationPath.endswith("/"):
            locationPath = locationPath[:-1]
            

        pathList = locationPath.split("/")
        dataNodeName = pathList.pop()
        newNodeFlag = False
        attachToNode = self
        for item in pathList:
            matchingNode = self.getChildNode(attachToNode, item )
            if matchingNode == None:
                newNode = IMProvNode(item, None, Type = "Path")
                attachToNode.addNode(newNode)
                attachToNode = newNode
                newNodeFlag = True
            else:                
                if self.isDataNode(matchingNode):
                    msg = "Attempting to add path node to data node:\n"
                    msg += "Node %s:\n" % matchingNode.name
                    msg += "Is a data node and cannot be extended with\n"
                    msg += "more child nodes\n"
                    msg += "Path: %s \n Data: %s\n" % (locationPath, data)
                    raise RunResError(msg, ClassInstance = self)
                
                attachToNode = matchingNode
                
                
        if newNodeFlag:
            dataNode = IMProvNode(dataNodeName, str(data), Type = "Data")
            attachToNode.addNode(dataNode)
            return

        
        existingNode = self.getChildNode(attachToNode, dataNodeName)
        if existingNode != None:
            if self.isPathNode(existingNode):
                msg = "Node %s is a Path Node:" % attachToNode.name
                msg += "Cannot add Data to a path node:\n"
                msg += "Path: %s \n Data: %s\n" % (locationPath, data)
                raise RunResError(msg, ClassInstance = self)

        
        dataNode = IMProvNode(dataNodeName, str(data), Type = "Data")
        attachToNode.addNode(dataNode)
        return
        
    


    def hasChildNode(self, nodeRef, nodeName):
        """
        _hasChildNode_

        Return true if the node nodeRef has a child called
        nodeName

        """
        for child in nodeRef.children:
            if child.name == nodeName:
                return True
        return False


    def isDataNode(self, nodeRef):
        """
        _isDataNode_

       
        """
        if nodeRef.attrs.get("Type", None) == "Data":
            return True
        return False

    def isPathNode(self, nodeRef):
        """
        _isPathNode_

        A Path Node is a node that has children

        """
        return len(nodeRef.children) != 0
        
    
    def getChildNode(self, nodeRef, nodeName):
        """
        _hasChildNode_

        If the nodeRef provided has a child named
        nodeName, (or many) return the last one matched.

        If the node does not have a child named nodeName,
        return None
        """
        matchedChild = None
        for child in nodeRef.children:
            if child.name == nodeName:
                matchedChild = child
        
        return matchedChild
    
    def toDictionary(self):
        """
        _toDictionary_

        Generate a nested dictionary structure based on the content of this
        object

        """
        result = makeDict(self)
        return result

    def populate(self, dictionary):
        """
        _populate_

        Fill this instance with a structure based on the nested dictionary
        provided. 
        
        """
        for item in self.children:
            del item

        for item in dictToNode(dictionary):
            self.addNode(item)
        return 
        

def makeDict(node):
    """
    _makeDict_

    Convert the RunResComponent into a tree of nested dictionaries.
    Values are stored as lists

    """
    result = {}
    for child in node.children:
        if not result.has_key(child.name):
            result[child.name] = None
        if child.attrs.get('Type', "Path") == 'Data':
            if result[child.name] == None:
                result[child.name] = []
            chardata = child.chardata.strip()
            if len(chardata) > 0 :
                result[child.name].append(child.chardata)
        else:
            result[child.name] = makeDict(child)
            
    return result


def dictToNode(dictRef):

    results = []
    for key, value in dictRef.items():
        if type(value) == type({}):
            keyNode = IMProvNode(str(key), None, Type = "Path")
            for item in dictToNode(value):
                keyNode.addNode(item)
            results.append(keyNode)
        if type(value) == type([]):
            for item in value:
                dataNode = IMProvNode(str(key), str(item), Type = "Data")
                results.append(dataNode)
    return results

            


        
    
if __name__ == '__main__':
    comp = RunResComponent()

    comp.addData("CMKIN/Output/Files", "file://$JOBAREA/CMKIN/CMKIN-out1.ntpl")
    comp.addData("CMKIN/Output/Files", "file://$JOBAREA/CMKIN/CMKIN-out2.ntpl")
    comp.addData("CMKIN/Output/Files", "file://$JOBAREA/CMKIN/CMKIN-out3.ntpl")
    comp.addData("CMKIN/NumEvents", "1000")
    comp.addData("CMKIN/RunNumber", "1")
    

    comp.addPath("cmsRun1/InputSource")
    comp.addPath("cmsRun1/OutputModules/module1")
    #comp.addData("cmsRun1/InputSource/MaxEvents", "runresdb:///CMKIN/NumEvents")
    
    
    #comp.addData("cmsRun1/OutputModules/module1/Catalog", "SimHitsCatalog1.xml")
    #comp.addData("cmsRun1/OutputModules/module1/LogicalName", "SimHits1.root")

    
    #comp.addData("cmsRun1/OutputModules/module2/Catalog", "SimHitsCatalog2.xml")
    #comp.addData("cmsRun1/OutputModules/module2/LogicalName", "SimHits2.root")
    
    #comp.addData("cmsRun1/OutputModules/module3/Catalog", "SimHitsCatalog3.xml")
    #comp.addData("cmsRun1/OutputModules/module3/LogicalName", "SimHits3.root")
    
    
    
    #comp.addData("cmsRun2/InputSource/Catalog", "runresdb:///cmsRun1/OutputModules/module1/Catalog")
    #comp.addData("cmsRun2/InputSource/MaxEvents", "runresdb:///CMKIN/NumEvents")

    #comp.addData("cmsRun2/OutputModules/module1/Catalog", "RecHitsCatalog1.xml")
    #comp.addData("cmsRun2/OutputModules/module1/LogicalName", "RecHits1.root")
    
    
    

    
    

    d = comp.toDictionary()

    
    #d = makeDict(comp)
    
    #print d
    
    print d['CMKIN']['Output']['Files']

    print d['cmsRun1']['OutputModules']['module1']

    #print d['cmsRun1']['OutputModules']['module1']['Catalog']
    
    #print fromDict(d)

    #d2 = makeDict(fromDict(d))
    #print d2['CMKIN']['Output']['Files']
    
    comp2 = RunResComponent()
    comp2.populate(comp.toDictionary())

    #print comp
    #print comp2


                
