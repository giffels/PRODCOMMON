#!/usr/bin/env python
"""
_Utilities_

Util objects for working with job report objects

"""


class SearchOp:
    """
    _SearchOp_

    Search operator to act on a Tree
    """
    def __init__(self, lfn):
        self.target = lfn
        self.result = None

    def __call__(self, node):
        if node.lfn == self.target:
            self.result = node

class OrderOp:
    """
    _OrderOp_

    Ordering operator to act on a Tree

    """
    def __init__(self):
        self.result = []

    def __call__(self, node):
        self.result.append(node.data)



class FileNode:
    """
    _FileNode_

    Tree Node container class for a FileInfo object that allows
    building a tree of FileInfo objects based on parentage information

    """
    def __init__(self, fileInfo):
        self.lfn = fileInfo['LFN']
        self.data = fileInfo
        self.children = {}

    def addChild(self, fileInfo):
        """
        _addChild_

        Add a child file to this node
        """
        self.children[fileInfo['LFN']] = FileNode(fileInfo)

    def traverse(self,operator):
        """
        _traverse_

        Recursive descent through children
        Call operator on self and then pass down
        """
        operator(self)
        for c in self.children.values():
            c.traverse(operator)


    def stringMe(self, indent = 0):
        """
        _stringMe_

        Recursive print util that indents children to aid debugging
        """
        padding = ""
        for x in range(0, indent):
            padding += " "
        msg = "%sNode : %s\n" % (padding, self.lfn)
        for c in self.children.values():
            msg += "%s%s" % (padding, c.stringMe(indent+1))
        return msg

class Tree:
    """
    _Tree_

    Top level Tree object, maintains a list of FileNode roots
    and allows them all to be queries and sorted in a single operation

    """

    def __init__(self):
        self.roots = {}

    def addRoot(self, fileInfo):
        """
        _addRoot_

        Add a new Root node to the Tree

        """
        self.roots[fileInfo['LFN']] = FileNode(fileInfo)


    def search(self, lfn):
        """
        _search_

        Recursive search through all root trees for the LFN
        requested, returning the node matching that LFN

        """
        for root in self.roots.values():
            searcher = SearchOp(lfn)
            root.traverse(searcher)
            if searcher.result != None:
                return searcher.result

        return None

    def sort(self):
        """
        _sort_

        Collapse tree in order based on parentage tree for each root node

        """
        sorter = OrderOp()
        for root in self.roots.values():
            root.traverse(sorter)
        return sorter.result


    def __str__(self):
        """
        format method to aid debugging
        """
        msg = ""
        for root in self.roots.values():
            msg += "%s\n" % root.stringMe()

        return msg


def processList(tree, *input):
    """
    _processList_

    Operator that reduces the input list for each node that it
    can add to the tree.
    Any nodes that cannot be added in this pass are returned as
    a list of remainders.

    """
    remainders = []
    for r in input:
        searchFor = r['MatchParent']
        searchResult = tree.search(searchFor)
        if searchResult != None:
            searchResult.addChild(r)
            continue
        else:
            remainders.append(r)
    return remainders



def sortFiles(report):
    """
    _sortFiles_

    Sort the output files in the report based on parentage information
    to ensure that any files that depend on each other are processed in
    the correct order.

    """
    tree = Tree()
    remainders = []
    allLFNs = [ x['LFN'] for x in report.files ]
    allParents = []
    [ allParents.extend(x.parentLFNs()) for x in report.files ]
    externalParents = set(allParents).difference(set(allLFNs))

    #  //
    # // firstly we build the tree roots from all the files that
    #//  dont have parents within the list of files we are dealing with
    for f in report.files:
        parents = set(f.parentLFNs())
        # strip out external parents
        parents = list(parents.difference(externalParents))
        if len(parents) == 0:
            # No parents, top of tree
            tree.addRoot(f)
            continue
        if len(parents) == 1:
            f['MatchParent'] = parents[0]
            remainders.append(f)
            continue
        if len(parents) > 1:
            # Multiple parents ==> PANIC!
            raise RuntimeError, "File has too many parents"

    #  //
    # // Now we have pruned out the roots, we process the
    #//  dependencies for each node, we do this recursively
    #  //and make sure the list keeps decreasing
    # //
    #//
    recursionCheck = 0
    remainderLen = len(remainders)
    while len(remainders) > 0:
        remainders = processList(tree, *remainders)
        if len(remainders) == remainderLen:
            recursionCheck += 1
        remainderLen = len(remainders)

        if recursionCheck > 10:
            #  //
            # // further reduction may not be possible
            #//  for now, blow an exception
            msg = "Parentage sorting appears to be stuck in a loop"
            raise RuntimeError, msg

    #  //
    # // Now we have built the tree, we can traverse it in order
    #//  to rebuild the ordered list of parents

    return tree.sort()





