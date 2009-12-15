#! /usr/bin/env python
"""
The CmsSiteMapper module provides information from SiteDB and assists in the
mappings to and from CMS site names and some resource name (both SEs and CEs).

Elaborate caching mechanisms are employed to keep the load off SiteDB and
improve response time.
"""

import types
import fnmatch

# Create a global instance of the SiteDBInfo object
from SiteDBReport import SiteDBReport
siteDB = SiteDBReport()


def match_list(names, match_list):
    """
    Filter a list of names against a comma-separated list of expressions.

    This uses the `match` function to do the heavy lifting

    @param names: A list of input names to filter
    @type names: list
    @param match_list: A comma-separated list of expressions
    @type match_list: str
    @returns: A list, filtered from `names`, of all entries which match an
       expression in match_list
    @rtype: list
    """
    results = []
    if isinstance(match_list, types.StringType):
	match_list = match_list.split(',')
    for expr in match_list:
	expr = expr.strip()
	matching = match(names, expr)
	if matching:
	    results.extend(matching)
	else:
	    results.append(expr)
    return results


def match(names, expr):
    """
    Return all the entries in `names` which match `expr`

    First, try to apply wildcard-based filters, then look at substrings,
    then interpret expr as a regex.

    @param names: An input list of strings to match
    @param expr: A string expression to use for matching
    @returns: All entries in the list `names` which match `expr`
    """
    results = fnmatch.filter(names, expr)
    results.extend([i for i in names if i.find(expr) >= 0])
    try:
	my_re = re.compile(expr)
    except:
	my_re = None
    if not my_re:
	return results
    results.extend([i for i in names if my_re.search(i)])
    return results



class CmsResourceMap(dict):
    """
    A dictionary-like object which maps from the CMS name to some resource;
    this is meant to be sub-classed; one for CEs, one for SEs.
    """

    def __init__(self):
	self._loaded = False
	self._tuples = []
	self._map = {}
	self._cmsnames = []


    def load_tuples(self):
	"""
	Internal method to load the tuples; return a value like SiteDBInfo's
	parse_report method.

	This is an abtract method; overload it.
	"""
	raise NotImplementedError()


    def load(self):
	"""
	Load the contents of a SiteDB report.

	This uses self.load_tuples to load a list of tuples from the SiteDBInfo
	object, then does some preliminary parsing of the results.

	All methods which need the preliminary parsing already call this
	method, so there's no need to call it directly.
	"""
	if not self._loaded:
	    self._loaded = True
	    self._tuples = self.load_tuples()
	    for tuple in self._tuples:
		name, node, resource = tuple
		self._map[name] = self._map.get(name, [])
		self._map[node] = self._map.get(node, [])
		self._map[name].append(resource)
		self._map[node].append(resource)
	    self._cmsnames = [i[0] for i in self._tuples]
	    self._cmsnames.extend([i[1] for i in self._tuples])


    def __getitem__(self, cmsname):
	if not self._loaded:
	    self.load()
	return self._map.get(cmsname, [cmsname])[0]


    def match(self, cmsname):
	"""
	Given a string which contains a comma-separated list of expressions
	which match some CMS site name, return a list of all the resources
	that match at least one expression

	If no CMS site matches one of the expressions, return the expression in
	the list (this behavior is for backward compatibility, in case if the
	expression is actually a CMS resource expression, not CMS.

	Note: This method uses match_list to do the heavy liftin.

	@param cmsname: A string containing a comma-separated list of
	    expressions.
	"""
	if not self._loaded:
	    self.load()
	matching_names = match_list(self._cmsnames, cmsname)
	results = []
	for i in matching_names:
	    if i in self._map:
		results.extend(self._map[i])
	    else:
		results.append(i)
	return list(set(results))



class CmsSEMap(CmsResourceMap):
    """
    A dictionary-like object which maps from the CMS name to the SE name.
    After the object is created, run the `load` method to perform the lookups;
    if this is not run, it will be done at the first lookup
    """

    def load_tuples(self):
	"""
	Return the results from SiteDBInfo::load_SE.

	Internal method; the bulk of the work is done by CmsResourceMap
	"""
	return siteDB.load_SE()



class CmsCEMap(CmsResourceMap):
    """
    A dictionary-like object which maps from the CMS name to the CE name.
    After the object is created, run the `load` method to perform the lookups;
    if this is not run, it will be done at the first lookup
    """

    def load_tuples(self):
	"""
	Return the results from SiteDBInfo::load_CE.

	Internal method; the bulk of the work is done by CmsResourceMap
	"""
	return siteDB.load_CE()



class ResourceCmsMap(dict):
    """
    This dictionary-like class is the base class for objects which map some
    resource name to a CMS site name.
    """

    def __init__(self):
	self._loaded = False
	self._tuples = []
	self._map = {}


    def load_tuples(self):
	"""
	Return the results from a SiteDB report as provided by SiteDBInfo; this
	needs to be implemented by the subclass.  See SiteDBInfo::parse_report
	documentation for the expected result format
	"""
	raise NotImplementedError()


    def load(self):
	"""
	Load up the SiteDB information and do some preliminary parsing; this
	is automatically called by functions which need it, so there's no
	need to call it directly.
	"""
	if not self._loaded:
	    self._loaded = True
	    self._tuples = self.load_tuples()
	    for tuple in self._tuples:
		name, node, resource = tuple
		self._map[resource] = node


    def __getitem__(self, resource):
	if not self._loaded:
	    self.load()
	return self._map.get(resource, resource)



class CECmsMap(ResourceCmsMap):
    """
    A dictionary-like class which maps from CE name to CMS name.

    Most of the work is done by the ResourceCmsMap.
    """

    def load_tuples(self):
	"""
	Return the results from SiteDBInfo::load_CE.

	Internal method; the bulk of the work is done by ResourceCmsMap.
	"""
	return siteDB.load_CE()



class SECmsMap(ResourceCmsMap):
    """
    A dictionary-like class which maps from CE name to CMS name.

    Most of the work is done by the ResourceCmsMap.
    """

    def load_tuples(self):
	"""
	Return the results from SiteDBInfo::load_SE.

	Internal method; the bulk of the work is done by ResourceCmsMap.
	"""
	return siteDB.load_SE()



if __name__ == '__main__':
    ce_cms = CECmsMap()
    se_cms = SECmsMap()
    cms_ce = CmsCEMap()
    cms_se = CmsSEMap()
    assert ce_cms['blah'] == 'blah'
    assert ce_cms['red.unl.edu'] == 'T2_US_Nebraska'
    assert se_cms['srm.unl.edu'] == 'T2_US_Nebraska'
    assert cms_ce['T2_US_Nebraska'] == 'red.unl.edu'
    assert cms_se['T2_US_Nebraska'] == 'srm.unl.edu'
    print "All sites matching 'T0_*,T1_*':"
    print ', '.join(cms_se.match('T0_*,T1_*'))
