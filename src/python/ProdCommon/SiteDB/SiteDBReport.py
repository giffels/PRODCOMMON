#! /usr/bin/env python

# Original author: Brian Bockelman

import signal
import re
import os
import time
import urllib
import urllib2
from xml.dom.minidom import parse

class AlarmClock(Exception):
    """
    Exception indicating that the alarm clock went off
    """



class SiteDBReport:
    """
    An module-internal object which retrieves reports from SiteDB and
    attempts to cache the data
    """

    # The expire is the age of the cache which will trigger a new query to
    # SiteDB.  The force_expire is the age of the cache where the contents of
    # the cache will be ignored, even if the SiteDB query fails.
    expire        = 1  # In days
    force_expire  = 5  # In days
    alarm_timeout = 15 # In seconds

    # SiteDB URL
    sitedb_url = 'https://cmsweb.cern.ch/sitedb/sitedb/reports/showXMLReport'


    def __init__(self):
        self.ce_results = []
        self.se_results = []


    def write_cache(self, name, contents):
        """
        Write the contents of a report into the cache in the ~/.sitedb directory
        """
        filename = '$HOME/.sitedb/%s.cache' % name
        filename = os.path.expandvars(filename)
        dirname  = '$HOME/.sitedb/'
        dirname  = os.path.expandvars(dirname)

        if not os.path.isdir(dirname):
            try:
                os.mkdir(dirname)
            except:
                pass

        try:
            fd = open(filename, 'w')
            fd.write(contents)
        except:
            pass


    def check_cache(self, name):
        """
        Check the cache for a report named `name`

        If the file has not been modified in self.expire days, the `fresh`
        variable will return True.  If the file has been modified within the
        last self.force_expire days, the `fresh` variable will return False.
        If the last modification was greater than self.force_expire days ago,
        then return nothing.

        @param name: The name of the report to look for
        @returns: A
        @rtype: (bool, file descriptor)
        """
        filename = '$HOME/.sitedb/%s.cache' % name
        filename = os.path.expandvars(filename)
        # Check the status of the cache; catch common harmless problems
        try:
           si =  os.stat(filename)
        except OSError, e:
            if e.errno == 2: # No such file or directory; cache miss
                return False, None
            elif e.errno == 13: # Permission denied
                return False, None
            elif e.errno == 20: # .sitedb exists, not a directory
                return False, None
            else:
                raise

        # Look at the last-modified time; if the age is too old, return
        try:
            mtime = si.st_mtime
        except AttributeError:
            mtime = si[8]
        if time.time()-mtime > self.expire*86400:
            fresh = False
        elif time.time()-mtime > self.force_expire*86400:
            return False, None
        else:
            fresh = True

        # Read contents of the cache, return the freshness
        fd = open(filename, 'r')
        return fresh, fd


    def load_siteDB(self, query):
        """
        Load the contents of `query` from SiteDB

        @param query: Name of the SiteDB query.
        @returns: File-descriptor like object.
        """
        params = {'reportid': query}
        params = urllib.urlencode(params)
        fd = urllib2.urlopen(self.sitedb_url, params)
        return fd


    def load_report(self, report):
        """
        Load the SiteDB report and return the DOM contents.

        Great care is taken to make this resilient, including timeouts and
        fallback to the contents of the cache.

        @param report: The name of the SiteDB report
        @returns: The DOM object representing the contents of the report
        """

        # Start off by setting the alarm clock to timeout faulty operations.
        def interrupt_op(*args):
            raise AlarmClock()
        signal.signal(signal.SIGALRM, interrupt_op)
        try:
            # Check the contents of the local cache; only return here if they
            # are fresh and parse cleanly.
            try:
                signal.alarm(self.alarm_timeout)
                fresh, results = self.check_cache(report)
                if results:
                    try:
                        results = parse(results)
                        if fresh:
                            return results
                    except:
                        results = None
            except AlarmClock:
                fresh = False
                results = None
            # Check SiteDB for the report.  If there's a problem, no urlresults
            # are available.  SiteDB results are save to the cache.
            try:
                signal.alarm(self.alarm_timeout)
                urlresults = self.load_siteDB(report)
                try:
                    urlresults = parse(urlresults)
                except:
                    urlresults = None
            except (AlarmClock, urllib2.URLError):
                fresh, urlresults = False, None
        finally:
            # Restore alarm handlers
            signal.alarm(0)
            signal.signal(signal.SIGALRM, signal.SIG_DFL)
        # Default to the SiteDB results if available; if there was a problem
        # with SiteDB, the cached results will return.
        if urlresults:
            try:
                self.write_cache(report, urlresults.toprettyxml())
            except:
                pass
            return urlresults
        if results:
            return results

        # Neither SiteDB nor cached results; raise an Exception.
        raise Exception("Unable to get CMS info from SiteDB.")


    def parse_report(self, dom, kind='ce'):
        """
        Parse the contents of the SiteDB report.

        The SiteDB row results are expected to have three columns - sitename,
        the PhEDEx node, and a column given by `kind`; usually "ce" or "se".
        See the SiteDB results to understand what this is parsing.

        A list of tuples is returned; none of the entries in the tuples are
        guaranteed to be unique.

        @param dom: DOM object containing the contents of the SiteDB report
        @keyword kind: The kind of report - "ce" or "se"
        @returns: A list of 3-tuples: (sitename, phedex node, ce/se name).
        """
        result = dom.getElementsByTagName('result')
        if not result:
            return []
        result = result[0]
        results = []
        items = result.getElementsByTagName('item')
        for item in items:
            try:
                name = str(item.getElementsByTagName('name')[0].firstChild.data)
            except:
                name = None
            try:
                node = str(item.getElementsByTagName('node')[0].firstChild.data)
            except:
                node = None
            try:
                ce = str(item.getElementsByTagName(kind)[0].firstChild.data)
            except:
                ce = None
            results.append((name.strip(), node.strip(), ce.strip()))
        return results


    def load_SE(self):
        """
        Load the contents of the se_node_map.ini report from SiteDB.

        For the returned format, see the parse_report method documentation.
        """
        if not self.se_results:
            dom = self.load_report('se_node_map.ini')
            self.se_results = self.parse_report(dom, kind='se')
        return self.se_results


    def load_CE(self):
        """
        Load the contents of the ce_node_map.ini report from SiteDB.

        For the returned format, see the parse_report method documentation.
        """
        if not self.ce_results:
            dom = self.load_report('ce_node_map.ini')
            self.ce_results = self.parse_report(dom, kind='ce')
        return self.ce_results
