#!/usr/bin/env python
import random
import unittest
from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport



class FileSortingTest(unittest.TestCase):
    """
    Test cases for sorting files within job reports

    """

    def testA(self):
        """
        test single file use cases

        standalone file
        standalone file with external parent

        """

        report = FwkJobReport()
        f = report.newFile()
        f['LFN'] = "LFN1"
        f['PFN'] = "file:LFN1"
        f['ModuleLabel'] = "MODULE1"

        try:
            sorted = report.sortFiles()
        except Exception, ex:
            msg = "Exception calling sortFiles:\n"
            msg += str(ex)
            self.fail(msg)
        self.assertEqual(len(sorted), 1)

        report = FwkJobReport()
        f = report.newFile()
        f['LFN'] = "LFN1"
        f['PFN'] = "file:LFN1"
        f['ModuleLabel'] = "MODULE1"
        f.addInputFile("file:INPUT", "INPUT")

        try:
            sorted = report.sortFiles()
        except Exception, ex:
            msg = "Exception calling sortFiles:\n"
            msg += str(ex)
            self.fail(msg)
        self.assertEqual(len(sorted), 1)

    def testB(self):
        """
        test multiple unrelated files

        multiple standalone files with no parents
        multiple standalone files with same external parent
        multiple standalone files with different external parents

        """
        report = FwkJobReport()
        f1 = report.newFile()
        f1['LFN'] = "LFN1"
        f1['PFN'] = "file:LFN1"
        f1['ModuleLabel'] = "MODULE1"

        f2 = report.newFile()
        f2['LFN'] = "LFN2"
        f2['PFN'] = "file:LFN2"
        f2['ModuleLabel'] = "MODULE2"

        f3 = report.newFile()
        f3['LFN'] = "LFN3"
        f3['PFN'] = "file:LFN3"
        f3['ModuleLabel'] = "MODULE3"

        random.shuffle(report.files)

        try:
            sorted = report.sortFiles()
        except Exception, ex:
            msg = "Exception calling sortFiles:\n"
            msg += str(ex)
            self.fail(msg)
        self.assertEqual(len(sorted), 3)
        self.assertEqual(sorted[0]['LFN'], "LFN1")
        self.assertEqual(sorted[1]['LFN'], "LFN2")
        self.assertEqual(sorted[2]['LFN'], "LFN3")


        report = FwkJobReport()
        f1 = report.newFile()
        f1['LFN'] = "LFN1"
        f1['PFN'] = "file:LFN1"
        f1['ModuleLabel'] = "MODULE1"
        f1.addInputFile("file:INPUT", "INPUT")

        f2 = report.newFile()
        f2['LFN'] = "LFN2"
        f2['PFN'] = "file:LFN2"
        f2['ModuleLabel'] = "MODULE2"
        f2.addInputFile("file:INPUT", "INPUT")

        f3 = report.newFile()
        f3['LFN'] = "LFN3"
        f3['PFN'] = "file:LFN3"
        f3['ModuleLabel'] = "MODULE3"
        f3.addInputFile("file:INPUT", "INPUT")


        random.shuffle(report.files)

        try:
            sorted = report.sortFiles()
        except Exception, ex:
            msg = "Exception calling sortFiles:\n"
            msg += str(ex)
            self.fail(msg)
        self.assertEqual(len(sorted), 3)
        self.assertEqual(sorted[0]['LFN'], "LFN1")
        self.assertEqual(sorted[1]['LFN'], "LFN2")
        self.assertEqual(sorted[2]['LFN'], "LFN3")


        report = FwkJobReport()
        f1 = report.newFile()
        f1['LFN'] = "LFN1"
        f1['PFN'] = "file:LFN1"
        f1['ModuleLabel'] = "MODULE1"
        f1.addInputFile("file:INPUT1", "INPUT1")

        f2 = report.newFile()
        f2['LFN'] = "LFN2"
        f2['PFN'] = "file:LFN2"
        f2['ModuleLabel'] = "MODULE2"
        f2.addInputFile("file:INPUT2", "INPUT2")

        f3 = report.newFile()
        f3['LFN'] = "LFN3"
        f3['PFN'] = "file:LFN3"
        f3['ModuleLabel'] = "MODULE3"
        f3.addInputFile("file:INPUT2", "INPUT2")


        random.shuffle(report.files)

        try:
            sorted = report.sortFiles()
        except Exception, ex:
            msg = "Exception calling sortFiles:\n"
            msg += str(ex)
            self.fail(msg)
        self.assertEqual(len(sorted), 3)
        self.assertEqual(sorted[0]['LFN'], "LFN1")
        self.assertEqual(sorted[1]['LFN'], "LFN2")
        self.assertEqual(sorted[2]['LFN'], "LFN3")



    def testC(self):
        """
        test linear parentage

        file1 -> file2 -> file3 -> file4 -> file5

        """
        report = FwkJobReport()

        for i in range(0,5):
            f = report.newFile()
            f['LFN'] = "LFN%s" % i
            f['PFN'] = "file:LFN%s" % i
            f['ModuleLabel'] = "MODULE%s" % i
            prev = i-1
            if i > 0:
                f.addInputFile("file:LFN%s" % prev,
                               "LFN%s" % prev)
            else:
                f.addInputFile("file:INPUT", "INPUT")

        random.shuffle(report.files)
        try:
            sorted = report.sortFiles()
        except Exception, ex:
            msg = "Exception calling sortFiles:\n"
            msg += str(ex)
            self.fail(msg)

        for i in range(0, 5):
            self.assertEqual(sorted[i]['LFN'], 'LFN%s' % i)

    def testD(self):
        """
        test multiple sets of linear parentage

        file1 -> file2 -> file3 -> file4 -> file5
        file6 -> ... file10
        file11 -> ... file15

        """
        report = FwkJobReport()

        for i in range(0,5):
            f = report.newFile()
            f['LFN'] = "LFN%s" % i
            f['PFN'] = "file:LFN%s" % i
            f['ModuleLabel'] = "MODULE%s" % i
            prev = i-1
            if i > 0:
                f.addInputFile("file:LFN%s" % prev,
                               "LFN%s" % prev)
            else:
                f.addInputFile("file:INPUT1", "INPUT1")

        for i in range(5, 10):
            f = report.newFile()
            f['LFN'] = "LFN%s" % i
            f['PFN'] = "file:LFN%s" % i
            f['ModuleLabel'] = "MODULE%s" % i
            prev = i-1
            if i > 5:
                f.addInputFile("file:LFN%s" % prev,
                               "LFN%s" % prev)
            else:
                f.addInputFile("file:INPUT2", "INPUT2")

        for i in range(10, 15):
            f = report.newFile()
            f['LFN'] = "LFN%s" % i
            f['PFN'] = "file:LFN%s" % i
            f['ModuleLabel'] = "MODULE%s" % i
            prev = i-1
            if i > 10:
                f.addInputFile("file:LFN%s" % prev,
                               "LFN%s" % prev)
            else:
                f.addInputFile("file:INPUT3", "INPUT3")

        random.shuffle(report.files)
        try:
            sorted = report.sortFiles()
        except Exception, ex:
            msg = "Exception calling sortFiles:\n"
            msg += str(ex)
            self.fail(msg)


        self.assertEqual(len(sorted), 15)

        lfn0pos = None
        lfn5pos = None
        lfn10pos = None
        for i in range(0, len(sorted)):
            if sorted[i]['LFN'] == "LFN0": lfn0pos = i
            if sorted[i]['LFN'] == "LFN5": lfn5pos = i
            if sorted[i]['LFN'] == "LFN10": lfn10pos = i

        self.assertEqual(sorted[lfn0pos]['LFN'] , "LFN0")
        self.assertEqual(sorted[lfn0pos+1]['LFN'] , "LFN1")
        self.assertEqual(sorted[lfn0pos+2]['LFN'] , "LFN2")
        self.assertEqual(sorted[lfn0pos+3]['LFN'] , "LFN3")
        self.assertEqual(sorted[lfn0pos+4]['LFN'] , "LFN4")

        self.assertEqual(sorted[lfn5pos]['LFN'] , "LFN5")
        self.assertEqual(sorted[lfn5pos+1]['LFN'] , "LFN6")
        self.assertEqual(sorted[lfn5pos+2]['LFN'] , "LFN7")
        self.assertEqual(sorted[lfn5pos+3]['LFN'] , "LFN8")
        self.assertEqual(sorted[lfn5pos+4]['LFN'] , "LFN9")

        self.assertEqual(sorted[lfn10pos]['LFN'] , "LFN10")
        self.assertEqual(sorted[lfn10pos+1]['LFN'] , "LFN11")
        self.assertEqual(sorted[lfn10pos+2]['LFN'] , "LFN12")
        self.assertEqual(sorted[lfn10pos+3]['LFN'] , "LFN13")
        self.assertEqual(sorted[lfn10pos+4]['LFN'] , "LFN14")


    def testE(self):
        """
        test tree like dependencies

        file1 -> file2 -> file3,file4
               -> file5 -> file6,file7
                -> file8 -> file9,file10

        """
        report = FwkJobReport()

        file1 = report.newFile()
        file1['LFN'] = "LFN1"
        file1['PFN'] = "file:PFN1"

        file2 = report.newFile()
        file2['LFN'] = "LFN2"
        file2['PFN'] = "file:PFN2"
        file2.addInputFile("file:LFN1", "LFN1")


        file3 = report.newFile()
        file3['LFN'] = "LFN3"
        file3['PFN'] = "file:PFN3"
        file3.addInputFile("file:LFN1", "LFN1")

        file4 = report.newFile()
        file4['LFN'] = "LFN4"
        file4['PFN'] = "file:PFN4"
        file4.addInputFile("file:LFN2", "LFN2")

        file5 = report.newFile()
        file5['LFN'] = "LFN5"
        file5['PFN'] = "file:PFN5"
        file5.addInputFile("file:LFN2", "LFN2")

        file6 = report.newFile()
        file6['LFN'] = "LFN6"
        file6['PFN'] = "file:PFN6"
        file6.addInputFile("file:LFN3", "LFN3")

        file7 = report.newFile()
        file7['LFN'] = "LFN7"
        file7['PFN'] = "file:PFN7"
        file7.addInputFile("file:LFN3", "LFN3")

        file8 = report.newFile()
        file8['LFN'] = "LFN8"
        file8['PFN'] = "file:PFN8"
        file8.addInputFile("file:LFN3", "LFN3")

        file9 = report.newFile()
        file9['LFN'] = "LFN9"
        file9['PFN'] = "file:PFN9"
        file9.addInputFile("file:LFN4", "LFN4")

        file10 = report.newFile()
        file10['LFN'] = "LFN10"
        file10['PFN'] = "file:PFN10"
        file10.addInputFile("file:LFN4", "LFN4")

        random.shuffle(report.files)
        try:
            sorted = report.sortFiles()
        except Exception, ex:
            msg = "Exception calling sortFiles:\n"
            msg += str(ex)
            self.fail(msg)


        positions = {}
        for i in range(0, len(sorted)):
            positions[sorted[i]['LFN'] ] = i

        self.failUnless(positions['LFN2'] > positions['LFN1'])
        self.failUnless(positions['LFN3'] > positions['LFN1'])

        self.failUnless(positions['LFN4'] > positions['LFN2'])
        self.failUnless(positions['LFN5'] > positions['LFN2'])

        self.failUnless(positions['LFN6'] > positions['LFN3'])
        self.failUnless(positions['LFN7'] > positions['LFN3'])
        self.failUnless(positions['LFN8'] > positions['LFN3'])

        self.failUnless(positions['LFN9'] > positions['LFN4'])
        self.failUnless(positions['LFN10'] > positions['LFN4'])





if __name__ == '__main__':
    unittest.main()





##for i in range(0,10):
##    f = report1.newFile()
##    f['LFN'] = "LFN%s" % i
##    f['PFN'] = "file:LFN%s" % i
##    f['ModuleLabel'] = "MODULE%s" % i
##    prev = i-1
##    if i > 0:
##        f.addInputFile("file:LFN%s" % prev,
##                       "LFN%s" % prev)
##    else:
##        f.addInputFile("file:INPUT", "INPUT")






##random.shuffle(report1.files)

##print report1.sortFiles()

##print rep.sortFiles()

