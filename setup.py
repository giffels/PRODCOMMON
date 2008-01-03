#!/usr/bin/env python
from distutils.core import setup

setup (name='prodcommon',
       version='1.0',
       package_dir={'IMProv': 'src/python/IMProv',
                    'RunRes' : 'src/python/RunRes',
                    'ProdCommon': 'src/python/ProdCommon'},
       packages=['IMProv',
                 'RunRes',
                 'ProdCommon',
		 'ProdCommon.CMSConfigTools',
                 'ProdCommon.CMSConfigTools.ConfigAPI',
                 'ProdCommon.Core',
                 'ProdCommon.CmsGen',
                 'ProdCommon.Database',
                 'ProdCommon.DataMgmt',
                 'ProdCommon.DataMgmt.DBS',
                 'ProdCommon.DataMgmt.PhEDEx',
                 'ProdCommon.DataMgmt.JobSplit',
                 'ProdCommon.DataMgmt.Pileup',
                 'ProdCommon.FwkJobRep',
                 'ProdCommon.JobFactory',
		 'ProdCommon.MCPayloads',
                 'ProdCommon.WebServiceClient',
                 'ProdCommon.WebServices',
                 'ProdCommon.ThreadTools'],)

