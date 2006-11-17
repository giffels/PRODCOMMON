#!/usr/bin/env python
from distutils.core import setup

setup (name='prodcommon',
       version='1.0',
       package_dir={'IMProv': 'src/python/IMProv',
                    'ProdCommon': 'src/python/ProdCommon'},
       packages=['IMProv', 'ProdCommon'],)

