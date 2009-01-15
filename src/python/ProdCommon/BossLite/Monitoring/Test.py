#!/usr/bin/env python

from ProdCommon.BossLite.API.BossLiteDB import  BossLiteDB
from ProdCommon.BossLite.Monitoring.Monitoring import Monitoring
from ProdAgentDB.Config import defaultConfig as dbConfig

bossSession = BossLiteDB( 'MySQL', dbConfig )

dbClass = Monitoring( bossSession )

print 'Exit codes per site'

print dbClass.exitCodes()

print ''
print 'Active status'
print dbClass.activeStatus( 'days', 365, 1228384986 )

print ''
print 'Destination'
print dbClass.destination( 'hours', 12 )
