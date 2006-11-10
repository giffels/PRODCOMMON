from ProdCommon.Database import Session
from ProdCommon.Core.Initialize import configuration as Cconfiguration

import logging

def setProdCommonDB():
   Session.set_database(Cconfiguration.get('DB'))
   Session.connect('ProdCommon')
   Session.set_session('ProdCommon')
   Session.start_transaction()


def finish():
   Session.commit_all()
   Session.close_all()


def fail():
   Session.rollback_all()
   Session.close_all()

