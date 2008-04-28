from SElement import SElement
from SBinterface import *
from Exceptions import TransferException, OperationException

protocols = ["srmv1", "srmv2", "local", "gridftp", "rfio"]
proxy = ""

def tester(action, source, dest):
    try:
        action.copy( source, dest )
        print action.checkExists( source )
        print action.getPermission( source )
        print action.getList( source )
        print action.getSize( source )
        print action.getDirSpace( source )
        print action.getGlobalSpace( source )
        #action.createDir ( source )
        print action.getTurl ( source )
        action.move( source, dest )
        action.delete( dest )
    except Exception, ex:
        print str(ex)
        import traceback
        print str(traceback.format_exc())
        for errore in ex.detail:
            print errore
        print ex.output

### local ###
se1 = SElement("localhost", "local")
se2 = SElement("localhost", "local")
# local - local
action = SBinterface(se1, se2)
tester(action, '/afs/cern.ch/user/m/mcinquil/prp', '/afs/cern.ch/user/m/mcinquil/prp2')

### gridftp ###
se1 = SElement("lxb0704.cern.ch", "gridftp")
se2 = SElement("localhost", "local")
se3 = SElement("crabdev1.cern.ch", "gridftp")
# gridftp - local
action = SBinterface(se1, se2)
tester(action, '/datacms/mcinquil_prova_mail_fe8762be-e474-4422-8e1f-274c77530b00/out_files_1.tgz', '/afs/cern.ch/user/m/mcinquil/pippo')
# gridftp - gridftp
action = SBinterface(se3, se1)
tester(action, '/datacms/mcinquil_prova_mail_fe8762be-e474-4422-8e1f-274c77530b00/out_files_2.tgz', '/flatfiles/cms/prova')
# local - gridftp
action = SBinterface(se2, se1)
tester(action, '/afs/cern.ch/user/m/mcinquil/pippo', '/datacms/mcinquil_prova_mail_fe8762be-e474-4422-8e1f-274c77530b00/prova')

### srmv1 ###
se1 = SElement("srm.cern.ch", "srmv1")
se2 = SElement("localhost", "local")
se3 = SElement("gridse3.pg.infn.it", "srmv1")
# srm - local
action = SBinterface(se1, se2)
tester(action, '/castor/cern.ch/user/m/mcinquil/test33', '/afs/cern.ch/user/m/mcinquil/poppi')
# srm - srm
action = SBinterface(se1, se3)
tester(action, '/castor/cern.ch/user/m/mcinquil/test.py', '/pnfs/pg.infn.it/data/cms/store/user/pippo')
# local - srm
action = SBinterface(se2, se1)
tester(action, '/afs/cern.ch/user/m/mcinquil/poppi', '/castor/cern.ch/user/m/mcinquil/provaprova')

### srmv2 ###
# srm - local
# srm - srm
# local - srm

### rfio ###
se1 = SElement("vocms35.cern.ch","rfio")
se2 = SElement("localhost", "local")
# rfio - local
action = SBinterface(se1, se2)
tester(action, '/data/mcinquil/WM_fut.fig', '/afs/cern.ch/user/m/mcinquil/pipa')
# local - rfio
action = SBinterface(se2, se1)
tester(action, '/afs/cern.ch/user/m/mcinquil/provala', '/data/mcinquil/pippo')
