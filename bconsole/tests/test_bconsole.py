
import unittest
import logging
import socket
import re
from datetime import datetime
from unittest.mock import patch
from struct import pack, unpack
from bconsole.bconsole import BSocket, BConsole, BSocketWallet, JobStatus

#logging.basicConfig(filename='',level=logging.DEBUG)

TEST_USER_AGENT = None
DIR_TEST_PASSWORD = 'dirpassword12345'
TEST_JOBID = '5'
TEST_VERSION_ANSWER = '1000 OK: 102 dev-dir Version: 7.4.7 (16 March 2017)\n'
TEST_VERSION = '7.4.7'
TEST_JOB_STATUS = {'jobid': 5, 'name': 'RestoreJob', 'jobstatus': 'f', 'starttime': datetime(2018, 5, 5, 8, 13, 7), 'type': 'R', 'jobbytes': 843432234, 'level': 'F', 'jobfiles': 5}

CMD_RESTORE0_OUT = b'''
Automatically selected Catalog: DefaultCatalog
Using Catalog "DefaultCatalog"

First you select one or more JobIds that contain files
to be restored. You will be presented several methods
of specifying the JobIds. Then you will be allowed to
select which files from those JobIds are to be restored.

To select the JobIds, you have the following choices:
     1: List last 20 Jobs run
     2: List Jobs where a given File is saved
     3: Enter list of comma separated JobIds to select
     4: Enter SQL list command
     5: Select the most recent backup for a client
     6: Select backup for a client before a specified time
     7: Enter a list of files to restore
     8: Enter a list of files to restore before a specified time
     9: Find the JobIds of the most recent backup for a client
     10: Find the JobIds for a backup for a client before a specified time
     11: Enter a list of directories to restore for found JobIds
     12: Select full restore to a specified Job date
     13: Cancel
Select item:  (1-13):
'''

CMD_RESTORE1_OUT = b'''
Automatically selected FileSet: TestClient1Fileset
+-------+-------+----------+------------+---------------------+-------------+
| jobid | level | jobfiles | jobbytes   | starttime           | volumename  |
+-------+-------+----------+------------+---------------------+-------------+
|     3 | F     |        4 | 81,788,928 | 2018-04-05 05:51:38 | TestVolume2 |
+-------+-------+----------+------------+---------------------+-------------+
You have selected the following JobId: 3

Building directory tree for JobId(s) 3 ...  
3 files inserted into the tree.

You are now entering file selection mode where you add (mark) and
remove (unmark) files to be restored. No files are initially added, unless
you used the "all" keyword on the command line.
Enter "done" to leave this mode.

cwd is: /
'''

CMD_RESTORE2_OUT = b'''
Bootstrap records written to /BACKUP/bacula/var/dev-dir.restore.8.bsr
Bootstrap records written to /BACKUP/bacula/var/dev-dir.restore.8.bsr

The Job will require the following (*=>InChanger):
       Volume(s)                 Storage(s)                SD Device(s)
===========================================================================
          
        TestVolume2               dev-sd                    FileStorage              

Volumes marked with "*" are in the Autochanger.


4 files selected to be restored.

Using Catalog "DefaultCatalog"
Run Restore job
JobName:         RestoreJob
Bootstrap:       /BACKUP/bacula/var/dev-dir.restore.8.bsr
Where:           /tmp/restore
Replace:         Always
FileSet:         DefaultFileSet
Backup Client:   RestoreFromClient1
Restore Client:  RestoreFromClient1
Storage:         dev-sd
When:            2018-05-03 04:27:41
Catalog:         DefaultCatalog
Priority:        10
Plugin Options:  
OK to run? (yes/mod/no):
'''

CMD_RESTORE3_OUT = b'''
Parameters to modify:
     1: Level
     2: Storage
     3: Job
     4: FileSet
     5: Restore Client
     6: When
     7: Priority
     8: Bootstrap
     9: Where
    10: File Relocation
    11: Replace
    12: JobId
    13: Plugin Options
Select parameter to modify (1-13):
'''

CMD_RESTORE4_OUT = b'''
The defined Client resources are:
     1: RestoreToClient1
     2: LocalClient1
Select Client (File daemon) resource (1-2):
'''

CMD_JOBSTATUS_OUT = b'''
Automatically selected Catalog: DefaultCatalog
Using Catalog "DefaultCatalog"
+-------+------------+---------------------+------+-------+----------+--------------------+-----------+
| jobid | name       | starttime           | type | level | jobfiles | jobbytes           | jobstatus |
+-------+------------+---------------------+------+-------+----------+--------------------+-----------+
|     5 | RestoreJob | 2018-05-05 08:13:07 | R    | F     |        5 |        843,432,234 | f         |
+-------+------------+---------------------+------+-------+----------+--------------------+-----------+
'''

STATES = {
    'AUTH0': [{
        'in': b'Hello *UserAgent* calling\n',
        'out': b'auth cram-md5 <111111111.2222222222@dev-dir> ssl=0\n',
        'next': 'AUTH1'
    }],
    'AUTH1': [{
        'in': b'RiQLEuKuykeY/ca/DU+JbQ',
        'out': b'1000 OK auth\n',
        'next': 'AUTH2'
    }],
    'AUTH2': [{
        'in': b'auth cram-md5c <111111111.2222222222@dev-dir> ssl=0\n',
        'out': b'RiQLEuKuykeY/ca/DU+JbQ',
        'next': 'AUTH3'
    }],
    'AUTH3': [{
        'in': b'1000 OK auth\n',
        'out': b'1000 OK auth\n',
        'next': 'CMD'
    }],
    'CMD': [
        {
            'in': b'quit',
            'out': b'bye',
            'next': 'END'
        },
        {
            'in': b'version',
            'out': b'1000 OK: 102 dev-dir Version: 7.4.7 (16 March 2017)\n',
            'next': 'CMD'
        },
        {
            'in': b'restore where=/tmp/restore client=RestoreFromClient1',
            'out': CMD_RESTORE0_OUT,
            'next': 'CMD_RESTORE1'
        },
        {
            'in': b'list jobid=5',
            'out': CMD_JOBSTATUS_OUT,
            'next': 'CMD'
        }
    ],
    'CMD_RESTORE1': [{
        'in': b'5',
        'out': CMD_RESTORE1_OUT,
        'next': 'CMD_RESTORE2'
    }],
    'CMD_RESTORE2': [
        {
            'in': b'cd /',
            'out': b'cwd is: /',
            'next': 'CMD_RESTORE2'
        },
        {
            'in': re.compile('^cd\s+.+'),
            'out': b'cwd is: XXX',
            'next': 'CMD_RESTORE2'
        },
        {
            'in': re.compile('^mark\s+.+'),
            'out': b'X files marked.',
            'next': 'CMD_RESTORE2'
        },
        {
            'in': re.compile('^unmark\s+.+'),
            'out': b'X file unmarked.',
            'next': 'CMD_RESTORE2'
        },
        { 
            'in': b'done',
            'out': CMD_RESTORE2_OUT,
            'next': 'CMD_RESTORE3'
        }
    ],
    'CMD_RESTORE3': [{
        'in': b'mod',
        'out': CMD_RESTORE3_OUT,
        'next': 'CMD_RESTORE4'
    }],
    'CMD_RESTORE4': [{
        'in': b'5',
        'out': CMD_RESTORE4_OUT,
        'next': 'CMD_RESTORE5'
    }],
    'CMD_RESTORE5': [{
        'in': b'1',
        'out': CMD_RESTORE2_OUT,
        'next': 'CMD_RESTORE6'
    }],
    'CMD_RESTORE6': [{
        'in': b'yes',
        'out': b'Job queued. JobId=5',
        'next': 'CMD'
    }],
    'END': [{
        'in': '',
        'out': '',
        'next': None
    }]
}


class FakeBaculaStateMachine:
    def __init__(self, init_state, states):
        self.currentState = init_state
        self.currentStateCondition = None
        self.states = states
        self.logger = logging.getLogger(self.__class__.__name__)

    def __ensureString(self, string):
        if type(string) is bytes:
            return string.decode('utf8')
        else:
            return string

    def __changeState(self, new_state):
        self.logger.debug("DEBUG: change state from {} to {}".format(self.currentState, new_state))
        self.currentState = new_state

    def __checkCondition(self, condition, input):
        condition_in = condition['in']
        if type(condition_in) is str or type(condition_in) is bytes:
            if input == condition['in']:
                return True
        elif type(condition_in) is re._pattern_type:
            if condition['in'].match(self.__ensureString(input)):
                return True
        else:
            raise Exception("Unknown condition type")
        return False

    def resetState(self, new_state):
        self.currentState = new_state

    def next(self, input = None):
        '''
            Method moves FSM to next state due to msg content
            If input is None - returns output for current state and doesn't change FSM state
        '''
        if input is None:
            if self.currentStateCondition is None:
                return None
            else:
                return self.currentStateCondition['out']

        if not self.currentStateCondition is None:
            self.__changeState(self.currentStateCondition['next'])
            self.currentStateCondition = None

        for condition in self.states[self.currentState]:
            if self.__checkCondition(condition, input):
                self.currentStateCondition = condition
                return condition['out']
        raise Exception("Wrong input data for the state {}".format(self.currentState))

    def output(self):
        return self.next()


class FakeBaculaServerSocket:
    """
        Mock object class which emulates bacula server answers via socket
    """
    def __init__(self, *args, **kwargs):
        self.isConnected = False
        self.wallet = BSocketWallet(DIR_TEST_PASSWORD)
        self.recv_bytes = 0
        self.answer = None
        self.statem = FakeBaculaStateMachine('AUTH0', STATES)
    
    def __checkInputDataSize(self, data):
        if data is None:
            return
        if len(data) < 4:
            raise Exception("Data too small")
        data_size = unpack("!i", data[:4])[0]
        if  data_size != len(data[4:]):
            raise Exception("Wrong data size")

    def connect(self, conn_data):
        self.isConnected = True

    def close(self):
        self.isConnected = False
        self.isAuthenticated = False
        self.statem.resetState('AUTH0')

    def send(self, data):
        '''
            send fake method - receive data from client (like client sent smthng)

        '''
        if not self.isConnected:
            raise Exception("Not Connected")

        self.__checkInputDataSize(data)
        self.statem.next(data[4:])
        self.recv_bytes = 0

    def recv(self, size = 0):
        '''
            recv fake method - send data to client
        '''
        if not self.isConnected:
            raise Exception("Not Connected")

        state_answer = self.statem.output()
        answer = pack("!i", len(state_answer)) + state_answer
        if size > 0:
            answer = answer[self.recv_bytes:(size + self.recv_bytes)]
            self.recv_bytes += size
        else:
            answer = answer[self.recv:]
        return answer


def getFakeChallengeString(cls):
    return "<111111111.2222222222@dev-dir>"


@patch.object(BSocket, '_BSocket__getChallengeString', getFakeChallengeString) 
@patch('socket.socket', new=FakeBaculaServerSocket)
class TestBSocket(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.console = BConsole(None, None, DIR_TEST_PASSWORD, TEST_USER_AGENT)

    def test_connection(self):
        self.assertEqual(self.console.getVersion()['director_version'], TEST_VERSION)

    def test_restore(self):
        restore_files = ['/opt/DATA1', '/opt/DATA2']
        exclude_files = ['/opt/DATA1/exclude1', '/opt/DATA1/exclude2/*']
        restore_from_client = 'RestoreFromClient1'
        restore_to_client = 'RestoreToClient1'
        wrong_restore_from_client = 'RestoreFromClientWRONG'
        wrong_restore_to_client = 'RestoreToClientWRONG'
        restore_path = '/tmp/restore'
        self.assertEqual(self.console.doRestore(restore_from_client, restore_to_client, restore_path, restore_files, exclude_from_restore=exclude_files)['jobid'], TEST_JOBID)

    def test_jobstatus(self):
        self.assertEqual(self.console.getJobStatus(TEST_JOBID), JobStatus(TEST_JOB_STATUS))

    def test_backup(self):
        pass
