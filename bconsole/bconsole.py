
# BConsole python implementation
# author: avdmitrenok@gmail.com
# version: 0.6.13

import logging
import socket
import hashlib
import hmac
import base64
import random
import time
import os
import re
from datetime import datetime
from abc import abstractmethod
from struct import pack, unpack

DIR_AUTH_OK_MESSAGE = "1000 OK auth\n"
DIR_AUTH_ERROR_MESSAGE = "1999 Authorization failed.\n"

TASK_STATUSES = {
    'C': 'Created, not yet running',
    'R': 'Running',
    'B': ' Blocked',
    'T': 'Completed successfully',
    'E': 'Terminated with errors',
    'e': 'Non-fatal error',
    'f': 'Fatal error',
    'D': 'Verify found differences',
    'A': 'Canceled by user',
    'F': 'Waiting for Client',
    'S': 'Waiting for Storage daemon',
    'm': 'Waiting for new media',
    'M': 'Waiting for media mount',
    's': 'Waiting for storage resource',
    'j': 'Waiting for job resource',
    'c': 'Waiting for client resource',
    'd': 'Waiting on maximum jobs',
    't': 'Waiting on start time',
    'p': 'Waiting on higher priority jobs',
    'a': 'SD despooling attributes',
    'i': 'Doing batch insert file records',
    'I': 'Incomplete Job'
}

class BSocketWallet:
    '''
        Class for password storage, I don't want to store password in plain text
    '''
    def __encodePassword(self, password):
        if isinstance(password, str):
            password = password.encode('utf8')
        md5 = hashlib.md5()
        md5.update(password)
        return md5.hexdigest()

    def __init__(self, dir_password, dir_host = None, dir_port = None):
        self.host = dir_host
        self.port = dir_port
        self.password = self.__encodePassword(dir_password)

class BSocket:
    DIR_HELLO_MESSAGE = "Hello {} calling\n"
    DEFAULT_USER_AGENT = "*UserAgent*"

    '''
        Class provides bacula director socket interface (with implicit authentification)
    '''

    def __init__(self, wallet, user_agent=None):
        if user_agent is None:
            user_agent = self.DEFAULT_USER_AGENT
        self.wallet = wallet
        self.isSSLRequired = False
        self.isAuthenticated = False
        self.socket = None
        self.userAgent = user_agent
        self.logger = logging.getLogger(self.__class__.__name__)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, traceback):
        if self.socket != None:
            if self.isAuthenticated:
                self.__send("quit")
            self.socket.close()

    def __getSocket(self):
        if self.socket == None:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.wallet.host, self.wallet.port))
        return self.socket

    def __send(self, message):
        '''use socket to send request to director '''
        if isinstance(message, str): message = message.encode('utf8')
        socket = self.__getSocket()
        socket.send(pack("!i", len(message)) + message) # convert to network flow
        self.logger.debug("send message {}".format(message))

    def __receive(self): # throws RuntimeError
        '''will receive data from director '''
        socket = self.__getSocket()
        message = socket.recv(4) # first get the message length
        if len(message) < 4:
            return None # TODO: decide how to process this case
        nbyte = unpack("!i", message)[0]
        if nbyte <= 0:
            return None
        message = socket.recv(nbyte)
        self.logger.debug("received message ({}/{}): {}".format(nbyte, len(message), message))
        return message

    def __getHMACDiggest(self, key, message):
        if isinstance(key, str): key = key.encode('utf8')
        if isinstance(message, str): message = message.encode('utf8')
        hmac_md5 = hmac.new(key, digestmod='md5')
        hmac_md5.update(message)
        return base64.b64encode(hmac_md5.digest()).rstrip(b'=')

    def __getChallengeString(self):
        rand = random.randint(1000000000, 9999999999)
        return "<{}.{}@{}>".format(rand, int(time.time()), socket.gethostname())

    # TODO: implement TLS auth
    def __authenticate(self): # throws RuntimeError
        socket = self.__getSocket()

        # authenticate on the directory
        self.__send(self.DIR_HELLO_MESSAGE.format(self.userAgent))
        resp = self.__receive()
        (cmd, auth_type, server_challenge_string, require_ssl) = resp.split(b' ')[:4]
        if require_ssl == b'ssl=1':
            self.isSSlRequire = True
        if cmd != b'auth':
            raise RuntimeError("Autorization error: wrong director answer")
        self.__send(self.__getHMACDiggest(self.wallet.password, server_challenge_string))
        resp = self.__receive()

        if resp == DIR_AUTH_OK_MESSAGE.encode('utf8'):
            # authenticate director
            client_challenge_string = self.__getChallengeString()
            self.__send("auth cram-md5c {} ssl={}\n".format(client_challenge_string, (1 if self.isSSLRequired else 0)))
            res = self.__receive().rstrip(b'\x00')
            hmac_cmp = self.__getHMACDiggest(self.wallet.password, client_challenge_string)
            self.logger.debug("Diggest check. Received: {} --- Calculated: {}".format(res, hmac_cmp))
            if hmac_cmp == res:
                self.__send(DIR_AUTH_OK_MESSAGE)
                self.__receive()
                self.isAuthenticated = True
            else:
                self.__send(DIR_AUTH_ERROR_MESSAGE)
        else:
            raise RuntimeError("Authorization error: check your password")

    def send(self, message):
        if not self.isAuthenticated:
            self.__authenticate()
        self.__send(message)

    def receive(self, rstrip=None):
        if not self.isAuthenticated:
            self.__authenticate()
        msg = self.__receive()
        if msg == None:
            return msg
        if rstrip != None:
            return msg.decode('utf8').rstrip(rstrip)
        return msg.decode('utf8')

    def cmd(self, cmd):
        self.send(cmd)
        result = []
        msg = self.receive()
        while msg != None:
            result.append(msg)
            msg = self.receive()
        return "".join(result)

class BConsoleCommand:
    '''
        Base abstract class for all command classes
    '''
    RE_OPTION = re.compile('^\s*(\d+)\s*:\s*(.+)$')

    def __init__(self, wallet, user_agent):
        self.wallet = wallet
        self.userAgent = user_agent
        self.logger = logging.getLogger(self.__class__.__name__)

    def _parseTable(self, table_text):
        head = []
        data = []
        in_table = False
        for line in table_text.splitlines():
            if line.startswith('+--') and in_table:
                in_tables = True
            elif line.startswith('|'):
                row_data = [v.rstrip().lstrip() for v in line.split('|') if v != '']
                if not head: # first line in the table is always header
                    head = row_data
                else:
                    data.append(dict(zip(head, row_data)))
        return data

    def _parseMenuOptions(self, options_text):
        options = {}
        for line in options_text.splitlines():
            mr = self.RE_OPTION.match(line)
            if mr:
                options[mr.group(2)] = mr.group(1)
        return options

    def log(self, message, severity='INFO'):
        pass

    @abstractmethod
    def run(self):
        pass


class BConsoleCommandVersion(BConsoleCommand):
    RE_VERSION = re.compile('^.*?Version: (.+?) ')

    def run(self):
        msg = None
        with BSocket(self.wallet, user_agent=self.userAgent) as dir:
            msg = dir.cmd("version")
        mres = self.RE_VERSION.match(msg)
        version = None
        if mres is not None:
            version = mres.group(1)
        return version


class BConsoleCommandClientStatus(BConsoleCommand):
    MSG_CLIENT_CONNECTION_ERROR = "Failed to connect to Client"
    MSG_CLIENT_CONNECTION_OK = "Daemon started"

    def __init__(self, wallet, client_name, user_agent):
        super().__init__(wallet, user_agent)
        self.clientName = client_name

    def run(self):
       msg = None
       with BSocket(self.wallet, user_agent=self.userAgent) as dir:
           msg = dir.cmd("status client={}".format(self.clientName))
       if self.MSG_CLIENT_CONNECTION_OK in msg:
           return {'result': True}
       else:
           return {'result': False}


class BConsoleCommandJobStatus(BConsoleCommand):
    def __init__(self, wallet, job_id, user_agent):
        super().__init__(wallet, user_agent)
        self.jobId = job_id

    def run(self):
        res = None
        with BSocket(self.wallet, user_agent=self.userAgent) as dir:
            res = self._parseTable(dir.cmd("list jobid={}".format(self.jobId)))
        return res


class BConsoleCommandRestore(BConsoleCommand):
    '''
        Class implements bacula restore command
    '''
    RE_JOBID = re.compile('.*Job queued.\s+JobId=(\d+).*')

    def __init__(self, wallet, restore_from_client, restore_to_client, restore_where, files_to_restore, user_agent, exclude_from_restore=[], date=None, fileset=None):
        super().__init__(wallet, user_agent)
        self.restoreFromClient = restore_from_client
        self.restoreToClient = restore_to_client
        self.restoreWhere = restore_where
        self.filesToRestore = files_to_restore
        self.excludeFromRestore = exclude_from_restore
        self.restoreDate = date
        self.fileset = fileset

    def __selectFiles(self, dir, filelist = [], action="mark"):
        '''
            Mark or unmark files. If filelist is empty and action=mark - mark all files
        '''
        if 'cwd is: /' in dir.cmd("cd /"):
            if action == "mark" and len(filelist) == 0:
                dir.cmd("mark *")
            else:
                for filelist_element in filelist:
                    path_elements = filelist_element.rstrip('/').lstrip('/').split('/')
                    dir.cmd("cd /")
                    for path_element in path_elements[:-1]:
                        dir.cmd("cd {}".format(path_element))
                    dir.cmd("{} {}".format(action, path_elements[-1]))

    def run(self):
        jobid = None
        with BSocket(self.wallet, user_agent=self.userAgent) as dir:
            console_output = None

            option = self._parseMenuOptions(
                dir.cmd("restore where={} client={}".format(self.restoreWhere, self.restoreFromClient))
            ).get('Select the most recent backup for a client', None)
            if option is None:
                raise Exception("Wrong answer from the director (I)")
            console_output = dir.cmd(option)
            if console_output is None:
                raise Exception("Wrong answer from the director (II)")
            elif 'Select the Client' in console_output:
                raise Exception("Client {} not found".format(self.restoreFromClient))
            if 'The defined FileSet resources are:' in console_output:
                if self.fileset is None:
                    raise Exception("Fileset wasn't set")
                option = self._parseMenuOptions(console_output).get(self.fileset)
                console_output = dir.cmd(option)
            if 'cwd is' in console_output:
                self.__selectFiles(dir, self.filesToRestore)
                self.__selectFiles(dir, self.excludeFromRestore, action="unmark")
                console_output = dir.cmd("done")
            if 'OK to run? (yes/mod/no):' in console_output:
                option = self._parseMenuOptions(dir.cmd('mod')).get('Restore Client', None)
                option = self._parseMenuOptions(dir.cmd(option)).get(self.restoreToClient, None)
                if option is None:
                    raise Exception("Can't restore to {} client. There is no such client in restore list".format(self.restoreToClient))
                console_output = dir.cmd(option)
            if 'OK to run? (yes/mod/no):' in console_output:
                console_output = dir.cmd("yes")
            if not 'Job queued.' in console_output:

                with open('/tmp/xxx.log', 'a') as f:
                    print(console_output, file=f)

                raise Exception("Can't start restore procedure. Something went wrong")
            jobid = self.RE_JOBID.match(console_output).group(1)
        return jobid


class BConsoleCommandBackup(BConsoleCommand):
    def run(self):
        pass


class JobStatus:
    def __init__(self, job_data):
        self.id = job_data['jobid']
        self.starttime = job_data['starttime']
        self.status = job_data['jobstatus']
        self.files = job_data['jobfiles']
        self.bytes = job_data['jobbytes']
        self.type = job_data['type']
        self.level = job_data['level']

    def isFinished(self):
        if self.status == 'T' or self.status == 'E':
            return True
        else:
            return False

    def isSuccess(self):
        if self.status == 'T':
            return True
        return False

    def as_dict(self):
        return {
            'id': self.id,
            'starttime': self.starttime,
            'status': self.status,
            'files': self.files,
            'bytes': self.bytes,
            'type': self.type,
            'level': self.level
        }

    def __str__(self):
        return "id={}, finished={}, description={}".format(self.id, self.isFinished(), TASK_STATUSES[self.status])

    def __eq__(self, other):
        if (self.id == other.id and
            self.starttime == other.starttime and
            self.status == other.status and
            self.files == other.files and
            self.bytes == other.bytes and
            self.type == other.type and
            self.level == other.level):
            return True
        else:
            return False


class BConsole:
    def __init__(self, dir_addr, dir_port, dir_password, user_agent):
        self.wallet = BSocketWallet(dir_password, dir_addr, dir_port)
        self.userAgent = user_agent
        self.logger = logging.getLogger(self.__class__.__name__)

    def getVersion(self):
        dir_version = BConsoleCommandVersion(self.wallet, self.userAgent).run()
        return {'director_version': dir_version}

    def getClientStatus(self, client_name):
        client_status = BConsoleCommandClientStatus(self.wallet, client_name, self.userAgent).run()
        return {'client_name': client_name, 'status': client_status}

    def getJobStatus(self, job_id):
        job_status = BConsoleCommandJobStatus(self.wallet, job_id, self.userAgent).run()
        if len(job_status) > 0:
            job_status = job_status[0]
            job_status['jobid'] = int(job_status['jobid'].replace(',', ''))
            job_status['jobbytes'] = int(job_status['jobbytes'].replace(',', ''))
            job_status['jobfiles'] = int(job_status['jobfiles'].replace(',', ''))
            if job_status['starttime'] is not None and job_status['starttime'] != '':
                job_status['starttime'] = datetime.strptime(job_status['starttime'], "%Y-%m-%d %H:%M:%S")
            self.logger.debug(job_status)
            return JobStatus(job_status)
        else:
            return {}

    def doRestore(self, restore_from_client, restore_to_client, restore_where, files_to_restore=[], exclude_from_restore=[], date=None, fileset=None):
        '''
            Restores backup.
            If date == None - restores last backup for the restore_from_client, else - will be restored backup for a specified date
        '''
        if not date is None and not type(date) is datetime.datetime:
            raise Exception("Wrong restore date format, should be datetime.datetime")
        jobid = BConsoleCommandRestore(self.wallet, restore_from_client, restore_to_client, restore_where, files_to_restore, self.userAgent, exclude_from_restore=exclude_from_restore, date=date, fileset=fileset).run()
        self.logger.debug("jobid={}".format(jobid))
        return {'jobid': jobid, 'jobtype': 'restore'}

    def doBackup(self):
        jobid = BConsoleCommandBackup(self.wallet, self.userAgent).run()
        return {'jobid': jobid, 'jobtype': 'backup'}
