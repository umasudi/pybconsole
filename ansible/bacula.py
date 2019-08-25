#!/usr/bin/env python

# Ansible module for bacula control
# author: avdmitrenok@gmail.com
# version: 0.4.2

import sys
import time
import pprint
from ansible.module_utils.basic import AnsibleModule

try:
    from bconsole.bconsole import BConsole
    HAS_BCONSOLE_LIB = True
except:
    HAS_BCONSOLE_LIB = False

DEFAULT_DIRECTOR_PORT=9101

DOCUMENTATION = '''
---
module: bacula
short_description: Module for bacula operations (Restore/Status)
version_added: "0.4.2"
description:
  - "Module for ansible operations (restore/status/backup)"
options:
  action:
    description:
    - Action for bacula client (restore/jobstatus/waitforjob)
    required: true
  director_address:
    description:
    - Address of Bacula Director server
    required: true
  director_port:
    description:
    - Port of Bacula Director server (default is 9101)
    required: false
  director_password:
    description:
    - Password for bacula director
    required: true
  user_agent:
    description:
    - User Agent name for console client (the same as username).
    required: true
  backup_client:
    description:
    - Hostname which backup is required for
    required: false
  restore_client:
    description:
    - Hostname where the backup should be restored
    required: true
  restore_location:
    description:
    - A path where the backup should be restored
    required: true
  job_id:
    description:
    - Job ID
    required: false
author:
- Anton Dmitrenok (avdmitrenok@gmail.com)
'''

EXAMPLES = '''
- name: Backup restore
  ncbacula: action=restore user_agent=console1 director_address=192.168.0.50 director_port=12345 director_password=12345 backup_host=hostB.domain.com restore_host=hostR.domain.com restore_location=/opt/restore
'''

#RETURN = '''
#original_message:
#  description: The original name param that was passed in
#  type: str
#message:
#  description: The output message that the sample module generates
#'''

def do_restore(dir_addr, dir_password, user_agent, backup_client, restore_client, restore_location, files_to_restore, dir_port=DEFAULT_DIRECTOR_PORT, files_to_exclude=[], fileset=None):
    if backup_client is None or restore_client is None or restore_location is None or files_to_restore is None:
        raise Error("Action: restore. Mandatory parameter(s) does not exist")
    bcon = BConsole(dir_addr, dir_port, dir_password, user_agent)
    res = bcon.doRestore(backup_client, restore_client, restore_location, files_to_restore, exclude_from_restore=files_to_exclude, fileset=fileset)
    return res

def get_job_status(dir_addr, dir_password, user_agent, dir_port, job_id):
    if job_id is None:
        raise Error("Action: jobstatus. Job ID wasn't specified")
    bcon = BConsole(dir_addr, dir_port, dir_password, user_agent)
    return bcon.getJobStatus(job_id)

def wait_for_job(module, dir_addr, dir_password, user_agent, dir_port, job_id):
    job_status = None
    while True:
        job_status = get_job_status(dir_addr, dir_password, user_agent, dir_port, job_id)
        if job_status.isFinished():
            break
        time.sleep(60)

    if job_status.isSuccess():
        return {'jobid': job_id, 'fail': False, 'success': True, 'result': job_status.as_dict()}
    else:
        return {'jobid': job_id, 'fail': True, 'success': False, 'result': job_status.as_dict()}

def main():
    module = AnsibleModule(
        argument_spec = dict(
            action = dict(required=True, type=str),
            director_address = dict(required=True, type=str),
            director_password = dict(required=True, type=str),
            user_agent = dict(required=True, type=str),
            director_port = dict(required=False, type=int, default=DEFAULT_DIRECTOR_PORT),
            backup_client = dict(required=False, type=str),
            restore_client = dict(required=False, type=str),
            restore_location = dict(required=False, type=str),
            files_to_restore = dict(required=False, type=list),
            files_to_exclude = dict(required=False, type=list, default=[]),
            job_id = dict(required=False, type=int),
            fileset = dict(required=False, type=str, default=None)
        )
    )
    
    if not HAS_BCONSOLE_LIB:
        module.fail_json(msg="Module bconsole not found")

    result = None
    try:
        if module.params['action'] == 'restore':
            result = do_restore(
                module.params['director_address'],
                module.params['director_password'],
                module.params['user_agent'],
                module.params['backup_client'],
                module.params['restore_client'],
                module.params['restore_location'],
                module.params['files_to_restore'],
                dir_port=module.params['director_port'],
                files_to_exclude = module.params['files_to_exclude'],
                fileset=module.params['fileset']
            )
        elif module.params['action'] == 'jobstatus':
            result = get_job_status(
                module.params['director_address'], 
                module.params['director_password'],
                module.params['user_agent'],
                module.params['director_port'],
                module.params['job_id']
            ).as_dict()
        elif module.params['action'] == 'waitforjob':
            result = wait_for_job(
                module,
                module.params['director_address'],
                module.params['director_password'],
                module.params['user_agent'],
                module.params['director_port'],
                module.params['job_id']
            )
            if result['fail']:
                module.fail_json(msg="")

        module.exit_json(**result)
    except Exception as e:
        module.fail_json(msg="Error: {}".format(e))

if __name__ == '__main__':
    main()
