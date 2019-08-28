#!/usr/bin/env python3.6
# -*- encoding: utf-8 -*-

import argparse
import itertools
import json
import logging
import math
import os
import pathlib
import re
import shlex
import sys
import subprocess
import tempfile
import time

import googleapiclient.discovery

from collections import namedtuple


PROJECT = os.getenv('PROJECT')
ZONE = os.getenv('ZONE')


MachineType = namedtuple('MachineType', ['name', 'cpu', 'ram'])


def create_service():
    return googleapiclient.discovery.build('compute', 'v1')


def get_machine_types(service):
    lst_mt = service.machineTypes().list(project=PROJECT, zone=ZONE).execute()['items']
    lst_conf = filter_machine_types(lst_mt, my_filter_mt)
    return list(map(
        lambda x: MachineType(lst_mt[x[0]]['name'], float(x[1]['cpu']), float(x[1]['ram'])), lst_conf
    ))


def my_filter_mt(d):
    if d is None:
        return False
    return d is not None \
        and 4 <= float(d['cpu']) <= 8 \
        and 4 <= float(d['ram']) <= 64
    


def filter_machine_types(lm, mt_filter):
    def pattern_id(m):
        return m.groupdict() if m is not None else None

    regex = re.compile(r'(?P<cpu>\d*\.*\d+) vCPUs, (?P<ram>\d*\.*\d+) GB RAM')
    return list(
      filter(
        lambda x: mt_filter(x[1]), 
        enumerate(
            map(
                lambda x: pattern_id(regex.match(x['description'])), lm
            )
        )
        )
    )


def get_image(service, project='ubuntu-os-cloud', family='ubuntu-1604-lts'):
    return service.images().getFromFamily(project=project, family=family).execute()


def create_instance(service, mt, name='test-instance'):
    source_disk_image = get_image(service)['selfLink']
    machine_type = f'zones/{ZONE}/machineTypes/{mt.name}'
    startup_script = pathlib.Path('./startup-script.sh').read_text()

    config = {
        'name': name,
        'machineType': machine_type,

        'disks': [
            {
                'boot': True,
                'autoDelete': True,
                'initializeParams': {
                    'sourceImage': source_disk_image,
                    'diskSizeGb': 25
                }
            },
            {
                "boot": False,
                "mode": 'READ_ONLY',
                "source": 'https://www.googleapis.com/compute/v1/projects/virtual-sylph-248909/zones/europe-west3-c/disks/disk-1',
                "deviceName": 'disk-1',
                "index": 1,
                "autoDelete": False,
                "interface": 'SCSI',
                "kind": 'compute#attachedDisk'
            }
        ],

        'networkInterfaces': [{
            'network': 'global/networks/default',
            'accessConfigs': [
                {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
            ]
        }],

        'serviceAccounts': [{
            'email': '526812840799-compute@developer.gserviceaccount.com',
            'scopes': [
                'https://www.googleapis.com/auth/devstorage.read_write',
                'https://www.googleapis.com/auth/logging.write'
            ]
        }],

        'metadata': {
            'items': [
            #     {
            #     # Startup script is automatically executed by the
            #     # instance upon startup.
            #     'key': 'startup-script',
            #     'value': startup_script
            # }
            ]
        }
    }

    return service.instances().insert(
        project=PROJECT,
        zone=ZONE,
        body=config).execute()


def delete_instance(compute, name):
    return compute.instances().delete(
        project=PROJECT,
        zone=ZONE,
        instance=name).execute()


def wait_for_operation(compute, operation):
    while True:
        result = compute.zoneOperations().get(
            project=PROJECT,
            zone=ZONE,
            operation=operation).execute()

        if result['status'] == 'DONE':
            if 'error' in result:
                raise Exception(result['error'])
            return result

        time.sleep(1)



def scp_file_to(src, dst, username, host, key=os.getenv('SSH_KEY')):
    command = f"scp -o StrictHostKeyChecking=no -i {key} {src} {username}@{host}:{dst}"
    proc = subprocess.run(shlex.split(command))
    return proc.returncode, proc.stdout, proc.stderr


def scp_file_from(src, dst, username, host, key='~/.ssh/gcloud'):
    command = f"scp -o StrictHostKeyChecking=no -i {key} {username}@{host}:{src} {dst}"
    proc = subprocess.run(shlex.split(command))
    return proc.returncode, proc.stdout, proc.stderr


def scp_folder_to(src, dst, username, host, key='~/.ssh/gcloud'):
    command = f"scp -r -o StrictHostKeyChecking=no -i {key} {src} {username}@{host}:{dst}"
    proc = subprocess.run(shlex.split(command))
    return proc.returncode, proc.stdout, proc.stderr


def ssh_command(cmd, username, host, key='~/.ssh/gcloud'):
    command = f"ssh  -o StrictHostKeyChecking=no -i {key} -t {username}@{host} \"{cmd}\""
    proc = subprocess.run(shlex.split(command))
    return proc.returncode, proc.stdout, proc.stderr


def str_pg_conf(conf):
    return ''.join(list(map(lambda x: 'ALTER SYSTEM SET ' + x + ';\n', (
        f"max_connections = {conf['max_connections']}",
        f"shared_buffers = \"{conf['shared_buffers']}MB\"",
        f"effective_cache_size = \"{conf['effective_cache_size']}MB\"",
        f"maintenance_work_mem = \"{conf['maintenance_work_mem']}MB\"",
        f"checkpoint_completion_target = {0.9}",
        f"wal_buffers = \"{16}MB\"",
        f"default_statistics_target = {100}",
        f"random_page_cost = {4}",
        f"effective_io_concurrency = {2}",
        f"work_mem = \"{conf['work_mem']}kB\"",
        f"min_wal_size = \"{1}GB\"",
        f"max_wal_size = \"{2}GB\"",
        f"max_worker_processes = {conf['max_worker_processes']}",
        f"max_parallel_workers_per_gather = {conf['max_parallel_workers_per_gather']}",
        f"max_parallel_workers = {conf['max_parallel_workers']}")))
    )



def make_conf(cpu, ram):
    confpath = f'./pg_confs/cpu{cpu}ram{ram}.sql'
    # if not pathlib.Path(confpath).is_file():  
    if True:
        conf = {}
        conf['max_connections'] = 20
        conf['shared_buffers'] = 1024 // 4 * ram
        conf['effective_cache_size'] = 1024 * 3 // 4 * ram
        conf['maintenance_work_mem'] = 64 * ram
        conf['max_worker_processes'] = cpu
        conf['max_parallel_workers_per_gather'] = cpu // 2 + cpu % 2
        conf['max_parallel_workers'] = cpu
        conf['work_mem'] = max(
            64, math.floor(
                (ram * 1024 * 1024 - conf['shared_buffers'] * 1024) \
                    / (2 * 3 * conf['max_connections'] * conf['max_parallel_workers_per_gather']))
                    )
        with open(confpath, 'w') as f:
            f.write(str_pg_conf(conf))

    return confpath


def create_instance_handler(service, machine_type, name='test-instance'):
    operation = create_instance(service, machine_type, name)
    wait_for_operation(service, operation['name'])
    instance = service.instances().get(project=PROJECT, zone=ZONE, instance=name).execute()
    return instance


def delete_instance_handler(service, name='test-instance'):
    operation = delete_instance(service, name)
    wait_for_operation(service, operation['name'])


def get_natip(instance):
    return instance['networkInterfaces'][0]['accessConfigs'][0]['natIP']


def generate_confs(machine_type):
    def generate_lb_biased(value):
        logval = math.log(value) / math.log(2)
        ilogval = math.floor(logval)
        if ilogval == logval:
            return int(2 ** (ilogval - 1)), int(2 ** ilogval)
        else:
            return int(2 ** (ilogval)), int(2 ** (ilogval + 1))

    grid = list(itertools.product(
        generate_lb_biased(machine_type.cpu), 
        generate_lb_biased(machine_type.ram)))
    
    return list(map(lambda x: make_conf(*x), grid))
    

def instance_handler(service, machine_type, name='test-instance'):
    kh = pathlib.Path.home() / '.ssh' / 'known_hosts'
    if kh.is_file():
        kh.unlink()

    logger = logging.getLogger(__name__)

    logger.info(f'{machine_type.name}: Creating instance')
    instance = create_instance_handler(service, machine_type, name)
    natIP = get_natip(instance)
    scp_file_to('./startup-script.sh', '~/startup-script.sh', 'hyd99nsker', natIP)
    ssh_command('bash startup-script.sh', 'hyd99nsker', natIP)

    logger.info(f'{machine_type.name}: Uploading tests')

    scp_file_to('./test.sh', '~/test.sh', 'hyd99nsker', natIP)
    scp_folder_to('./tests', '~/tests', 'hyd99nsker', natIP)

    table = []
    logger.info(f'{machine_type.name}: Running tests')
    for confpath in generate_confs(machine_type):
        scp_file_to(confpath, '~/conf.sql', 'hyd99nsker', natIP)
        ssh_command(
            'sudo -u postgres psql -f ~/conf.sql \
                && sudo service postgresql restart', 'hyd99nsker', natIP)
        ssh_command('bash test.sh output.txt', 'hyd99nsker', natIP)
        fname = pathlib.Path(confpath).name.split('.')[0]
        scp_file_from(
            './output.txt', f'./out/{machine_type.name}_{fname}.txt', 'hyd99nsker', natIP)
        #  with open('./output.txt', 'r') as f:
        #     for line in f.readlines():
        #         table.append([machine_type.name, confpath] + line.split())


    logger.info(f'{machine_type.name}: Deleting instance')
    delete_instance_handler(service, name)
    return table


def main(argc, argv):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)


    service = create_service()
    mts = get_machine_types(service)
    table = []
    for mt in mts:
        logger.info(mt.name)
        instance_handler(service, mt)



if __name__ == '__main__':
    main(len(sys.argv), sys.argv)
