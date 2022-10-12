# (C) 2021 The University of Chicago
# See COPYRIGHT in top-level directory.


"""
.. module:: specs
  :synopsis: This package provides various functions to
  build specs for bits of the HEPnOS configuration.

.. moduleauthor:: Matthieu Dorier <mdorier@anl.gov>


"""


import mochi.bedrock.spec
import os
import copy
import json


__object_types = ['dataset', 'run', 'subrun', 'event', 'product']


def gen_database_spec(contains: str, index: int=0, type: str="map", path: str='', config: dict={}):
    if contains not in __object_types:
        raise ValueError('"contains" should be "dataset", "run", "subrun", "event", or "product".')
    name = "hepnos-"+contains+"s-"+str(index)
    config = copy.deepcopy(config)
    if type in ['rocksdb', 'leveldb', 'lmdb']:
        config['create_if_missing'] = True
        if path != '':
            config['path'] = os.path.join(path, name)
        else:
            raise RuntimeError(f'"--database-path-prefix" expected for databases of type {type}')
    return { "name": name, "type": type, "config": config }


def gen_provider_spec(provider_id: int=0, pool=mochi.bedrock.spec.PoolSpec, databases: list=[]):
    return mochi.bedrock.spec.ProviderSpec(name=f'hepnos_{provider_id}',
                                           type='yokan', pool=pool,
                                           provider_id=provider_id,
                                           config={'databases': databases})


def gen_queue_provider_spec(provider_id: int=0, pool=mochi.bedrock.spec.PoolSpec):
    return mochi.bedrock.spec.ProviderSpec(name=f'hepnos_queues_{provider_id}',
                                           type='hqp', pool=pool,
                                           provider_id=provider_id,
                                           config={})


def gen_config(output: str='',
               address: str='na+sm',
               use_progress_xstream: bool=False,
               num_rpc_xstreams: int=0,
               num_rpc_pools: int=0,
               num_providers: int=1,
               num_dataset_databases: int=1,
               num_run_databases: int=1,
               num_subrun_databases: int=1,
               num_event_databases: int=1,
               num_product_databases: int=1,
               num_queue_providers: int=0,
               database_type: str='map',
               database_path_prefix: str='',
               ssg_group_file: str='hepnos.ssg',
               jx9: bool=False):
    total_num_databases = num_dataset_databases + \
                          num_run_databases + \
                          num_subrun_databases + \
                          num_event_databases + \
                          num_product_databases
    if total_num_databases < num_providers:
        raise ValueError('Total number of databases is smaller than the number of providers')
    if num_rpc_pools > num_rpc_xstreams:
        raise ValueError('Number of RPC pools exceeds number of RPC xstreams')
    if num_providers < num_rpc_pools:
        raise ValueError('Number of RPC pools exceeds the number of providers')
    # define the rool ProcSpec
    proc_spec = mochi.bedrock.spec.ProcSpec(margo=address)
    # add the yokan library
    proc_spec.libraries['yokan'] = 'libyokan-bedrock-module.so'
    # add the hqp library
    if num_queue_providers > 0:
        proc_spec.libraries['hqp'] = 'libhepnos-queue.so'
    # add SSG info
    proc_spec.ssg.add(name='hepnos', bootstrap='mpi', group_file=ssg_group_file,
                      pool=proc_spec.margo.argobots.pools[0],
                      swim=mochi.bedrock.spec.SwimSpec(disabled=True))
    # add the progress pool if needed
    if use_progress_xstream:
        progress_pool = proc_spec.margo.argobots.pools.add(
            name='__progress__', kind='fifo_wait', access='mpmc')
        proc_spec.margo.progress_pool = progress_pool
        progress_es = proc_spec.margo.argobots.xstreams.add(
            name='__progress__',
            scheduler=mochi.bedrock.spec.SchedulerSpec(type='basic_wait', pools=[progress_pool]))
    # add rpc pools
    rpc_pools = []
    for i in range(num_rpc_pools):
        rpc_pool = proc_spec.margo.argobots.pools.add(
            name=f'__rpc_{i}__', kind='fifo_wait', access='mpmc')
        rpc_pools.append(rpc_pool)
    if num_rpc_pools == 0:
        rpc_pools.append(proc_spec.margo.rpc_pool)
    # add rpc xstreams
    for i in range(num_rpc_xstreams):
        proc_spec.margo.argobots.xstreams.add(
            name=f'__rpc_{i}__',
            scheduler=mochi.bedrock.spec.SchedulerSpec(type='basic_wait', pools=[rpc_pools[i%len(rpc_pools)]]))
    # create databases
    dbs = []
    map_type = database_type
    set_type = 'set' if database_type == 'map' else database_type
    for i in range(num_dataset_databases):
        dbs.append(gen_database_spec('dataset', i, map_type, database_path_prefix))
    for i in range(num_run_databases):
        dbs.append(gen_database_spec('run', i, set_type, database_path_prefix))
    for i in range(num_subrun_databases):
        dbs.append(gen_database_spec('subrun', i, set_type, database_path_prefix))
    for i in range(num_event_databases):
        dbs.append(gen_database_spec('event', i, set_type, database_path_prefix))
    for i in range(num_product_databases):
        dbs.append(gen_database_spec('product', i, map_type, database_path_prefix))
    # add providers
    providers = []
    for i in range(num_providers):
        proc_spec.providers.append(gen_provider_spec(i, rpc_pools[i%len(rpc_pools)], []))
    # distribute databases to providers
    for i, db in enumerate(dbs):
        proc_spec.providers[i%num_providers].config['databases'].append(db)
    # queue providers
    j = num_providers
    for i in range(num_queue_providers):
        proc_spec.providers.append(gen_queue_provider_spec(i, rpc_pools[(i+j)%len(rpc_pools)]))
    # output result to file or to stdin
    json_config = proc_spec.to_json(indent=4)
    if jx9:
        file_content = "$pid = getpid();\n"
        path_elems = database_path_prefix.split('/')
        if path_elems[0] == '':
            del path_elems[0]
            path_elems[0] = '/' + path_elems[0]
        fp = path_elems[0]
        file_content += f'mkdir("{fp}");\n'
        del path_elems[0]
        for sub in path_elems:
            fp = os.path.join(fp, sub)
            file_content += f'mkdir("{fp}");\n'
        file_content += f"$config = {json_config};\nreturn $config;"
    else:
        file_content = json_config
    if output == '':
        print(file_content)
    else:
        with open(output, 'w+') as f:
            f.write(file_content)

