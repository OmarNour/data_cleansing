import sys, os
sys.path.append("C:\\Users\\Omar\\PycharmProjects\\data_cleansing")
import subprocess
import multiprocessing
from data_cleansing.build_configuration_schema.config_schema import build_config_db
import pymongo
import data_cleansing.CONFIG.Config as DNXConfig
import datetime
import math


def get_cpu_count_cpu_num_workers(config_database, parameters_collection, no_of_subprocess=None):
    if no_of_subprocess is None:
        no_of_subprocess = int(config_database[parameters_collection].find_one({'_id': 'no_of_subprocess'})['value'])

    server_cpu_count = multiprocessing.cpu_count()
    if 0 < no_of_subprocess <= server_cpu_count:
        cpu_count = no_of_subprocess
    else:
        cpu_count = server_cpu_count

    if cpu_count == server_cpu_count:
        cpu_num_workers = 1
    else:
        cpu_num_workers = math.floor(server_cpu_count / cpu_count)

    print('no_of_subprocess', no_of_subprocess)
    print('server_cpu_count', server_cpu_count)
    print('cpu_count', cpu_count)
    print('cpu_num_workers', cpu_num_workers)

    return cpu_count, cpu_num_workers

if __name__ == '__main__':
    build_config_db()
    bt_process_dict = {}
    dq_process_dict = {}

    dnx_config = DNXConfig.Config()
    client = pymongo.MongoClient(dnx_config.mongo_uri)
    config_database = client[dnx_config.config_db_name]

    module_path = os.path.dirname(sys.modules['__main__'].__file__)

    run_time = datetime.datetime.now()

    result = get_cpu_count_cpu_num_workers(config_database, dnx_config.parameters_collection, no_of_subprocess=None)
    bt_cpu_count, bt_cpu_num_workers = result[0], result[1]

    result = get_cpu_count_cpu_num_workers(config_database, dnx_config.parameters_collection, no_of_subprocess=1)
    dq_cpu_count, dq_cpu_num_workers = result[0], result[1]

    run_engine_data = config_database[dnx_config.run_engine_collection].find({'start_time': ''})

    for i in run_engine_data:
        BT = i['BT']
        DQ = i['DQ']

        if BT == 1:
            loading_source_data = subprocess.Popen(['python',
                                                    module_path + '/load_source_data/load_source_data.py',
                                                    str(bt_cpu_count)])

            while loading_source_data.poll() is None:
                None

            config_database[dnx_config.multiprocessing_collection].drop()

            for p in range(bt_cpu_count):
                process_no = str(p)
                config_database[dnx_config.multiprocessing_collection].insert_one({dnx_config.multiprocessing_p_no: p,
                                                                                   dnx_config.multiprocessing_cpu_num_workers: bt_cpu_num_workers,
                                                                                   dnx_config.multiprocessing_etl: 1,
                                                                                   dnx_config.multiprocessing_bt_inserts: 1,
                                                                                   dnx_config.multiprocessing_bt_current_inserts: 1,
                                                                                   dnx_config.multiprocessing_bt_current_deletes: 1,
                                                                                   dnx_config.multiprocessing_process_alive: 1})
                bt_process_dict[process_no] = subprocess.Popen(['python',
                                                                module_path + '/run_engine.py',
                                                                process_no,
                                                                str(BT),
                                                                str(0),
                                                                str(bt_cpu_num_workers)])

            count_finished_processes = 0
            process_list = []
            for p_no in range(bt_cpu_count):
                process_list.append(p_no)

            while process_list:
                for p_no in range(bt_cpu_count):
                    if bt_process_dict[str(p_no)].poll() is not None:
                        try:
                            process_list.remove(p_no)
                            count_finished_processes += 1
                            config_database[dnx_config.multiprocessing_collection].update_one({dnx_config.multiprocessing_p_no: p_no},
                                                                                              {'$set': {dnx_config.multiprocessing_process_alive: 0}})
                            print('-----------------------------------------------------------')
                            print('BT Process no.', p_no, 'finished, total finished', count_finished_processes, 'out of', bt_cpu_count)

                        except:
                            None

            # 65,010,912 bt current
        if DQ == 1:
            for p in range(dq_cpu_count):
                process_no = str(p)
                dq_process_dict[process_no] = subprocess.Popen(['python',
                                                                module_path + '/run_engine.py',
                                                                process_no,
                                                                str(0),
                                                                str(DQ),
                                                                str(dq_cpu_num_workers)])

            count_finished_processes = 0
            process_list = []
            for p_no in range(dq_cpu_count):
                process_list.append(p_no)

            while process_list:
                for p_no in range(dq_cpu_count):
                    if dq_process_dict[str(p_no)].poll() is not None:
                        try:
                            process_list.remove(p_no)
                            count_finished_processes += 1
                            print('-----------------------------------------------------------')
                            print('DQ Process no.', p_no, 'finished, total finished', count_finished_processes, 'out of', bt_cpu_count)

                        except:
                            None

        config_database[dnx_config.run_engine_collection].update_one({'_id': i['_id']}, {'$set': {'end_time': datetime.datetime.now()}})
        print('####################     total time:', datetime.datetime.now() - run_time, '      ####################')

