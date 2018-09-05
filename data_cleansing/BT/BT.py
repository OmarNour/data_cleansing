import data_cleansing.CONFIG.Config as DNXConfig
import datetime
from sqlalchemy import create_engine
import pandas as pd
import pymongo
import numpy as np
import sys
from data_cleansing.dc_methods.dc_methods import insert_into_collection, sha1, get_parameter_values,\
    delete_from_collection_where_ids,get_sub_data,data_to_list, get_start_end_index,chunk_list,df_to_dict, chunk_list_loop, get_minimum_category,\
    assing_process_no
from dask import delayed,compute
from pymongo import IndexModel
import os
from dask.diagnostics import ProgressBar
# import dask.multiprocessing
# dask.config.set(scheduler='processes')


class StartBt:
    def __init__(self):
        pd.set_option('mode.chained_assignment', None)
        self.dnx_config = DNXConfig.Config()
        self.parameters_dict = self.dnx_config.get_parameters_values()

    dnx_config = None
    parameters_dict = None
    cpu_num_workers = None
    all_etls_processes_done = False
    all_bt_inserts_processes_done = False
    all_bt_current_inserts_processes_done = False
    all_bt_current_deletes_processes_done = False

    parallel_etls = []
    parallel_data_manipulation = []
    parallel_bt_inserts = []
    parallel_bt_current_inserts = []
    parallel_deletes = []
    parallel_prepare_chunks_delete = []
    parallel_delete_chunk_ids_from_collection = []

    bt_columns = ['bt_id', 'SourceID', 'RowKey', 'AttributeID', 'BTSID', 'AttributeValue', 'RefSID',
                  'HashValue', 'InsertedBy', 'ModifiedBy', 'ValidFrom', 'ValidTo',
                  'IsCurrent', 'ResetDQStage', 'new_row']
    bt_columns_without_bt_id = ['SourceID', 'RowKey', 'AttributeID', 'BTSID', 'AttributeValue', 'RefSID',
                                'HashValue', 'InsertedBy', 'ModifiedBy', 'ValidFrom', 'ValidTo',
                                'IsCurrent', 'ResetDQStage', 'new_row']

    def get_data_from_source(self,url,schema, query):
        # print('\n',url,schema)
        if schema is None:
            schema = ""
        url_schema = url+schema
        engine = create_engine(url_schema)
        # Session.configure(bind=engine)
        connection = engine.connect()
        source_query = query
        results_proxy = connection.execute(source_query)
        return results_proxy

    # @delayed
    def melt_query_result(self,df_result,source_id):
        # melt_time = datetime.datetime.now()
        # df_result.columns = partial_results_keys
        # print('df_result.columns', df_result.head())
        df_melt_result = pd.melt(df_result, id_vars='rowkey', var_name='AttributeName', value_name='AttributeValue')
        df_melt_result.columns = ['RowKey', 'AttributeName', 'AttributeValue']
        df_melt_result['BTSID'] = 1
        df_melt_result['SourceID'] = source_id
        # df_melt_result['RowKey'] = df_melt_result['RowKey'].apply(sha1)
        df_melt_result['new_row'] = 1
        df_melt_result['RefSID'] = None
        df_melt_result['HashValue'] = df_melt_result['AttributeValue'].apply(sha1)
        df_melt_result['InsertedBy'] = 'ETL'
        df_melt_result['ModifiedBy'] = None
        df_melt_result['ValidFrom'] = datetime.datetime.now().isoformat()
        df_melt_result['ValidTo'] = None
        df_melt_result['IsCurrent'] = 1
        df_melt_result['bt_id'] = 0
        # df_melt_result['ResetDQStage'] = 0
        return df_melt_result

    def get_att_ids_df(self, be_data_source_id):
        client = pymongo.MongoClient(self.dnx_config.mongo_uri)
        config_db = client[self.dnx_config.config_db_name]
        data_sources_mapping = self.dnx_config.be_data_sources_mapping_collection

        data_sources_mapping_data = config_db[data_sources_mapping].find({'be_data_source_id': be_data_source_id}, {'_id': 0, 'query_column_name': 1, 'be_att_id': 1})
        data_sources_mapping_data_list = data_to_list(data_sources_mapping_data)
        att_ids_df = pd.DataFrame(data_sources_mapping_data_list)
        # print(att_ids_df)
        att_ids_df['ResetDQStage'] = att_ids_df.apply(lambda row: get_minimum_category(row['be_att_id']), axis=1)
        att_ids_df = att_ids_df.rename(index=str, columns={"query_column_name": "AttributeName", "be_att_id": "AttributeID"})
        # print(att_ids_df)
        return att_ids_df

    # @delayed
    def attach_attribute_id(self, att_query_df, melt_df):

        # print(att_query_df.columns)
        # print(melt_df.columns)
        new_rows_df = melt_df.merge(att_query_df, left_on='AttributeName',
                                    right_on='AttributeName',
                                    how='left')[self.bt_columns]

        new_rows_df = new_rows_df[new_rows_df['AttributeID'].notnull()]
        new_rows_df['AttributeID'] = new_rows_df.AttributeID.astype('int64')
        # new_rows_df['_id'] = new_rows_df['SourceID'].astype(str) + new_rows_df['AttributeID'].astype(str) + new_rows_df['RowKey']
        new_rows_df['bt_id'] = new_rows_df['SourceID'].astype(str) + new_rows_df['AttributeID'].astype(str) + new_rows_df['RowKey']
        bt_ids = data_to_list(new_rows_df['bt_id'])
        # SourceID ResetDQStage AttributeID RowKey
        # new_rows_df = new_rows_df.sort_values(by=['_id', 'SourceID', 'ResetDQStage', 'AttributeID', 'RowKey'], kind='mergesort')
        # new_rows_df = new_rows_df.sort_values(by=['_id'], kind='mergesort')
        return new_rows_df[self.bt_columns], bt_ids

    # @delayed
    def get_delta(self, source_df, p_current_df):
        etl_occurred = -1
        current_df = p_current_df
        # print(source_df.columns)
        # print(p_current_df.columns)
        if not current_df.empty:
            source_df = source_df.set_index(['bt_id'])
            current_df = current_df.set_index(['bt_id'])
            merge_df = source_df.merge(current_df,
                                       left_on=['bt_id'],
                                       right_on=['bt_id'],
                                       suffixes=('_new', '_cbt'),
                                       how='left'
                                       )
            merge_df = merge_df.reset_index()

            # print(merge_df.columns, merge_df['SourceID_cbt'])
            new_data_df = merge_df.loc[(merge_df['SourceID_cbt'].isnull())]
            new_data_df = new_data_df[['bt_id', 'SourceID_new', 'RowKey_new', 'AttributeID_new', 'BTSID_new',
                                       'AttributeValue_new', 'RefSID_new', 'HashValue_new', 'InsertedBy_new',
                                       'ModifiedBy_new', 'ValidFrom_new', 'ValidTo_new', 'IsCurrent_new',
                                       'ResetDQStage_new',
                                       'new_row_new']]
            new_data_df.columns = self.bt_columns

            merge_df = merge_df.loc[(merge_df['SourceID_cbt'].notnull()) &
                                    (merge_df['HashValue_new'].notnull())]

            bt_modified_expired = merge_df.loc[(merge_df['HashValue_cbt'] != merge_df['HashValue_new'])]

            bt_modified_expired[['ValidTo_cbt', 'IsCurrent_cbt', 'ModifiedBy_cbt']] = \
                [datetime.datetime.now().isoformat(), 0, 'ETL']

            bt_modified_df = bt_modified_expired[['bt_id', 'SourceID_new', 'RowKey_new', 'AttributeID_new', 'BTSID_new',
                                                  'AttributeValue_new', 'RefSID_new', 'HashValue_new', 'InsertedBy_new',
                                                  'ModifiedBy_new', 'ValidFrom_new', 'ValidTo_new', 'IsCurrent_new',
                                                  'ResetDQStage_new',
                                                  'new_row_new']]
            bt_modified_df.columns = self.bt_columns
            # print(bt_modified_df.index)

            bt_expired_data_df = bt_modified_expired[
                ['bt_id', 'SourceID_cbt', 'RowKey_cbt', 'AttributeID_cbt', 'BTSID_cbt',
                 'AttributeValue_cbt', 'RefSID_cbt', 'HashValue_cbt', 'InsertedBy_cbt',
                 'ModifiedBy_cbt', 'ValidFrom_cbt', 'ValidTo_cbt', 'IsCurrent_cbt', 'ResetDQStage_cbt',
                 'new_row_cbt']]

            bt_expired_data_df.columns = self.bt_columns
            expired_ids = bt_modified_expired[['_id']]
            # print(bt_expired_data_df.index)
        else:
            bt_expired_data_df = pd.DataFrame()
            bt_modified_df = pd.DataFrame()
            new_data_df = source_df
            expired_ids = []

        # sort_by = ['_id', 'SourceID', 'ResetDQStage', 'AttributeID', 'RowKey']
        if len(bt_modified_df.index) > 0 and len(new_data_df.index) > 0:
            # new_data_df = new_data_df.sort_values(by=sort_by, kind='mergesort')
            # bt_modified_df = bt_modified_df.sort_values(by=sort_by, kind='mergesort')
            etl_occurred = 2
        elif len(new_data_df.index) > 0:
            # new_data_df = new_data_df.sort_values(by=sort_by, kind='mergesort')
            etl_occurred = 1
        elif len(bt_modified_df.index) > 0:
            # bt_modified_df = bt_modified_df.sort_values(by=sort_by, kind='mergesort')
            etl_occurred = 0
        # print('---------- bt_modified_df', len(bt_modified_df.index))
        # print('---------- bt_expired_data_df', len(bt_expired_data_df.index))
        # print('---------- bt_same_data_df', len(bt_same_data_df.index))
        # print('---------- new_data_df', len(new_data_df.index))
        # print('---------- current_df', len(current_df.index))

        # print('get_delta time:',datetime.datetime.now() - merge_time)
        # print('$$$$$$$$$$$$$$$$$$$$$$ get_delta End $$$$$$$$$$$$$$$$$$')
        # print('bt_modified_expired', bt_modified_expired.columns)
        # print('expired_ids', expired_ids)
        print('---------- source df', 'p_current_df', 'bt_modified_df, new_data_df:',
              len(source_df.index), len(p_current_df.index), len(bt_modified_df.index), ',', len(new_data_df.index))
        return bt_modified_df, bt_expired_data_df, new_data_df, etl_occurred, expired_ids

    def load_data(self, p_source_data, p_current_data, bt_collection, bt_current_collection):

        get_delta_result = self.get_delta(p_source_data, p_current_data)

        if get_delta_result[3] in (0,2): #etl_occurred
            assert len(get_delta_result[0]) == len(get_delta_result[1])

            modified_df = delayed(get_delta_result[0])
            expired_df = delayed(get_delta_result[1])
            expired_ids = delayed(get_delta_result[4])

            manipulate = delayed(self.manipulate_etl_data)(bt_collection, expired_df, expired_ids, bt_current_collection)  # expired data
            self.parallel_data_manipulation.append(manipulate)

            manipulate = delayed(self.manipulate_etl_data)(bt_current_collection, modified_df)  # modified data
            self.parallel_data_manipulation.append(manipulate)

        if get_delta_result[3] in (1, 2):  # etl_occurred
            new_data_df = delayed(get_delta_result[2])
            manipulate = delayed(self.manipulate_etl_data)(bt_current_collection, new_data_df)  # new data
            self.parallel_data_manipulation.append(manipulate)

    def delete_chunks_of_data(self,chunks,from_collection):
        for i in chunks:
            delete = delayed(delete_from_collection_where_ids)(self.dnx_config.mongo_uri, self.dnx_config.dnx_db_name, from_collection, delayed(i))
            self.parallel_deletes.append(delete)

    def delete_chunk_ids_from_collection(self, collection_name, ids):
        list_ids = delayed(ids)
        chunks = delayed(chunk_list)(list_ids,int(self.parameters_dict['delete_operations_batch_size']))
        # chunks = delayed(chunk_list)(list_ids, 10000)
        chunks_delete = delayed(self.delete_chunks_of_data)(chunks,collection_name)

        self.parallel_prepare_chunks_delete.append(chunks_delete)

    def manipulate_etl_data(self, into_collection, data_df, delete_ids=pd.DataFrame(), delete_from=None):
        for df in chunk_list(data_df, int(self.parameters_dict['insert_operations_batch_size'])):
            data_dict = delayed(df_to_dict)(df)
            insert = delayed(insert_into_collection)(self.dnx_config.mongo_uri, self.dnx_config.dnx_db_name, into_collection, data_dict)
            if not delete_ids.empty:
                self.parallel_bt_inserts.append(insert)
            else:
                self.parallel_bt_current_inserts.append(insert)
        # print('delete_ids', type(delete_ids))
        if not delete_ids.empty: # modified or same data
            expired_ids = delayed(self.get_expired_ids)(delete_ids)
            # print(expired_ids)
            ids = delayed(data_to_list)(expired_ids)
            self.delete_chunk_ids_from_collection(delete_from, ids)

    def prepare_source_df(self, partial_results, partial_results_keys, row_key_column_name, process_no_column_name, no_of_cores):
        partial_results_df = pd.DataFrame(partial_results, columns=partial_results_keys)
        partial_results_df[row_key_column_name] = partial_results_df[row_key_column_name].apply(sha1)
        partial_results_df['_id'] = partial_results_df[row_key_column_name]
        partial_results_df[process_no_column_name] = partial_results_df.apply(lambda x: assing_process_no(no_of_cores, x.name), axis=1)
        return partial_results_df

    def load_source_data(self, no_of_cores=1):
        process_no_column_name = self.dnx_config.process_no_column_name
        client = pymongo.MongoClient(self.dnx_config.mongo_uri)
        config_database = client[self.dnx_config.config_db_name]
        source_database = client[self.dnx_config.src_db_name]
        be_att_ids = config_database[self.dnx_config.be_attributes_data_rules_lvls_collection].find({'active': 1}).distinct('be_att_id')
        be_ids = config_database[self.dnx_config.be_attributes_collection].find({'_id': {'$in': be_att_ids}}).distinct('be_id')
        parallel_load_source_data = []
        for be_id in be_ids:
            org_id = config_database[self.dnx_config.org_business_entities_collection].find_one({'_id': be_id})['org_id']
            mapping_be_source_ids = config_database[self.dnx_config.be_data_sources_mapping_collection].find().distinct('be_data_source_id')
            be_source_ids = config_database[self.dnx_config.be_data_sources_collection].find(
                {'be_id': be_id, 'active': 1, '_id': {'$in': mapping_be_source_ids}}).distinct('_id')
            be_source_ids.sort()
            bt_current_collection = config_database[self.dnx_config.org_business_entities_collection].find_one({'_id': be_id})['bt_current_collection']
            source_collection = config_database[self.dnx_config.org_business_entities_collection].find_one({'_id': be_id})['source_collection']
            be_att_id = config_database[self.dnx_config.be_attributes_collection].find_one({'be_id': be_id, 'att_id': 0})['_id']
            source_database[source_collection].drop()
            self.create_bt_indexes(bt_current_collection,None)
            for source_id in be_source_ids:
                print('Start loading data for business entity', be_id, 'from source', source_id)
                source_query = config_database[self.dnx_config.be_data_sources_collection].find_one({'_id': source_id})['query']
                org_source_id = config_database[self.dnx_config.be_data_sources_collection].find_one({'_id': source_id})['org_connection_id']

                source_url = config_database[self.dnx_config.org_connections_collection].find_one({'_id': org_source_id, 'org_id': org_id})['url']
                source_schema = config_database[self.dnx_config.org_connections_collection].find_one({'_id': org_source_id, 'org_id': org_id})['schema']
                row_key_column_name = config_database[self.dnx_config.be_data_sources_mapping_collection].find_one({'be_data_source_id': source_id,
                                                                                                                    'be_att_id': be_att_id})[
                    'query_column_name']

                source_query_results_proxy = self.get_data_from_source(source_url, source_schema, source_query)
                more_results_from_source = True
                while more_results_from_source:
                    partial_results = source_query_results_proxy.fetchmany(int(self.parameters_dict['source_batch_size']))
                    if not partial_results:
                        more_results_from_source = False
                    else:
                        partial_results_keys = partial_results[0].keys()
                        partial_results_df = delayed(self.prepare_source_df)(partial_results, partial_results_keys, row_key_column_name,
                                                                             process_no_column_name,
                                                                             no_of_cores)
                        partial_results_dict = delayed(df_to_dict)(partial_results_df)
                        delayed_insert_into_collection = delayed(insert_into_collection)(self.dnx_config.mongo_uri, self.dnx_config.src_db_name,
                                                                                         source_collection, partial_results_dict)
                        parallel_load_source_data.append(delayed_insert_into_collection)
        compute(*parallel_load_source_data, num_workers=self.dnx_config.cpu_count)

    def get_source_data(self, source_id, source_collection, process_no):

        client = pymongo.MongoClient(self.dnx_config.mongo_uri)
        source_database = client[self.dnx_config.src_db_name]
        collection_filter = {'process_no': process_no}
        count_rows = source_database[source_collection].find(collection_filter).count()
        att_query_df = self.get_att_ids_df(source_id)
        collection_project = {'_id': 0}
        # print('count_rows', count_rows)
        # print('collection_filter', collection_filter)
        for i in get_start_end_index(count_rows, int(self.parameters_dict['temp_source_batch_size'])):
            # print(i[0], i[1])
            sub_source_data = delayed(get_sub_data)(self.dnx_config.mongo_uri, self.dnx_config.src_db_name,
                                                    source_collection, i[0], i[1], None, collection_filter, collection_project)
            list_sub_source_data = delayed(data_to_list)(sub_source_data)
            sub_source_data_df = delayed(pd.DataFrame)(list_sub_source_data)
            melt_sub_source_data_df = delayed(self.melt_query_result)(sub_source_data_df, source_id)
            source_data_df = delayed(self.attach_attribute_id)(att_query_df, melt_sub_source_data_df)
            yield source_data_df[0], source_data_df[1]

    def get_expired_ids(self, sorce_df):
        expired_ids = sorce_df['_id']
        return expired_ids

    def get_cpu_num_workers(self, process_no):
        client = pymongo.MongoClient(self.dnx_config.mongo_uri)
        config_db = client[self.dnx_config.config_db_name]
        cpu_num_workers = config_db[self.dnx_config.multiprocessing_collection].find_one({'p_no': process_no})['cpu_num_workers']
        return cpu_num_workers

    def maintain_multiprocessing_collection(self, process_no, parallel_process_name):
        client = pymongo.MongoClient(self.dnx_config.mongo_uri)
        config_db = client[self.dnx_config.config_db_name]
        if process_no is not None:
            config_db[self.dnx_config.multiprocessing_collection].update_one({self.dnx_config.multiprocessing_p_no: process_no},
                                                                             {'$set': {parallel_process_name: 0}})

        sum_field = '$'+parallel_process_name
        all_processes_status = config_db[self.dnx_config.multiprocessing_collection].aggregate([{'$group': {'_id': 'null',
                                                                                                            'all_processes': {'$sum': sum_field}}},
                                                                                                {'$project': {'_id': 0, 'all_processes': 1}}])
        for i in all_processes_status:
            if i['all_processes'] == 0:
                if parallel_process_name == self.dnx_config.multiprocessing_etl:
                    self.all_etls_processes_done = True
                elif parallel_process_name == self.dnx_config.multiprocessing_bt_inserts:
                    self.all_bt_inserts_processes_done = True
                elif parallel_process_name == self.dnx_config.multiprocessing_bt_current_inserts:
                    self.all_bt_current_inserts_processes_done = True
                elif parallel_process_name == self.dnx_config.multiprocessing_bt_current_deletes:
                    self.all_bt_current_deletes_processes_done = True

    def etl_be(self, source_id, bt_current_collection, bt_collection, source_collection, process_no):
        for source_data_df, ids in self.get_source_data(source_id, source_collection, process_no):

            collection_filter = {'bt_id': {'$in': ids}}
            # collection_projection = {'_id': 0}
            collection_projection = None
            bt_current_data = delayed(get_sub_data)(self.dnx_config.mongo_uri, self.dnx_config.dnx_db_name, bt_current_collection,
                                                    0, 0, None, collection_filter, collection_projection)
            list_bt_current_data = delayed(data_to_list)(bt_current_data)
            bt_current_data_df = delayed(pd.DataFrame)(list_bt_current_data)

            etl_delayed = delayed(self.load_data)(source_data_df, bt_current_data_df,
                                                  bt_collection,
                                                  bt_current_collection)

            self.parallel_etls.append(etl_delayed)

        print('-----------------------------------------------------------')
        # org_id_be_id = str(org_id)+'_'+str(be_id)
        if self.parallel_etls:
            print('Process_no:', process_no, 'Executing', len(self.parallel_etls),
                  'parallel ETLs for:', bt_current_collection)
            compute(*self.parallel_etls, num_workers=self.get_cpu_num_workers(process_no))
            # compute(*self.parallel_etls)
        self.maintain_multiprocessing_collection(process_no, self.dnx_config.multiprocessing_etl)

        if self.parallel_data_manipulation:
            compute(*self.parallel_data_manipulation, num_workers=self.get_cpu_num_workers(process_no))
            # compute(*self.parallel_data_manipulation)
            self.parallel_data_manipulation = []

        if self.parallel_bt_current_inserts:
            print('Process_no:', process_no, 'Executing', len(self.parallel_bt_current_inserts),
                  'parallel inserts into:', bt_current_collection)
            compute(*self.parallel_bt_current_inserts, num_workers=self.get_cpu_num_workers(process_no))
            # compute(*self.parallel_bt_current_inserts)
            self.parallel_bt_current_inserts = []
        self.maintain_multiprocessing_collection(process_no, self.dnx_config.multiprocessing_bt_current_inserts)

        if self.parallel_bt_inserts:
            print('Process_no:', process_no, 'Executing', len(self.parallel_bt_inserts),
                  'parallel inserts into:', bt_collection)
            compute(*self.parallel_bt_inserts, num_workers=self.get_cpu_num_workers(process_no))
            # compute(*self.parallel_bt_inserts)
            self.parallel_bt_inserts = []
        self.maintain_multiprocessing_collection(process_no, self.dnx_config.multiprocessing_bt_inserts)

        if self.parallel_prepare_chunks_delete:
            compute(*self.parallel_prepare_chunks_delete, num_workers=self.get_cpu_num_workers(process_no))
            # compute(*self.parallel_prepare_chunks_delete)
            self.parallel_prepare_chunks_delete = []

        # print('self.all_bt_current_inserts_processes_done', self.all_bt_current_inserts_processes_done)
        while not self.all_bt_current_inserts_processes_done:
            self.maintain_multiprocessing_collection(None, self.dnx_config.multiprocessing_bt_current_inserts)

        if self.parallel_deletes:
            print('Process_no:', process_no, 'Executing', len(self.parallel_deletes),
                  'parallel deletes from:', bt_current_collection)
            compute(*self.parallel_deletes, num_workers=self.get_cpu_num_workers(process_no))
            # compute(*self.parallel_deletes)
            self.parallel_deletes = []
        self.maintain_multiprocessing_collection(process_no, self.dnx_config.multiprocessing_bt_current_deletes)

    def create_bt_indexes(self, bt_current_collection,bt_collection):
        print('start create indexes for', bt_current_collection)
        create_index_time = datetime.datetime.now()
        client = pymongo.MongoClient(self.dnx_config.mongo_uri)
        dnx_database = client[self.dnx_config.dnx_db_name]
        # result_database = client[self.dnx_config.result_db_name]
        # try:
        #     bt_index1 = IndexModel([("SourceID", 1), ('RowKey', 1), ('AttributeID', 1)], name='index1')
        #     dnx_database[bt_collection].create_indexes([bt_index1])
        # except:
        #     print(dnx_database[bt_collection].index_information())
        try:
            bt_id_index = IndexModel([("bt_id", pymongo.ASCENDING)], name='bt_id_index')
            # bt_current_index1 = IndexModel([("SourceID", 1), ("ResetDQStage", 1), ('AttributeID', 1), ('RowKey', 1)], name='index1')
            # bt_current_index2 = IndexModel([("SourceID", 1), ("ResetDQStage", 1), ('AttributeID', 1)], name='index2')
            dnx_database[bt_current_collection].create_indexes([bt_id_index, ])
        except:
            print("Unexpected error:", sys.exc_info()[0])
            # dnx_database[bt_current_collection].reindex()
        print('create_index_time:', datetime.datetime.now() - create_index_time)

    def start_bt(self,process_no, cpu_num_workers):
        client = pymongo.MongoClient(self.dnx_config.mongo_uri)
        config_database = client[self.dnx_config.config_db_name]

        be_att_ids = config_database[self.dnx_config.be_attributes_data_rules_lvls_collection].find({'active': 1}).distinct('be_att_id')
        be_ids = config_database[self.dnx_config.be_attributes_collection].find({'_id': {'$in': be_att_ids}}).distinct('be_id')
        mapping_be_source_ids = config_database[self.dnx_config.be_data_sources_mapping_collection].find().distinct('be_data_source_id')
        for be_id in be_ids:
            # print(be_id)
            bt_current_collection = config_database[self.dnx_config.org_business_entities_collection].find_one({'_id': be_id})['bt_current_collection']
            bt_collection = config_database[self.dnx_config.org_business_entities_collection].find_one({'_id': be_id})['bt_collection']
            source_collection = config_database[self.dnx_config.org_business_entities_collection].find_one({'_id': be_id})['source_collection']
            # org_id = config_database[self.dnx_config.org_business_entities_collection].find_one({'_id': be_id})['org_id']

            # self.create_bt_indexes(bt_current_collection, bt_collection)

            be_source_ids = config_database[self.dnx_config.be_data_sources_collection].find({'be_id': be_id, 'active': 1, '_id': {'$in': mapping_be_source_ids}}).distinct('_id')
            # be_source_ids.sort()
            for source_id in be_source_ids:
                # query = config_database[self.dnx_config.be_data_sources_collection].find_one({'_id': source_id})['query']
                # org_source_id = config_database[self.dnx_config.be_data_sources_collection].find_one({'_id': source_id})['org_connection_id']

                # source_url = config_database[self.dnx_config.org_connections_collection].find_one({'_id': org_source_id, 'org_id': org_id})['url']
                # source_schema = config_database[self.dnx_config.org_connections_collection].find_one({'_id': org_source_id, 'org_id': org_id})['schema']

                self.etl_be(source_id, bt_current_collection,bt_collection, source_collection, process_no)


