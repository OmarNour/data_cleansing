import data_cleansing.CONFIG.Config as DNXConfig
import datetime
import pandas as pd
import pymongo
import data_cleansing.DQ.Data_Rules.Data_Rules as Dr
from data_cleansing.dc_methods.dc_methods import insert_into_collection, df_to_dict, data_to_list, get_sub_data, get_start_end_index, chunk_list_loop
from dask import delayed,compute
from pymongo import IndexModel
from dask.diagnostics import ProgressBar


class StartDQ:
    parallel_upgrade_preparation = []
    dnx_config = None
    parameters_dict = None

    def get_data_value_pattern(self, att_value):
        try:
            float_att_value = float(att_value)
            return '9' * len(str(float_att_value))
        except:
            return 'A' * len(str(att_value))

    def prepare_result_df(self,bt_current_data_df, be_att_dr_id, data_rule_id):
        result_df = bt_current_data_df[['SourceID', 'RowKey', 'AttributeID', 'AttributeValue', 'ResetDQStage']]
        result_df['be_att_dr_id'] = be_att_dr_id
        result_df['data_rule_id'] = data_rule_id
        result_df['is_issue'] = result_df.apply(lambda x: Dr.rules_orchestrate(x['AttributeValue'], data_rule_id), axis=1)
        result_df['data_value_pattern'] = result_df['AttributeValue'].apply(self.get_data_value_pattern)
        return result_df

    def insert_result_df(self,result_df, g_result, result_collection, next_pass, next_fail, tmp_result_collection_name):
        if g_result == 1:
            result_dict = df_to_dict(result_df)
            insert_into_collection(self.dnx_config.mongo_uri, self.dnx_config.result_db_name, result_collection, result_dict)
        else:
            if next_pass == 1:
                next_df = result_df[result_df['is_issue'] == 0][['RowKey']]
            elif next_fail == 1:
                next_df = result_df[result_df['is_issue'] == 1][['RowKey']]
            else:
                next_df = pd.DataFrame()
            if not next_df.empty:
                next_df = next_df.rename(index=str, columns={"RowKey": "_id"})
                next_dict = df_to_dict(next_df)
                insert_into_collection(self.dnx_config.mongo_uri, self.dnx_config.tmp_result_db_name, tmp_result_collection_name, next_dict)

    def validate_data_rules(self, bt_current_collection_name, result_collection, source_id, be_att_dr_id, category_no, attribute_id,
                            data_rule_id, g_result, current_lvl_no, next_pass, next_fail):
        client = pymongo.MongoClient(self.dnx_config.mongo_uri)
        dnx_database = client[self.dnx_config.dnx_db_name]
        tmp_result_database = client[self.dnx_config.tmp_result_db_name]
        tmp_result_collection_name = self.dnx_config.tmp_result_db_name+'_'+str(be_att_dr_id)

        collection_filter = {'SourceID': source_id,
                             'ResetDQStage': category_no,
                             'AttributeID': attribute_id}
        collection_filters = [collection_filter, ]

        if current_lvl_no > 1:
            collection_filters = []
            count_row_keys = tmp_result_database[tmp_result_collection_name].find().count()
            if count_row_keys > 0:
                for i in get_start_end_index(count_row_keys, int(self.parameters_dict['bt_batch_size'])):
                    sub_row_keys = get_sub_data(self.dnx_config.mongo_uri, self.dnx_config.tmp_result_db_name,
                                                tmp_result_collection_name, i[0], i[1])
                    list_sub_row_keys = data_to_list(sub_row_keys)
                    sub_row_keys_df = pd.DataFrame(list_sub_row_keys)
                    row_keys = data_to_list(sub_row_keys_df['_id'])
                    collection_filter['RowKey'] = {'$in': row_keys}
                    collection_filters.append(collection_filter)
                tmp_result_database[tmp_result_collection_name].drop()

        collection_project = {"_id": 0}

        # print('bt_current_data_count', bt_current_data_count)
        # print('collection_filter', collection_filter)
        parallel_insert_result_df = []
        for curs_collection_filter in collection_filters:
            bt_current_data_count = dnx_database[bt_current_collection_name].find(curs_collection_filter).count()
            if bt_current_data_count > 0:
                for i in get_start_end_index(bt_current_data_count, int(self.parameters_dict['bt_batch_size'])):
                    bt_current_data = delayed(get_sub_data)(self.dnx_config.mongo_uri, self.dnx_config.dnx_db_name,
                                                            bt_current_collection_name, i[0], i[1], None, curs_collection_filter, collection_project)
                    list_bt_current_data = delayed(data_to_list)(bt_current_data)
                    bt_current_data_df = delayed(pd.DataFrame)(list_bt_current_data)
                    result_df = delayed(self.prepare_result_df)(bt_current_data_df, be_att_dr_id, data_rule_id)
                    delayed_insert_result_df = delayed(self.insert_result_df)(result_df, g_result, result_collection, next_pass, next_fail,
                                                                              tmp_result_collection_name)
                    parallel_insert_result_df.append(delayed_insert_result_df)
        compute(*parallel_insert_result_df)

    def get_next_be_att_id_category(self, be_att_id, current_category):
        client = pymongo.MongoClient(self.dnx_config.mongo_uri)
        config_database = client[self.dnx_config.config_db_name]
        be_categories_list = config_database[self.dnx_config.be_attributes_data_rules_collection].find({'be_att_id': be_att_id}).distinct('category_no')
        be_categories_list.sort()
        try:
            next_category = be_categories_list[be_categories_list.index(current_category)+1]
        except:
            next_category = current_category + 1
        # print('categories_list', be_att_id, current_category, categories_list, next_category)
        return next_category

    def reset_category(self):
        None

    def reset_last_category_flag(self):
        None

    def update_bt_current_collection(self, bt_current_collection, update_filter, update_set):
        client = pymongo.MongoClient(self.dnx_config.mongo_uri)
        dnx_database = client[self.dnx_config.dnx_db_name]
        dnx_database[bt_current_collection].update_many(update_filter,
                                                        update_set)

    def prepare_update_bt_current_collection(self, passed_rowkeys, source_id, current_category, be_att_id, next_category_no,
                                             bt_current_collection):
        # parallel_upgrade_preparation = []
        for chunk in chunk_list_loop(passed_rowkeys, int(self.parameters_dict['bt_batch_size'])):
            update_filter = {'SourceID': source_id,
                             'ResetDQStage': current_category,
                             'AttributeID': be_att_id,
                             'RowKey': {'$in': chunk}}
            update_set = {'$set': {'ResetDQStage': next_category_no}}
            delayed_update_bt_current_collection = delayed(self.update_bt_current_collection)(bt_current_collection, update_filter, update_set)
            self.parallel_upgrade_preparation.append(delayed_update_bt_current_collection)
        # compute(*parallel_upgrade_preparation)

    def get_rowkeys(self,df):
        rowkeys = df['RowKey']
        return rowkeys

    def upgrade_rowkeys_category(self, be_att_id, current_category, source_id):
        # print('start upgrade_rowkeys_category')
        client = pymongo.MongoClient(self.dnx_config.mongo_uri)
        config_database = client[self.dnx_config.config_db_name]
        result_database = client[self.dnx_config.result_db_name]

        # 3- get be_id from be_attributes where _id = be_att_id into be_id
        be_id = config_database[self.dnx_config.be_attributes_collection].find_one({'_id': be_att_id})['be_id']

        # 4- get bt_current_collection name from org_business_entities where _id == be_id into bt_current_collection
        bt_current_collection = config_database[self.dnx_config.org_business_entities_collection].find_one({'_id': be_id})['bt_current_collection']
        dq_result_collection = config_database[self.dnx_config.org_business_entities_collection].find_one({'_id': be_id})['dq_result_collection']

        # 5- get _id from be_attributes_data_rules where be_att_id = be_att_id into be_att_dr_ids
        be_att_dr_ids = config_database[self.dnx_config.be_attributes_data_rules_collection].find(
            {'be_att_id': be_att_id, 'category_no': current_category}).distinct('_id')

        # 6- select rowkeys from result collection be_att_dr_id in be_att_dr_ids and ResetDQStage == current_category into pd_rowkeys
        collection_filter = {'be_att_dr_id': {'$in': be_att_dr_ids}, 'is_issue': 0}
        projection = {'_id': 0,'RowKey': 1}
        # passed_rowkeys = result_database[dq_result_collection].find(collection_filter).distinct('RowKey')
        count_passed_rowkeys = result_database[dq_result_collection].find(collection_filter).count()
        # print('==================', current_category, passed_rowkeys, be_id, be_att_id, be_att_dr_ids)

        # 7- if issues_in_category == 0 then get next category no and update ResetDQStage in BT_current collection where AttributeID = be_att_id
        if count_passed_rowkeys > 0:
            next_category_no = self.get_next_be_att_id_category(be_att_id, current_category)
            parallel_update_bt_current_collection = []
            self.parallel_upgrade_preparation = []
            for i in get_start_end_index(count_passed_rowkeys,int(self.parameters_dict['bt_batch_size'])):
                sub_passed_rowkeys = delayed(get_sub_data)(self.dnx_config.mongo_uri, self.dnx_config.result_db_name,
                                                           dq_result_collection, i[0], i[1], None, collection_filter, projection)
                sub_passed_rowkeys_list = delayed(data_to_list)(sub_passed_rowkeys)
                sub_passed_rowkeys_df = delayed(pd.DataFrame)(sub_passed_rowkeys_list)
                passed_rowkeys = delayed(self.get_rowkeys)(sub_passed_rowkeys_df)
                list_passed_rowkeys = delayed(data_to_list)(passed_rowkeys)
                # print('passed_rowkeys found')
                delayed_parallel_update_bt_current_collection = delayed(self.prepare_update_bt_current_collection)(list_passed_rowkeys, source_id,
                                                                                                                   current_category, be_att_id,
                                                                                                                   next_category_no,
                                                                                                                   bt_current_collection)
                parallel_update_bt_current_collection.append(delayed_parallel_update_bt_current_collection)
            compute(*parallel_update_bt_current_collection)
            compute(*self.parallel_upgrade_preparation)

    def upgrade_category(self, current_category):
        client = pymongo.MongoClient(self.dnx_config.mongo_uri)
        config_database = client[self.dnx_config.config_db_name]
        # 1- select distinct be_att_id from be_attributes_data_rules where category_no = current_category into be_att_ids
        be_att_dr_ids = config_database[self.dnx_config.be_attributes_data_rules_collection].find({'category_no': current_category}).distinct('_id')
        # 2- loop over be_att_ids
        # print('be_att_ids', be_att_ids)
        parallel_upgrade_rowkeys_category = []
        for be_att_dr_id in be_att_dr_ids:
            be_att_id = config_database[self.dnx_config.be_attributes_data_rules_collection].find_one({'_id': be_att_dr_id})['be_att_id']
            be_data_source_id = config_database[self.dnx_config.be_attributes_data_rules_collection].find_one({'_id': be_att_dr_id})['be_data_source_id']
            delayed_upgrade_rowkeys_category = delayed(self.upgrade_rowkeys_category)(be_att_id, current_category, be_data_source_id)
            parallel_upgrade_rowkeys_category.append(delayed_upgrade_rowkeys_category)
        with ProgressBar():
            print('upgrade from category', current_category)
            compute(*parallel_upgrade_rowkeys_category)

    def execute_data_rules(self, data_rule, category_no):
        # data from self.dnx_config.be_attributes_data_rules_collection
        client = pymongo.MongoClient(self.dnx_config.mongo_uri)
        config_database = client[self.dnx_config.config_db_name]
        # print(data_rule)
        be_att_dr_id = data_rule['_id']
        source_id = data_rule['be_data_source_id']
        be_data_rule_lvls = config_database[self.dnx_config.be_attributes_data_rules_lvls_collection].find({'active': 1,
                                                                                                            'be_att_dr_id': be_att_dr_id}).sort(
            [('level_no', 1)])
        no_of_lvls = be_data_rule_lvls.count()
        for current_lvl_no, data_rule_lvls in enumerate(be_data_rule_lvls, start=1):
            be_att_id = data_rule_lvls['be_att_id']
            rule_id = data_rule_lvls['rule_id']
            next_pass = data_rule_lvls['next_pass']
            next_fail = data_rule_lvls['next_fail']
            g_result = 1 if no_of_lvls == current_lvl_no else 0
            be_id = int(config_database[self.dnx_config.be_attributes_collection].find_one({'_id': be_att_id})['be_id'])

            bt_current_collection = config_database[self.dnx_config.org_business_entities_collection].find_one({'_id': be_id})[
                'bt_current_collection']
            dq_result_collection = config_database[self.dnx_config.org_business_entities_collection].find_one({'_id': be_id})[
                'dq_result_collection']
            self.create_dq_indexes(dq_result_collection)
            # print('     lvl', current_lvl_no, data_rule_lvls)
            self.validate_data_rules(bt_current_collection, dq_result_collection, source_id, be_att_dr_id, category_no,
                                     be_att_id, rule_id, g_result, current_lvl_no, next_pass, next_fail)

    def create_dq_indexes(self, dq_result_collection):

        client = pymongo.MongoClient(self.dnx_config.mongo_uri)
        result_database = client[self.dnx_config.result_db_name]

        try:
            result_index1 = IndexModel([("be_att_dr_id", 1), ("is_issue", 1)], name='index1')
            result_database[dq_result_collection].create_indexes([result_index1, ])
        except:
            None

    def start_dq(self):
        pd.set_option('mode.chained_assignment', None)
        self.dnx_config = DNXConfig.Config()
        self.parameters_dict = self.dnx_config.get_parameters_values()

        client = pymongo.MongoClient(self.dnx_config.mongo_uri)
        config_database = client[self.dnx_config.config_db_name]
        client.drop_database(self.dnx_config.result_db_name)
        client.drop_database(self.dnx_config.tmp_result_db_name)

        self.reset_category()
        self.reset_last_category_flag()

        categories = config_database[self.dnx_config.be_attributes_data_rules_collection].find({'active': 1}).distinct('category_no')
        categories.sort()
        for category_no in categories:
            # print('------------------------------------ category_no', category_no, '------------------------------------')
            be_attributes_data_rules_data = config_database[self.dnx_config.be_attributes_data_rules_collection].find(
                {'active': 1, 'category_no': category_no})
            parallel_execute_data_rules = []
            for data_rule in be_attributes_data_rules_data:
                delayed_execute_data_rules = delayed(self.execute_data_rules)(data_rule, category_no)
                parallel_execute_data_rules.append(delayed_execute_data_rules)
            with ProgressBar():
                print('Execute',len(parallel_execute_data_rules),'data rules in category', category_no)
                compute(*parallel_execute_data_rules)
            # print('---------------------------------------------------------------------------------------')
            self.upgrade_category(category_no)
