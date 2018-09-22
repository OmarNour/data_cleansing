import pymongo
from data_cleansing.dc_methods.dc_methods import get_parameter_values
import multiprocessing


class Config:
    # config_file_path = 'D:/github/Python/Data_Cleansing/data_cleansing/dnx_config.xlsx'
    config_db_url = 'sqlite:///C:/Users/Omar/PycharmProjects/data_cleansing/data_cleansing/dnx_config_db.db'
    mongo_uri = "mongodb://127.0.0.1:27017"
    # mongo_uri = "mongodb://192.168.3.108:27017" # waleed
    src_db_name = 'Source_data'
    dnx_db_name = 'DNX'
    config_db_name = 'DNX_config'
    result_db_name = 'Result'
    tmp_result_db_name = 'tmp_Result'
    process_no_column_name = 'process_no'

    parameters_collection = 'parameters'
    organizations_collection = 'organizations'
    org_connections_collection = 'org_connections'
    org_business_entities_collection = 'org_business_entities'
    org_attributes_collection = 'org_attributes'
    be_attributes_collection = 'be_attributes'
    be_data_sources_collection = 'be_data_sources'
    be_data_sources_mapping_collection = 'be_data_sources_mapping'
    data_rules_collection = 'data_rules'
    be_attributes_data_rules_collection = 'be_attributes_data_rules'
    be_attributes_data_rules_lvls_collection = 'be_attributes_data_rules_lvls'
    run_engine_collection = 'run_engine'

    cpu_count = multiprocessing.cpu_count()
    multiprocessing_collection = 'multiprocessing'
    multiprocessing_p_no = 'p_no'
    multiprocessing_cpu_num_workers = 'cpu_num_workers'
    multiprocessing_etl = 'elt'
    multiprocessing_bt_inserts = 'bt_inserts'
    multiprocessing_bt_current_inserts = 'bt_current_inserts'
    multiprocessing_bt_current_deletes = 'bt_current_deletes'
    multiprocessing_process_alive = 'process_alive'

    def get_parameters_values(self):
        parameters_values = get_parameter_values(self.mongo_uri,
                                                 self.config_db_name,
                                                 self.parameters_collection)

        parameters_values_dict = {}
        for i in parameters_values:
            parameters_values_dict[i['_id']] = i['value']

        return parameters_values_dict


# if __name__ == '__main__':
#     x = Config()
#     print(x.get_parameters_values()['crud_operations_batch_size'])