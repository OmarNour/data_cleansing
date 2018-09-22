from sqlalchemy import create_engine
import pandas as pd
import pymongo
import data_cleansing.CONFIG.Config as DNXConfig


def get_data_from_db(url, schema, query):
    url_schema = url + schema
    engine = create_engine(url_schema)
    connection = engine.connect()
    source_query = query
    results_proxy = connection.execute(source_query)
    return results_proxy


def get_dict_data(source_query):
    url = DNXConfig.Config.config_db_url
    schema = ''
    result_proxy = get_data_from_db(url, schema, source_query)
    query_result_data = result_proxy.fetchall()
    query_result_columns = query_result_data[0].keys()
    query_result_data_df = pd.DataFrame(query_result_data, columns=query_result_columns)
    query_result_data_dict = query_result_data_df.to_dict('records')
    return query_result_data_dict


def build_config_db():
    client = pymongo.MongoClient(DNXConfig.Config.mongo_uri)
    database = client[DNXConfig.Config.config_db_name]

    table_names = ['parameters', 'organizations',
                   'org_connections', 'org_business_entities', 'org_attributes',
                   'be_attributes', 'be_data_sources', 'be_data_sources_mapping',
                   'be_attributes_data_rules', 'be_attributes_data_rules_lvls',
                   'data_rules', 'run_engine'
                   ]
    build_all = []

    for table_name in table_names:
        query = 'select * from ' + table_name
        build = [table_name, query]
        build_all.append(build)

    for i in build_all:
        database[i[0]].drop()
        data_dict = get_dict_data(i[1])
        database[i[0]].insert_many(data_dict)


# if __name__ == '__main__':
#     build_config_db()
