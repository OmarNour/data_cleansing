import datetime
import data_cleansing.CONFIG.Config as DNXConfig
from data_cleansing.build_configuration_schema.config_schema import build_config_db
from data_cleansing.BT.BT import StartBt
from data_cleansing.DQ.DQ import StartDQ
from multiprocessing import Process
import pymongo
import pandas as pd
# import xlrd
import sys

if __name__ == '__main__':
    ############################ parameters ################################
    # BT = 0
    # DQ = 0
    ####################################### source database connection #########################################################
    # source_url = 'mssql+pymssql://data-usr:data-usr@APP-SRV:1433/'
    # source_schema = 'DNX'

    # source_query = "SELECT top 20000 CAST(BR_CD AS VARCHAR(255)) AS rowkey, BR_CD,  BR_DESC_AR," \
    #                "  BR_DESC_EN," \
    #                " BR_COUNTRY_CD, BR_CITY_CD, BR_CLOSE_DT, BR_OPEN_DT, BR_STAT, isnull( ORIGN_USER,'0') ORIGN_USER," \
    #                " isnull(ORGN_BUS_AREA,'0') ORGN_BUS_AREA FROM dbo.BRNCH_LKP"
    #
    # source_query = "SELECT top 100000 CAST(BR_CD AS VARCHAR(255)) AS rowkey, 10 BR_CD, 'asd' BR_DESC_AR," \
    #                "  'www' BR_DESC_EN," \
    #                " 12 BR_COUNTRY_CD, 23 BR_CITY_CD, 34 BR_CLOSE_DT, '343422' BR_OPEN_DT, 'S' BR_STAT, isnull( ORIGN_USER,'0') ORIGN_USER," \
    #                " isnull(ORGN_BUS_AREA,'0') ORGN_BUS_AREA FROM dbo.BRNCH_LKP"

    # attribute_id_url = 'mssql+pymssql://data-usr:data-usr@APP-SRV:1433/'
    # attribute_id_schema = 'DNXSSL'
    # query_att_id = "SELECT DISTINCT upper(m.Name) AttributeName, b.AttributeID " \
    #                " FROM DNX.DNXCDataQueries q " \
    #                " INNER JOIN dnx.DNXCDataQueryMetadata m ON m.QueryID = q.ScrQueryID AND q.DSStagingJOB = 'DSStaging4383_10_2'" \
    #                " INNER JOIN dnx.DNXCSourceStagingMapping sm ON sm.SourceAttributeID = m.SourceAttributeID AND sm.QueryID = q.ScrQueryID" \
    #                " INNER JOIN dnx.VwBEAllAttributes b ON b.BusinessEntityAttributeID = sm.BusinessEntityAttributeID "

    ########################################  # # the below is from mysql local DB  ############################################

    # source_url = 'sqlite:///C:/Users/onour/dnx_source_data.db'
    # source_schema = ''

    # source_query = "SELECT cast(BR_CD as varchar) || 'a1' rowkey ,'x1' BR_CD,  'x2' BR_DESC_AR, 'x3' BR_DESC_EN , 'x4' BR_COUNTRY_CD,  'x5' BR_CITY_CD,  'x6' BR_CLOSE_DT,  'x7' BR_OPEN_DT,  'x8' BR_STAT, 'x9' ORIGN_USER,  'x10' ORGN_BUS_AREA " \
    #                "FROM BRNCH_LKP limit 50000"
    # source_query = "SELECT cast(BR_CD as varchar) || 'a1' rowkey ,BR_CD,  BR_DESC_AR, BR_DESC_EN , BR_COUNTRY_CD,  BR_CITY_CD, BR_CLOSE_DT, BR_OPEN_DT, BR_STAT, ORIGN_USER,  ORGN_BUS_AREA " \
    #                 "FROM BRNCH_LKP limit 1"

    # source_query = "SELECT cast(BR_CD as varchar) || 'a' rowkey ,BR_CD,  BR_DESC_AR, BR_DESC_EN , BR_COUNTRY_CD,  BR_CITY_CD, BR_CLOSE_DT, BR_OPEN_DT, BR_STAT, ORIGN_USER,  ORGN_BUS_AREA " \
    #                 "FROM BRNCH_LKP " \
    #                 "union " \
    #                 "SELECT cast(BR_CD as varchar) || 'b' rowkey ,BR_CD,  BR_DESC_AR, BR_DESC_EN , BR_COUNTRY_CD,  BR_CITY_CD, BR_CLOSE_DT, BR_OPEN_DT, BR_STAT, ORIGN_USER,  ORGN_BUS_AREA " \
    #                 "FROM BRNCH_LKP 				" \
    #                 "union " \
    #                 "SELECT cast(BR_CD as varchar) || 'c' rowkey ,BR_CD,  BR_DESC_AR, BR_DESC_EN , BR_COUNTRY_CD,  BR_CITY_CD, BR_CLOSE_DT, BR_OPEN_DT, BR_STAT, ORIGN_USER,  ORGN_BUS_AREA " \
    #                 "FROM BRNCH_LKP 				"

    # attribute_id_url = 'sqlite:///C:/Users/onour/dnx_source_data.db'
    # attribute_id_schema = ''
    # query_att_id = "select upper(att_name) AttributeName, att_id AttributeID from business_attributes"

    ############################################ mongo db connection ####################################################
    # mongo_uri = "mongodb://127.0.0.1:27017"
    # mongo_uri = "mongodb://192.168.3.108:27017" # waleed
    # dnx_db = 'DNX'

    ############################################ dnx parameters ####################################################
    # org_id = 4383
    # be_id = 20
    # source_id = 2

    # staging_collection_name = 'ST_' + str(org_id) + '_' + str(be_id)
    # bt_collection_name = 'BT_' + str(org_id) + '_' + str(be_id)
    # bt_current_collection_name = 'BT_current_' + str(org_id) + '_' + str(be_id)

    ############################ calling methods ################################
    try:
        process_no = int(sys.argv[1])
    except:
        process_no = -1

    try:
        BT = int(sys.argv[2])
    except:
        BT = 0

    try:
        DQ = int(sys.argv[3])
    except:
        DQ = 0

    try:
        cpu_num_workers = int(sys.argv[4])
    except:
        cpu_num_workers = -1


    # print('process_no:', process_no)
    # print('BT:', BT)
    # print('DQ:', DQ)

    if BT == 1:
        # BT_time = datetime.datetime.now()
        bt = StartBt()
        bt.start_bt(process_no, cpu_num_workers)
        # print('----------------     BT_time:', datetime.datetime.now() - BT_time, '      ----------------')
    if DQ == 1:
        # DQ_time = datetime.datetime.now()
        start_dq = StartDQ()
        start_dq.start_dq()
        # print('----------------     DQ_time:', datetime.datetime.now() - DQ_time, '      ----------------')
