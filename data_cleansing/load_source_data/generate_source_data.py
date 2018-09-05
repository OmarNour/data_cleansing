import sqlite3

# SELECT BR_CD rowkey ,
#               BR_CD,
#               BR_DESC_AR,
#               BR_DESC_EN ,BR_COUNTRY_CD, BR_CITY_CD, BR_CLOSE_DT, BR_OPEN_DT, BR_STAT,ORIGN_USER, ORGN_BUS_AREA FROM BRNCH_LKP

conn = sqlite3.connect('C:/Users/onour/dnx_source_data.db')
c = conn.cursor()
BR_CD = 1
BR_DESC_AR = 'ققققققققققققققق'
BR_DESC_EN = 'asdasda'
BR_COUNTRY_CD = 234422
BR_CITY_CD = 'CAwe'
BR_CLOSE_DT = '2020-11-30'
BR_OPEN_DT = '2010-11-30'
BR_STAT = 'D'
ORIGN_USER = 'Omar Nour'
ORGN_BUS_AREA = 'CairoABC'

# be_list = [['10','BR_CD','80'], ['10','BR_DESC_AR','90'],['10','BR_DESC_EN','100'],['10','BR_COUNTRY_CD','120'],
#            ['10','BR_CITY_CD','130'],['10','BR_CLOSE_DT','140'],['10','BR_OPEN_DT','150'],
#            ['10','BR_STAT','170'],['10','ORIGN_USER','1'],['10','ORGN_BUS_AREA','2']]
# for i in be_list:
#     c.execute("insert into business_attributes(be,att_name,att_id) values(?,?,?)", i)

for i in range(226232,300000):
    my_list = [i,BR_DESC_AR,BR_DESC_EN,BR_COUNTRY_CD,BR_CITY_CD,BR_CLOSE_DT,BR_OPEN_DT,BR_STAT,ORIGN_USER,ORGN_BUS_AREA]
    c.execute("insert into BRNCH_LKP values(?,?,?,?,?,?,?,?,?,?)", my_list)
    c.connection.commit()

# for i in range(0,10000):
#     my_list = [BR_DESC_AR,BR_DESC_EN,BR_COUNTRY_CD,BR_CITY_CD,BR_CLOSE_DT,BR_OPEN_DT,BR_STAT,ORIGN_USER,ORGN_BUS_AREA,i]
#     c.execute("UPDATE BRNCH_LKP set BR_DESC_AR = ?,BR_DESC_EN = ?,"
#               "BR_COUNTRY_CD = ?,BR_CITY_CD = ?,BR_CLOSE_DT = ?,BR_OPEN_DT = ?,"
#               "BR_STAT = ?,ORIGN_USER = ?,ORGN_BUS_AREA = ? where BR_CD = ?",my_list)
#     c.connection.commit()

# for item in my_list:
#     c.execute("insert into BRNCH_LKP values(?,?,?,?,?,?,?,?,?,?)", my_list)