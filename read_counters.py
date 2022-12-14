#!/python2
# -*- coding: utf-8 -*-

import fdb
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import logging
import configparser
from distutils.util import strtobool
from time import strftime,strptime
import inspect, os.path
import traceback

filename = inspect.getframeinfo(inspect.currentframe()).filename
wd1 = os.path.dirname(os.path.abspath(filename))+'\\'
wd = wd1+'read_counters_log\\'
path=wd1+"config_file_read_counters.txt"
if not os.path.exists(path):
    config = configparser.ConfigParser()
    config.add_section("Settings_DB_old")
    config.set("Settings_DB_old", "database", "counters")
    config.set("Settings_DB_old", "user", "counters")
    config.set("Settings_DB_old", "password", "counters")
    config.set("Settings_DB_old", "host", "192.168.105.30")
    config.set("Settings_DB_old", "port", "5432")
    config.set("Settings_DB_old", "schema", "tmp")
    config.add_section("Settings_DB_new")
    config.set("Settings_DB_new", "database", "scout_keys")
    config.set("Settings_DB_new", "user", "postgres")
    config.set("Settings_DB_new", "password", "webscout")
    config.set("Settings_DB_new", "host", "10.15.20.2")
    config.set("Settings_DB_new", "port", "5432")
    config.set("Settings_DB_new", "schema", "tmp")
    config.add_section("Settings")
    config.set("Settings", "new_db_flag", "True")
    config.set("Settings", "fb_lib", "G:\\Counters\\bin3\\fbclient.dll")
    config.set("Settings", "datetime_bad_flag", "False")
    config.set("Settings", "datetime_log_flag", "True")
    with open(path, "w") as config_file:
        config.write(config_file)

config = configparser.ConfigParser()
config.read(path)
old_db_name = config.get("Settings_DB_old", "database")
old_db_user = config.get("Settings_DB_old", "user")
old_db_password = config.get("Settings_DB_old", "password")
old_db_host = config.get("Settings_DB_old", "host")
old_db_port = config.get("Settings_DB_old", "port")
old_db_schema = config.get("Settings_DB_old", "schema")
new_db_name = config.get("Settings_DB_new", "database")
new_db_user = config.get("Settings_DB_new", "user")
new_db_password = config.get("Settings_DB_new", "password")
new_db_host = config.get("Settings_DB_new", "host")
new_db_port = config.get("Settings_DB_new", "port")
new_db_schema = config.get("Settings_DB_new", "schema")
new_db_flag_str = config.get("Settings", "new_db_flag")
fb_lib = config.get("Settings", "fb_lib")
datetime_bad = config.get("Settings", "datetime_bad_bool")
datetime_log = config.get("Settings", "datetime_log_bool")

dt_log = bool(strtobool(datetime_log))
if dt_log:
    date = strftime('%Y%m%d_%H-%M')
    name_log = wd+date+"_read_counters.log"
else: 
    name_log = wd+"read_counters.log"
logging.basicConfig(filename = name_log, 
    format = '[%(asctime)s] [%(levelname)s] => %(message)s',
    level = logging.DEBUG,
    filemode = 'w')
 
logging.info("Start of the program")

new_db_flag = bool(strtobool(new_db_flag_str))
dt_bad = bool(strtobool(datetime_bad))

sql_data = """  select ID_VALUE,ADRESS,TYPE_KPU,KLEMMA,SER_NUM,cast(VALUE_ZN as integer) as VALUE_ZN,DATE_VAL,EMPTY,LINE_STATE
                from KPU k inner join COUNTER c on k.ID_KPU=c.ID_KPU
                inner join DATE_VALUE v on v.ID_KLEMMA=c.ID_KLEMMA where c.TYPE_COUNTER<>0 or c.TYPE_COUNTER is NULL;"""
teplo_data = """select ID_VALUE,SER_NUM,ENERGY,T1,T2,V,R,DATE_VAL,M 
                from TEPLO_VALUE t inner join counter c on c.ID_KLEMMA = t.ID_KLEMMA 
                where c.SER_NUM is not null"""

# ?????????????????????? ?? ???????????? ????
flag_old=True
try:
    conn = psycopg2.connect(dbname=old_db_name,user=old_db_user, host=old_db_host, password=old_db_password, port=old_db_port)
    engine = create_engine('postgresql+psycopg2://'+old_db_user+':'+old_db_password+'@'+old_db_host+'/'+old_db_name)        
    logging.debug("Connecting to an old DB")
except:
    flag_old=False
    logging.error("Couldn't connect to the old DB")
    print (traceback.format_exc())

# ?????????????????????? ?? ?????????? ????
if new_db_flag:
    try:
        conn_new = psycopg2.connect(dbname=new_db_name,user=new_db_user, host=new_db_host, password=new_db_password, port=new_db_port)
        engine_new = create_engine('postgresql+psycopg2://'+new_db_user+':'+new_db_password+'@'+new_db_host+'/'+new_db_name)      
        logging.debug("Connecting to a new DB")
    except:
        new_db_flag=False
        logging.error("Could not connect to the new DB")
        print (traceback.format_exc())

if flag_old:
    # c?????????????? ?????????????? ???? ???????????? ????
    scout_old_sql = "select id_entr, ip_rassbery,fdb_login,fdb_password,fdb_path from save.login_data where not fdb_path is null;"
    counters_old_sql = """  select id_klemma,coalesce(k.id_entr,ct.id_entr,f.id_entr) as id_entr_,adress,type_kpu,klemma,serial_number,case when last_date is null then '2000-01-01 00:00:00' else last_date end AS last_date
                            from  cnt.counter ct left join cnt.kpu k on k.id_kpu=ct.id_kpu
                            left join flat f on ct.id_flat = f.id_flat
                            where (k.workability is null or k.workability = 1) and ct.working_capacity = true""" 
    scouts = pd.read_sql(scout_old_sql, conn)
    counters = pd.read_sql(counters_old_sql,conn)
    conn.close()

    logging.debug("Creating list SCOUT")
    
    # ???????????????? ?????????????? ???? ?????????? ????
    if new_db_flag:
        scout_new_sql = "select distinct id_complex, ip_address,alias_fdb from scout.v_scout_full where workability = true and not alias_fdb is null;"
        counters_new_sql = """  select id_counter,serial_number,terminal_number,address,type_number,id_complex, cast ((case when value_date is null then '2000-01-01 00:00:00' else value_date end) as timestamp without time zone) 
                                from resources.v_controller_counter
                                where counter_workability = true and controller_workability = true
                                union
                                select id_counter,serial_number,null as terminal_number,null as address,null as type_number,id_complex, cast ((case when value_date is null then '2000-01-01 00:00:00' else value_date end) as timestamp without time zone)
                                from resources.v_heat_counter where workability = true"""
        scouts_new = pd.read_sql(scout_new_sql, conn_new)
        counters_new = pd.read_sql(counters_new_sql,conn_new)
        conn_new.close()
    
    # ???????? ???? ??????????????
    scouts.insert(1,'id_complex',0)
    for i, scout in scouts.iterrows():
        id_entr = scout["id_entr"]
        if dt_bad:
            date = strftime('%Y%m%d_%H-%M')
            file_name = wd+date+'_'+scout["ip_rassbery"]+'.'+scout["fdb_path"]
        else:
            file_name = wd+scout["ip_rassbery"]+'.'+scout["fdb_path"]
            
        # ?????????? ???????????? ?? ?????????? ????????
        if new_db_flag:
            id_complex = 0
            complex = scouts_new[(scouts_new["ip_address"] == scout["ip_rassbery"])&
                (scouts_new["alias_fdb"] == scout["fdb_path"])]
            if complex.shape[0] > 0:
                id_complex = complex.iloc[0]['id_complex']            
                scouts.loc[i,'id_complex'] = id_complex
 
        # ???????????????????????????? ?? ??????????
        count_connect = 0
        while (count_connect<3):
            flag = True
            try:
                fdb_con=fdb.connect(database=scout["fdb_path"], host=scout["ip_rassbery"], user=scout["fdb_login"], password=scout["fdb_password"],
                                            sql_dialect=3,charset='UTF-8', 
                                            fb_library_name=str(fb_lib)
                                            )
                logging.info("Connecting with a SCOUT "+scout["ip_rassbery"]+" : "+scout["fdb_path"])
                count_connect = 3
            except:
                flag = False
                print("failed to connect to the DB "+scout["ip_rassbery"]+" : "+scout["fdb_path"])
                logging.error("Could not connect to SCOUT "+scout["ip_rassbery"]+" : "+scout["fdb_path"])
                count_connect+=1
                print (traceback.format_exc())
        
        if flag:        
            data = pd.read_sql(sql_data, fdb_con)
            max_data_id = data['ID_VALUE'].max()
            data.insert(1, "last_date", 0) # ?????? ???????????? ????
            data.insert(1, "last_date_n", 0) # ?????? ?????????? ????
            data.insert(1, "id_klemma", 0) # ?????? ???????????? ????
            data.insert(1, "id_counter",0) # ?????? ?????????? ????
            
            for j, pok in data.iterrows():
                address = pok["ADRESS"]
                type_kpu = pok["TYPE_KPU"]
                klemma = pok["KLEMMA"]
                
                # ???????? ?????????????? ?? ???????????? ????????
                counter = counters[(counters["id_entr_"] == id_entr)&
                    (counters["adress"] == address)&
                    (counters["type_kpu"] == type_kpu)&
                    (counters["klemma"] == klemma)]
                if counter.shape[0] > 0:
                    data.loc[j,'id_klemma'] =  counter.iloc[0]['id_klemma']
                    data.loc[j,'last_date'] =  counter.iloc[0]['last_date']
                
                # ???????? ?????????????? ?? ?????????? ????????
                if new_db_flag:
                    counter_new = counters_new[(counters_new["id_complex"] == id_complex)&
                        (counters_new["address"] == address)&
                        (counters_new["type_number"] == type_kpu)&
                        (counters_new["terminal_number"] == klemma)]
                    if counter_new.shape[0] > 0:
                        data.loc[j,'id_counter'] =  counter_new.iloc[0]['id_counter']
                        data.loc[j,'last_date_n'] =  counters_new.iloc[0]['value_date']
               
            if data.shape[0] != 0:                
                logging.info("============= There are "+str(data.shape[0])+" records in DATE_VALUE on the SCOUT")

                # ???????????????????? ?????????????????? ?? ???????????? ????????
                data_recognized = data[data["id_klemma"] != 0]
                
                if data_recognized.shape[0] != 0:
                    flag_delete = True
                    logging.info("============= There are "+str(data_recognized.shape[0])+" recognized records in DATE_VALUE (old DB)")  
                    data_duplicate = data_recognized[data_recognized["DATE_VAL"] <= data_recognized["last_date"]][["id_klemma","DATE_VAL","VALUE_ZN","EMPTY","LINE_STATE"]]
                    if data_duplicate.shape[0] != 0:
                        try:
                            logging.info("============= "+str(data_duplicate.shape[0])+" duplicate records in DATE_VALUE (old DB)")
                            path = file_name + "_duplicate.txt"
                            data_duplicate.to_csv(path,sep=';')
                        except:
                            flag_delete = False
                            logging.error("============= duplicate records in DATE_VALUE is not saved in the old DB")
                            print (traceback.format_exc())
                    del data_duplicate
                    
                    data_old = data_recognized[data_recognized["DATE_VAL"] > data_recognized["last_date"]][["id_klemma","DATE_VAL","VALUE_ZN","EMPTY","LINE_STATE"]]
                    del data_recognized
                    if data_old.shape[0] != 0:
                        data_old.rename(columns = {'VALUE_ZN':'impulse_value'}, inplace = True) 
                        data_old.rename(columns = {'DATE_VAL':'date_val'}, inplace = True) 
                        data_old.rename(columns = {'EMPTY':'empty'}, inplace = True) 
                        data_old.rename(columns = {'LINE_STATE':'line_state'}, inplace = True)
                        try:
                            data_old.to_sql('date_value',con=engine,schema=old_db_schema,index =False,if_exists ='append',method='multi')
                            logging.info("============= DATE_VALUE saved in the old DB")                        
                        except:
                            flag_delete = False
                            path = file_name + ".txt"
                            data_old.to_csv(path,sep=';')
                            logging.error("============= DATE_VALUE is not saved in the old DB")
                            print (traceback.format_exc())
                    else: logging.info("============= There is no new data in DATE_VALUE")
                    
                    del data_old
                
                if flag_delete:
                    try:
                        sql_query = "DELETE FROM DATE_VALUE WHERE ID_VALUE<=?"
                        cur = fdb_con.cursor()            
                        cur.execute(sql_query, (max_data_id, ))
                        fdb_con.commit()
                        logging.info("============= records in DATE_VALUE have been deleted on SCOUT")
                    except:
                        logging.info("============= deleting records on the SCOUT ended with an error")
                        print (traceback.format_exc())
                else: logging.info("============= records in DATE_VALUE were not deleted on SCOUT")    
                
                bad_count = data[data["id_klemma"] == 0 ].shape[0]
                if bad_count != 0:
                    path = file_name + '_bad.txt'
                    data[data["id_klemma"] == 0 ].to_csv(path,sep=';')
                    logging.error("============= There are unrecognized counters in DATE_VALUE (old DB) - "+str(bad_count))

                # ???????????????????? ?????????????????? ?? ?????????? ????????
                if new_db_flag:
                    if id_complex != 0:
                        data_recognized = data[data["id_counter"] != 0]
                        if data_recognized.shape[0] != 0:
                            logging.info("============= There are "+str(data_recognized.shape[0])+" recognized records in DATE_VALUE (new DB)")
                            data_duplicate = data_recognized[data_recognized["DATE_VAL"] <= data_recognized["last_date_n"]][["id_counter","DATE_VAL","VALUE_ZN","EMPTY","LINE_STATE"]]
                            if data_duplicate.shape[0] != 0:
                                try:
                                    logging.info("============= "+str(data_duplicate.shape[0])+" duplicate records in DATE_VALUE (new DB)")
                                    path = file_name + "_duplicate.new.txt"
                                    data_duplicate.to_csv(path,sep=';')
                                except:
                                    logging.error("============= duplicate records in TEPLO_VALUE is not saved in the new DB(energy)")
                                    print (traceback.format_exc())
                            del data_duplicate
                            
                            data_old = data_recognized[data_recognized["DATE_VAL"] > data_recognized["last_date_n"]][["id_counter","DATE_VAL","VALUE_ZN","EMPTY","LINE_STATE"]]
                            del data_recognized
                            if data_old.shape[0] != 0:
                                data_old.rename(columns = {'VALUE_ZN':'impulse_count'}, inplace = True) 
                                data_old.rename(columns = {'DATE_VAL':'date_value'}, inplace = True) 
                                data_old.rename(columns = {'EMPTY':'empty'}, inplace = True) 
                                data_old.rename(columns = {'LINE_STATE':'line_control'}, inplace = True)
                                try:     
                                    data_old.to_sql('indication',con=engine_new,schema=new_db_schema,index =False,if_exists ='append',method='multi')
                                    logging.info("============= DATE_VALUE saved in the new DB")  
                                except:
                                    path = file_name + ".new.txt"
                                    data_old.to_csv(path,sep=';')
                                    logging.error("============= DATE_VALUE is not saved in the new DB")
                                    print (traceback.format_exc())
                            else: logging.info("============= There is no new data in DATE_VALUE")

                            del data_old
                            
                        bad_count = data[data["id_counter"] == 0 ].shape[0]
                        if bad_count != 0:
                            path = file_name + '_bad.new.txt'
                            data[data["id_counter"] == 0 ].to_csv(path,sep=';')
                            logging.error("============= There are unrecognized counters in DATE_VALUE (new DB) - "+str(bad_count))
                    else: logging.error("============= there is no scout in the new DB")
            else:
                logging.info("============= There is no data on the SCOUT in DATE_VALUE")

            del data
            # ??????????????????????????
            teplo = pd.read_sql(teplo_data, fdb_con)
            max_teplo_id = teplo['ID_VALUE'].max()
            teplo.insert(1, "last_date", 0) # ?????? ???????????? ????
            teplo.insert(1, "last_date_n", 0) # ?????? ?????????? ????
            teplo.insert(1, "id_klemma", 0)
            teplo.insert(1, "id_counter", 0)
            for j, pok in teplo.iterrows():
            
                ser_num_u = pok["SER_NUM"]
                ser_num = ser_num_u.encode('utf-8')
                
                # ???????? ?????????????? ?? ???????????? ????
                counter = counters[(counters["id_entr_"] == id_entr)&
                    (counters["serial_number"] == ser_num)]
                    
                if counter.shape[0] > 0:
                    teplo.loc[j,'id_klemma'] =  counter.iloc[0]['id_klemma']
                    teplo.loc[j,'last_date'] =  counter.iloc[0]['last_date']
                
                # ???????? ?????????????? ?? ?????????? ????
                if new_db_flag:
                    counter = counters_new[(counters_new["id_complex"] == id_complex)&
                        (counters_new["serial_number"] == ser_num)]
                    if counter.shape[0] > 0:
                        teplo.loc[j,'id_counter'] =  counter.iloc[0]['id_counter']
                        teplo.loc[j,'last_date_n'] =  counters_new.iloc[0]['value_date']                        

            if teplo.shape[0] != 0:
                flag_delete = True
                logging.info("============= There are "+str(teplo.shape[0])+" records in TEPLO_VALUE on the SCOUT")
                # ???????????????????? ?????????????????? ?? ???????????? ????
                data_recognized = teplo[teplo["id_klemma"] != 0]
                
                if data_recognized.shape[0] != 0:
                    logging.info("============= There are "+str(data_recognized.shape[0])+" recognized records in TEPLO_VALUE (old DB)")
                    
                    data_duplicate = data_recognized[data_recognized["DATE_VAL"] <= data_recognized["last_date"]]
                    if data_duplicate.shape[0] != 0:
                        try:
                                logging.info("============= "+str(data_duplicate.shape[0])+" duplicate records in TEPLO_VALUE energy (old DB)")
                                path = file_name + "_duplicate.teplo.txt"
                                data_duplicate.to_csv(path,sep=';')
                        except:
                            flag_delete = False
                            logging.error("============= duplicate records in TEPLO_VALUE is not saved in the old DB(energy)")
                            print (traceback.format_exc())
                    del data_duplicate
                    try:                        
                        teplo_old = data_recognized[data_recognized["DATE_VAL"] > data_recognized["last_date"]][["id_klemma","DATE_VAL","ENERGY"]]
                        if teplo_old.shape[0] != 0:
                            teplo_old.rename(columns = {'ENERGY':'value_zn'}, inplace = True) 
                            teplo_old.rename(columns = {'DATE_VAL':'date_val'}, inplace = True) 
                            teplo_old.insert(1, "empty", 0)
                            teplo_old.insert(1, "line_state", 0)
                            
                            teplo_old.to_sql('date_value',con=engine,schema=old_db_schema,index =False,if_exists ='append',method='multi')
                            logging.info("============= TEPLO_VALUE saved in the old DB(energy)")
                        else: logging.info("============= There is no new data in TEPLO_VALUE")    
                    except:
                        flag_delete = False
                        path = file_name + '.teplo.txt'
                        teplo_old.to_csv(path,sep=';')
                        logging.error("============= TEPLO_VALUE is not saved in the old DB(energy)")
                        print (traceback.format_exc())
                    
                    del teplo_old

                    teplo_old= data_recognized[data_recognized["DATE_VAL"] > data_recognized["last_date"]][["id_klemma","DATE_VAL","ENERGY","T1","T2","V","R","M"]]  
                    del data_recognized
                    teplo_old.rename(columns = {'DATE_VAL':'date_value'}, inplace = True) 
                    teplo_old.rename(columns = {'id_klemma':'id_counter'}, inplace = True)
                    teplo_old.rename(columns = {'ENERGY':'q'}, inplace = True) 
                    teplo_old.rename(columns = {'V':'v1'}, inplace = True) 
                    teplo_old.rename(columns = {'R':'dv1'}, inplace = True) 
                    teplo_old.rename(columns = {'M':'dq'}, inplace = True) 
                    teplo_old.rename(columns = {'T1':'t1'}, inplace = True) 
                    teplo_old.rename(columns = {'T2':'t2'}, inplace = True) 
                    try:
                        teplo_old.to_sql('heat_indication',con=engine,schema=old_db_schema,index =False,if_exists ='append',method='multi')
                        logging.info("============= TEPLO_VALUE saved in the old DB(all data)") 
                        
                    except:
                        flag_delete = False
                        path = file_name + '.teplo_all.txt'
                        teplo_old.to_csv(path,sep=';')
                        logging.error("============= TEPLO_VALUE is not saved in the old DB(all data)")
                        print (traceback.format_exc())
                    
                    del teplo_old
                
                if flag_delete:
                    try:
                        sql_query_teplo = "DELETE FROM TEPLO_VALUE WHERE ID_VALUE<=?"
                        cur = fdb_con.cursor()            
                        cur.execute(sql_query_teplo, (max_teplo_id, ))
                        fdb_con.commit()
                        fdb_con.close()
                        logging.info("============= records in TEPLO_VALUE have been deleted on SCOUT")
                    except:
                        logging.info("============= deleting records on the SCOUT ended with an error")
                        print (traceback.format_exc())
                else: logging.info("============= records in TEPLO_VALUE were not deleted on SCOUT")    
                
                bad_count = teplo[teplo["id_klemma"] == 0 ].shape[0]
                if bad_count != 0:                    
                    path = file_name + '.teplo_bad.txt'
                    teplo[teplo["id_klemma"] == 0 ].to_csv(path,sep=';')
                    logging.error("============= There are unrecognized counters in TEPLO_VALUE (old DB) - "+str(bad_count))
                
                # ???????????????????? ?????????????????? ?? ?????????? ????
                if new_db_flag:
                    if id_complex != 0:
                        data_recognized = teplo[teplo["id_counter"] != 0]
                        if data_recognized.shape[0] != 0:
                            logging.info("============= There are "+str(data_recognized.shape[0])+" recognized records in DATE_VALUE (new DB)")
                            
                            data_duplicate = data_recognized[data_recognized["DATE_VAL"] <= data_recognized["last_date_n"]][["id_counter","DATE_VAL","ENERGY","T1","T2","V","R","M"]]
                            if data_duplicate.shape[0] != 0:
                                try:
                                    logging.info("============= "+str(data_duplicate.shape[0])+" duplicate records in DATE_VALUE (new DB)")
                                    path = file_name + "_duplicate.teplo_all.new.txt"
                                    data_duplicate.to_csv(path,sep=';')
                                except:
                                    logging.error("============= duplicate records in TEPLO_VALUE is not saved in the new DB(all data)")
                                    print (traceback.format_exc())
                            del data_duplicate
                            teplo_old= data_recognized[data_recognized["DATE_VAL"] > data_recognized["last_date_n"]][["id_counter","DATE_VAL","ENERGY","T1","T2","V","R","M"]]  
                            del data_recognized
                            if teplo_old.shape[0] != 0:
                                teplo_old.rename(columns = {'DATE_VAL':'date_value'}, inplace = True) 
                                teplo_old.rename(columns = {'ENERGY':'q'}, inplace = True) 
                                teplo_old.rename(columns = {'V':'v1'}, inplace = True) 
                                teplo_old.rename(columns = {'R':'dv1'}, inplace = True) 
                                teplo_old.rename(columns = {'M':'dq'}, inplace = True) 
                                teplo_old.rename(columns = {'T1':'t1'}, inplace = True) 
                                teplo_old.rename(columns = {'T2':'t2'}, inplace = True) 
                                teplo_old.insert(1,'system_number',1)
                                try:
                                    df_columns = list(teplo_old)
                                    columns = ",".join(df_columns)
                                    values = "VALUES({})".format(",".join(["%s" for _ in df_columns]))
                                    table_u = new_db_schema+'.v_heat_indication'
                                    table = table_u.encode('utf-8')
                                    insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)
                                    conn_new = psycopg2.connect(dbname=new_db_name,user=new_db_user, host=new_db_host, password=new_db_password, port=new_db_port)
                                    cur = conn_new.cursor()
                                    psycopg2.extras.execute_batch(cur, insert_stmt, teplo_old.values)
                                    conn_new.commit()
                                    conn_new.close()
                                    logging.info("============= TEPLO_VALUE saved in the new DB(all data)")
                                except:   
                                    path = file_name +'.teplo_all.new.txt'
                                    teplo_old.to_csv(path,sep=';')
                                    logging.error("============= TEPLO_VALUE is not saved in the new DB(all data)")
                                    print (traceback.format_exc())
                                    
                            del teplo_old 
                        
                        bad_count = teplo[teplo["id_counter"] == 0 ].shape[0]
                        if bad_count != 0:                    
                            path = file_name +'.teplo_bad.new.txt'
                            teplo[teplo["id_counter"] == 0 ].to_csv(path,sep=';')
                            logging.error("============= There are unrecognized counters in TEPLO_VALUE (new DB) - "+str(bad_count))
                    else: logging.info("============= there is no scout in the new DB")
            else:
                logging.info("============= There is no data on the SCOUT in TEPLO_VALUE")

            del teplo

logging.info("End of the program")