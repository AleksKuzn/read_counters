# -*- coding: utf-8 -*-

import fdb
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import logging
import configparser
from distutils.util import strtobool
from time import strftime
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

sql_data = """  select ID_VALUE,ADRESS,TYPE_KPU,KLEMMA,SER_NUM,LAST_POK,LAST_DATE,cast(value_zn as integer) as value_zn,date_val, empty,line_state
                from kpu k inner join COUNTER c on k.id_kpu=c.id_kpu
                inner join DATE_VALUE v on v.id_klemma=c.id_klemma where c.TYPE_COUNTER<>0 or c.TYPE_COUNTER is NULL;"""
teplo_data = """select ID_VALUE,ser_num,energy,t1,t2,v,r,date_val,m 
                from teplo_value t inner join counter c on c.id_klemma = t.id_klemma 
                where c.id_kpu is null"""

# Подключение к старой БД
flag_old=True
try:
    conn = psycopg2.connect(dbname=old_db_name,user=old_db_user, host=old_db_host, password=old_db_password, port=old_db_port)
    engine = create_engine('postgresql+psycopg2://'+old_db_user+':'+old_db_password+'@'+old_db_host+'/'+old_db_name)        
    logging.debug("Connecting to an old DB")
except:
    flag_old=False
    logging.error("Couldn't connect to the old DB")

# Подключение к новой БД
if new_db_flag:
    flag_new=True
    try:
        conn_new = psycopg2.connect(dbname=new_db_name,user=new_db_user, host=new_db_host, password=new_db_password, port=new_db_port)
        engine_new = create_engine('postgresql+psycopg2://'+new_db_user+':'+new_db_password+'@'+new_db_host+'/'+new_db_name)      
        logging.debug("Connecting to a new DB")
    except:
        flag_new=False
        logging.error("Could not connect to the new DB")

if flag_old:
    # cоздание списков из старой БД
    scout_old_sql = "select id_entr, ip_rassbery,fdb_login,fdb_password,fdb_path from save.login_data where not fdb_path is null;"
    counters_old_sql = """  select id_klemma,coalesce(k.id_entr,ct.id_entr,f.id_entr) as id_entr_,adress,type_kpu,klemma,serial_number
                            from  cnt.counter ct left join cnt.kpu k on k.id_kpu=ct.id_kpu
                            left join flat f on ct.id_flat = f.id_flat
                            where (k.workability is null or k.workability = 1) and ct.working_capacity = true""" 
    scouts = pd.read_sql(scout_old_sql, conn)#,index_col='id_entr')
    counters = pd.read_sql(counters_old_sql,conn)#,index_col='id_klemma')
    conn.close()

    logging.debug("Creating list SCAUT")
    
    # создание списков из новой БД
    if new_db_flag:
        if flag_new:
            scout_new_sql = "select distinct id_complex, ip_address,alias_fdb from scout.v_scout_full where workability = true and not alias_fdb is null;"
            counters_new_sql = """  select id_counter,serial_number,terminal_number,address,type_number,id_complex
                                    from resources.v_controller_counter
                                    where counter_workability = true and controller_workability = true
                                    union
                                    select id_counter,serial_number,null as terminal_number,null as address,null as type_number,id_complex
                                    from resources.v_heat_counter where workability = true"""
            scouts_new = pd.read_sql(scout_new_sql, conn_new)
            counters_new = pd.read_sql(counters_new_sql,conn_new)
            conn_new.close()
    
    # цикл по СКАУТам
    scouts.insert(1,'id_complex',0)
    for i, scout in scouts.iterrows():
        id_entr = scout["id_entr"]
        if dt_bad:
            date = strftime('%Y%m%d_%H-%M')
            file_name = wd+date+'_'+scout["ip_rassbery"]+'.'+scout["fdb_path"]
        else:
            file_name = wd+'_'+scout["ip_rassbery"]+'.'+scout["fdb_path"]
            
        # Поиск СКАУТа в новой базе
        if new_db_flag:
            id_complex = 0
            complex = scouts_new[(scouts_new["ip_address"] == scout["ip_rassbery"])&
                (scouts_new["alias_fdb"] == scout["fdb_path"])]
            if complex.shape[0] > 0:
                id_complex = complex.iloc[0]['id_complex']            
                scouts.loc[i,'id_complex'] = id_complex
                
#       print(i,scout["ip_rassbery"],' ',scout["fdb_path"], scouts['id_complex'][i])
 
        # Подсоединяемся к СКАУТ
        count_connect = 0
        while (count_connect<3):
            flag = True
            try:
                fdb_con=fdb.connect(database=scout["fdb_path"], host=scout["ip_rassbery"], user=scout["fdb_login"], password=scout["fdb_password"],
                                            sql_dialect=3,charset='UTF-8', 
                                            fb_library_name=str(fb_lib)
                                            )
                logging.info("Connecting with a SCAUT "+scout["ip_rassbery"]+" : "+scout["fdb_path"])
                count_connect = 3
            except:
                flag = False
                print("failed to connect to the DB "+scout["ip_rassbery"]+" : "+scout["fdb_path"])
                logging.error("Could not connect to SCAUT "+scout["ip_rassbery"]+" : "+scout["fdb_path"])
                count_connect+=1
        
        if flag:        
            data = pd.read_sql(sql_data, fdb_con)
            teplo = pd.read_sql(teplo_data, fdb_con)
            max_data_id = data['ID_VALUE'].max()
            max_teplo_id = teplo['ID_VALUE'].max()
            data.insert(1, "id_klemma", 0) # для старой БД
            data.insert(1, "id_counter",0) # для новой БД
            for j, pok in data.iterrows():
                address = pok["ADRESS"]
                type_kpu = pok["TYPE_KPU"]
                klemma = pok["KLEMMA"]
                
                # ищем счетчик в старой базе
                id_klemma=0
                counter = counters[(counters["id_entr_"] == id_entr)&
                    (counters["adress"] == address)&
                    (counters["type_kpu"] == type_kpu)&
                    (counters["klemma"] == klemma)]
                if counter.shape[0] > 0:
                    id_klemma= counter.iloc[0]['id_klemma']
                    data.loc[j,'id_klemma'] =  id_klemma
                
                # ищем счетчик в новой базе
                if new_db_flag:
                    counter_new = counters_new[(counters_new["id_complex"] == id_complex)&
                        (counters_new["address"] == address)&
                        (counters_new["type_number"] == type_kpu)&
                        (counters_new["terminal_number"] == klemma)]
                    if counter_new.shape[0] > 0:
                        id_counter= counter_new.iloc[0]['id_counter']
                        data.loc[j,'id_counter'] =  id_counter   
                
            if data.shape[0] != 0:                
                logging.info("============= There are "+str(data.shape[0])+" entries in DATE_VALUE on the SCAUT")

                # Записываем показания в старую базу
                good_count = data[data["id_klemma"] != 0 ].shape[0]
                if good_count != 0:
                    logging.info("============= There are "+str(good_count)+" recognized records in DATE_VALUE (old DB)")
                    data_old = data[data["id_klemma"] != 0 ][["id_klemma","DATE_VAL","VALUE_ZN","EMPTY","LINE_STATE"]]
                    data_old.rename(columns = {'VALUE_ZN':'impulse_value'}, inplace = True) 
                    data_old.rename(columns = {'DATE_VAL':'date_val'}, inplace = True) 
                    data_old.rename(columns = {'EMPTY':'empty'}, inplace = True) 
                    data_old.rename(columns = {'LINE_STATE':'line_state'}, inplace = True)
                    try:
                        data_old.to_sql('date_value',con=engine,schema=old_db_schema,index =False,if_exists ='append',method='multi')
                        logging.info("============= DATE_VALUE stored in the old DB")
                        
                        sql_query = "DELETE FROM DATE_VALUE WHERE ID_VALUE<=?"
                        cur = fdb_con.cursor()            
                        cur.execute(sql_query, (max_data_id, ))
                        fdb_con.commit()
                        logging.info("============= records in DATE_VALUE have been deleted on the SCAUT")
                    except:
                        path = file_name + ".txt"
                        data_old.to_csv(path,sep=';')
                        logging.error("============= DATE_VALUE is not saved in the old DB")

                    del data_old
                bad_count = data[data["id_klemma"] == 0 ].shape[0]
                if bad_count != 0:
                    path = file_name + '_bad.txt'
                    data[data["id_klemma"] == 0 ].to_csv(path,sep=';')
                    logging.error("============= There are unrecognized counters in DATE_VALUE (old DB) - "+str(bad_count))

                # Записываем показания в новую базу
                if new_db_flag:
                    if id_complex != 0:
                        good_count = data[data["id_counter"] != 0 ].shape[0]
                        if good_count != 0:
                            logging.info("============= There are "+str(good_count)+" recognized records in DATE_VALUE (new DB)")
                            data_old = data[data["id_counter"] != 0 ][["id_counter","DATE_VAL","VALUE_ZN","EMPTY","LINE_STATE"]]
                            data_old.rename(columns = {'VALUE_ZN':'impulse_count'}, inplace = True) 
                            data_old.rename(columns = {'DATE_VAL':'date_value'}, inplace = True) 
                            data_old.rename(columns = {'EMPTY':'empty'}, inplace = True) 
                            data_old.rename(columns = {'LINE_STATE':'line_control'}, inplace = True)
                            try:
                                data_old.to_sql('indication',con=engine_new,schema=new_db_schema,index =False,if_exists ='append',method='multi')
                                logging.info("============= DATE_VALUE stored in the new DB")
                            except Exception as ex:
                                path = file_name + ".new.txt"
                                data_old.to_csv(path,sep=';')
                                print('date_value new not preserved')
                                #print('Error:'+str(ex))
                                logging.error("============= DATE_VALUE is not saved in the new DB")
                                #logging.error("============= " + str(ex))

                            del data_old
                        bad_count = data[data["id_counter"] == 0 ].shape[0]
                        if bad_count != 0:
                            path = file_name + '_bad.new.txt'
                            data[data["id_counter"] == 0 ].to_csv(path,sep=';')
                            logging.error("============= There are unrecognized counters in DATE_VALUE (new DB) - "+str(bad_count))

            else:
                logging.info("============= There is no data on the SCAUT in DATE_VALUE")

            del data

            # Теплосчетчики
            teplo.insert(1, "id_klemma", 0)
            teplo.insert(1, "id_counter", 0)
            for j, pok in teplo.iterrows():
                ser_num = pok["SER_NUM"]
                
                # ищем счетчик в старой БД
                id_klemma=0
                counter = counters[(counters["id_entr_"] == id_entr)&
                    (counters["serial_number"] == ser_num)]
                if counter.shape[0] > 0:
                    id_klemma = counter.iloc[0]['id_klemma']
                    teplo.loc[j,'id_klemma'] =  id_klemma
                
                # ищем счетчик в новой БД
                if new_db_flag:
                    id_counter=0
                    counter = counters_new[(counters_new["id_complex"] == id_complex)&
                        (counters_new["serial_number"] == ser_num)]
                    if counter.shape[0] > 0:
                        id_counter = counter.iloc[0]['id_counter']
                        teplo.loc[j,'id_counter'] =  id_counter

            if teplo.shape[0] != 0:
                logging.info("============= There are "+str(teplo.shape[0])+" entries in TEPLO_VALUE on the SCAUT")
                # Записываем показания в старой БД
                good_count = teplo[teplo["id_klemma"] != 0 ].shape[0]
                if good_count != 0:
                    logging.info("============= There are "+str(good_count)+" recognized records in TEPLO_VALUE (old DB)")
                    teplo_old = teplo[teplo["id_klemma"] != 0 ][["id_klemma","DATE_VAL","ENERGY"]]
                    teplo_old.rename(columns = {'ENERGY':'value_zn'}, inplace = True) 
                    teplo_old.rename(columns = {'DATE_VAL':'date_val'}, inplace = True) 
                    teplo_old.insert(1, "empty", 0)
                    teplo_old.insert(1, "line_state", 0)
                    try:
                        teplo_old.to_sql('date_value',con=engine,schema=old_db_schema,index =False,if_exists ='append',method='multi')
                        logging.info("============= TEPLO_VALUE stored in the old DB(energy)")
                    except:
                        path = file_name + '.teplo.txt'
                        teplo_old.to_csv(path,sep=';')
                        #print('teplo date_value not preserved')
                        logging.error("============= TEPLO_VALUE is not saved in the old DB(energy)")
                    del teplo_old

                    teplo_old=teplo[teplo["id_klemma"] != 0 ][["id_klemma","DATE_VAL","ENERGY","T1","T2","V","R","M"]]  
                    
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
                        logging.info("============= TEPLO_VALUE stored in the old DB(all data)")
                        
                        sql_query_teplo = "DELETE FROM TEPLO_VALUE WHERE ID_VALUE<=?"
                        cur = fdb_con.cursor()            
                        cur.execute(sql_query_teplo, (max_teplo_id, ))
                        fdb_con.commit()
                        fdb_con.close()
                        logging.info("============= records in TEPLO_VALUE have been deleted on the SCAUT")
                    except Exception as ex:
                        path = file_name + '.teplo_all.txt'
                        teplo_old.to_csv(path,sep=';')
                        #print('heat_indication not preserved' + str(ex))
                        logging.error("============= TEPLO_VALUE is not saved in the old DB(all data)")
                        #logging.error("============= " + str(ex))
                    del teplo_old
                
                bad_count = teplo[teplo["id_klemma"] == 0 ].shape[0]
                if bad_count != 0:                    
                    path = file_name + '.teplo_bad.txt'
                    teplo[teplo["id_klemma"] == 0 ].to_csv(path,sep=';')
                    logging.error("============= There are unrecognized counters in TEPLO_VALUE (old DB) - "+str(bad_count))
                
                # Записываем показания в новую БД
                if new_db_flag:
                    if id_complex != 0:
                        good_count = teplo[teplo["id_counter"] != 0 ].shape[0]
                        if good_count != 0:
                            logging.info("============= There are "+str(good_count)+" recognized records in TEPLO_VALUE (new DB)")
                            
                            teplo_old=teplo[teplo["id_counter"] != 0 ][["id_counter","DATE_VAL","ENERGY","T1","T2","V","R","M"]]                          
                            teplo_old.rename(columns = {'DATE_VAL':'date_value'}, inplace = True) 
                            teplo_old.rename(columns = {'ENERGY':'q'}, inplace = True) 
                            teplo_old.rename(columns = {'V':'v1'}, inplace = True) 
                            teplo_old.rename(columns = {'R':'dv1'}, inplace = True) 
                            teplo_old.rename(columns = {'M':'dq'}, inplace = True) 
                            teplo_old.rename(columns = {'T1':'t1'}, inplace = True) 
                            teplo_old.rename(columns = {'T2':'t2'}, inplace = True) 
                            teplo_old.insert(1,'system_number',1)
                            try:
                                teplo_old.to_sql('heat_indication',con=engine_new,schema=new_db_schema,index =False,if_exists ='append',method='multi')
                                logging.info("============= TEPLO_VALUE stored in the new DB(all data)")
                            except Exception as ex:   
                                print('heat_indication new not preserved')
                                #print(ex)
                                path = file_name +'.teplo_all.new.txt'
                                teplo_old.to_csv(path,sep=';')
                                logging.error("============= TEPLO_VALUE is not saved in the new DB(all data)")
                                #logging.error("============= " + str(ex))
                            del teplo_old
                        
                        bad_count = teplo[teplo["id_counter"] == 0 ].shape[0]
                        if bad_count != 0:                    
                            path = file_name +'.teplo_bad.new.txt'
                            teplo[teplo["id_counter"] == 0 ].to_csv(path,sep=';')
                            logging.error("============= There are unrecognized counters in TEPLO_VALUE (new DB) - "+str(bad_count))
                    
            else:
                logging.info("============= There is no data on the SCAUT in TEPLO_VALUE")

            del teplo

logging.info("End of the program")