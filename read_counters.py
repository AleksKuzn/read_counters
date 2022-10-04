
import fdb
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import logging
#import numpy as np



logging.basicConfig(filename = "read_counters.log", 
    format = '[%(asctime)s] [%(levelname)s] => %(message)s',
    level = logging.DEBUG,
    filemode = 'w')
 
logging.info("Старт программы")

sql_data = "select  id_value,ADRESS,TYPE_KPU,KLEMMA,SER_NUM,LAST_POK,"+\
    "LAST_DATE,cast(value_zn as integer) as value_zn,date_val, empty,line_state "+\
    "from kpu k inner join COUNTER c on k.id_kpu=c.id_kpu "+\
    "inner join DATE_VALUE v on v.id_klemma=c.id_klemma;"
teplo_data = "select id_value, ser_num,energy,t1,t2,v,r,date_val,m "+\
    "from teplo_value t inner join counter c on c.id_klemma = t.id_klemma "+\
    "where c.id_kpu is null"

fb_lib = 'G:\\Counters\\bin3\\fbclient.dll'
flag_old=True
try:
    conn = psycopg2.connect(dbname='counters',user='', host='192.168.105.30', password='', port=5432)
    engine = create_engine('postgresql+psycopg2://login:password@192.168.105.30/counters')        
    logging.debug("Соединение со старой БД")
except:
    flag_old=False
    print ("I am unable to connect to the database.")
    logging.error("Не смог соединится со старой БД")

flag_new=True

try:
    conn_new = psycopg2.connect(dbname='scout_keys',user='', host='10.15.20.2', password='', port=5432)
    #engine_new = create_engine('postgresql+psycopg2://login:password@10.15.20.2/scout_keys')        
    logging.debug("Соединение с новой БД")

except:
    flag_new=False
    print ("I am unable to connect to the database.")
    logging.error("Не смог соединится с новой БД")

try:
     engine_new = create_engine('postgresql+psycopg2://login:password@10.15.20.2/scout_keys')        
    engine_new.connect()
    logging.debug("Соединение с новой БД2")

except:
    #flag_new=False
    print ("I am unable to connect to the database2.")
    logging.error("Не смог соединится с новой БД2")

if flag_old:
    # cсоздание списков из старой БД
    scout_old_sql = "select id_entr, ip_rassbery,fdb_login,fdb_password,fdb_path from save.login_data where not fdb_path is null ;"
    counters_old_sql = "select id_klemma,coalesce(k.id_entr,ct.id_entr,f.id_entr) as id_entr_, "+\
        "adress,type_kpu,klemma,serial_number "+\
        "from  cnt.counter ct left join cnt.kpu k on k.id_kpu=ct.id_kpu "+\
        "left join flat f on ct.id_flat = f.id_flat "+\
        "where (k.workability is null or k.workability = 1) and ct.working_capacity = true"    
    scouts = pd.read_sql(scout_old_sql, conn)#,index_col='id_entr')
    counters = pd.read_sql(counters_old_sql,conn)#,index_col='id_klemma')
    conn.close()

    print(scouts)
    print(counters.shape)
    logging.debug("Создание списка СКАУТов")

    # создание список из новой БД
    if flag_new:
        scout_new_sql = "select distinct id_complex, ip_address,alias_fdb from scout.v_scout_full where workability = true and not alias_fdb is null;"
        counters_new_sql = "select id_counter,serial_number,terminal_number,address,type_number,id_complex "+\
            "from resources.v_controller_counter "+\
            "where counter_workability = true and controller_workability = true "+\
            "union "+\
            "select id_counter,serial_number,null as terminal_number,null as address,null as type_number,id_complex "+\
            "from resources.v_heat_counter where workability = true"
        #scouts_new = pd.read_sql(scout_new_sql, conn_new)#,index_col='id_entr')
        scouts_new = pd.read_sql(scout_new_sql, conn_new)#,index_col='id_complex')
        counters_new = pd.read_sql(counters_new_sql,conn_new)#,index_col='id_klemma')
        conn_new.close()
        print(scouts_new)

    # цикл по СКАУТам
    scouts.insert(1,'id_complex',0)
    for i, scout in scouts.iterrows():
        id_entr = scout["id_entr"]#i
        file_name = f'tmp\{scout["ip_rassbery"]}.{scout["fdb_path"]}'
        file_name1 = f'tmp1\{scout["ip_rassbery"]}.{scout["fdb_path"]}'


        # Поиск СКАУТа в новой базе
        id_complex = 0
        complex = scouts_new[(scouts_new["ip_address"] == scout["ip_rassbery"])&
            (scouts_new["alias_fdb"] == scout["fdb_path"])]
        if complex.shape[0] > 0:
            id_complex= complex.iloc[0]['id_complex']            
            scouts['id_complex'][i] = id_complex

        print(i,scout["ip_rassbery"],' ',scout["fdb_path"], scouts['id_complex'][i])

        # Подсоединяемся к СКАУТ
        flag = True
        try:
            fdb_con=fdb.connect(database=scout["fdb_path"], host=scout["ip_rassbery"], user=scout["fdb_login"], password=scout["fdb_password"],
                                        sql_dialect=3,charset='UTF-8',
                                        fb_library_name=fb_lib)
            logging.info(f'Соединение со СКАУТом {scout["ip_rassbery"]} : {scout["fdb_path"]}')
        except:
            flag = False
            print(f'не удалось подключится к БД {scout["ip_rassbery"]} : {scout["fdb_path"]}')
            logging.error(f'Не смог соединится со СКАУТом {scout["ip_rassbery"]} : {scout["fdb_path"]}')
        
        if flag:        
            data = pd.read_sql(sql_data, fdb_con)
            teplo = pd.read_sql(teplo_data, fdb_con)
            #print(data)
            fdb_con.close()
            data.insert(1, "id_klemma", 0) # в старой БД
            data.insert(1, "id_counter",0) # в новой БД
            print(data.shape)
            for j, pok in data.iterrows():

                address = pok["ADRESS"]
                type_kpu = pok["TYPE_KPU"]
                klemma = pok["KLEMMA"]
                #print(id_entr,address,type_kpu,klemma)
                
                # ищем счетчик В СТАРОЙ БАЗЕ
                id_klemma=0
                counter = counters[(counters["id_entr_"] == id_entr)&
                    (counters["adress"] == address)&
                    (counters["type_kpu"] == type_kpu)&
                    (counters["klemma"] == klemma)]
                if counter.shape[0] > 0:
                    id_klemma= counter.iloc[0]['id_klemma']
                    data["id_klemma"][j] =  id_klemma
            
                # ищем счетчик в новой базе
                counter_new = counters_new[(counters_new["id_complex"] == id_complex)&
                    (counters_new["address"] == address)&
                    (counters_new["type_number"] == type_kpu)&
                    (counters_new["terminal_number"] == klemma)]
                if counter_new.shape[0] > 0:
                    id_counter= counter_new.iloc[0]['id_counter']
                    data["id_counter"][j] =  id_counter
                
            """
            if id_complex != 0:
                path = f'tmp\{scout["ip_rassbery"]}.{scout["fdb_path"]}.new.txt'
                data[data["id_counter"] == 0 ].to_csv(path,sep=';')
            """    
            if data.shape[0] != 0:                
                logging.info(f'=============На СКАУТе {data.shape[0]} записей в data_value')

                # Записываем показания в старую базу
                good_count = data[data["id_klemma"] != 0 ].shape[0]
                if good_count != 0:
                    logging.info(f'=============На СКАУТе {good_count} хороших date_value')
                    data_old = data[data["id_klemma"] != 0 ][["id_klemma","DATE_VAL","VALUE_ZN","EMPTY","LINE_STATE"]]
                    print(data_old.shape[0])
                    data_old.rename(columns = {'VALUE_ZN':'impulse_value'}, inplace = True) 
                    data_old.rename(columns = {'DATE_VAL':'date_val'}, inplace = True) 
                    data_old.rename(columns = {'EMPTY':'empty'}, inplace = True) 
                    data_old.rename(columns = {'LINE_STATE':'line_state'}, inplace = True) 
                    try:
                        data_old.to_sql('date_value',con=engine,schema='tmp',index =False,if_exists ='append',method='multi')
                        logging.info('============= Date_value сохранены в старой БД')
                    except:
                        path = file_name + ".txt"
                        data_old.to_csv(path,sep=';')
                        print('date_value все плохо')
                        logging.error("============= Date_value не сохранены в старой БД")

                    del data_old
                bad_count = data[data["id_klemma"] == 0 ].shape[0]
                if bad_count != 0:
                    path = file_name + '_bad.txt'
                    data[data["id_klemma"] == 0 ].to_csv(path,sep=';')
                    logging.error(f"=============Имеются нераспознанные счетчики в Date_value - {bad_count}")


                # Записываем показания в новую базу
                if id_complex != 0:
                    good_count = data[data["id_counter"] != 0 ].shape[0]
                    if good_count != 0:
                        logging.info(f'============= Имеется {good_count} распознанных записей в date_value (новая база)')
                        data_old = data[data["id_counter"] != 0 ][["id_counter","DATE_VAL","VALUE_ZN","EMPTY","LINE_STATE"]]
                        print(data_old.shape[0])
                        data_old.rename(columns = {'VALUE_ZN':'impulse_count'}, inplace = True) 
                        data_old.rename(columns = {'DATE_VAL':'date_value'}, inplace = True) 
                        data_old.rename(columns = {'EMPTY':'empty'}, inplace = True) 
                        data_old.rename(columns = {'LINE_STATE':'line_control'}, inplace = True)
                        #logging.info(data_old[data_old['impulse_count'].isnull].shape[0])
                        #data_old[['impulse_count']]=data_old[['impulse_count']].astype(int) 
                        #data_old = data_old.astype({'impulse_count': np.int8})

                        try:
                            data_old.to_sql('indication',con=engine_new,schema='tmp',index =False,if_exists ='append',method='multi')
                            logging.info('============= Date_value сохранено в новой БД')
                        except Exception as ex:
                            path = file_name1 + ".new.txt"
                            data_old.to_csv(path,sep=';')
                            print('date_value new все плохо')
                            print('Ошибка:'+str(ex))
                            logging.error("============= Date_value не сохранено в новой БД")
                            logging.error("============= " + str(ex))

                        del data_old
                    bad_count = data[data["id_counter"] == 0 ].shape[0]
                    if bad_count != 0:
                        path = file_name1 + '_bad_new.txt'
                        data[data["id_counter"] == 0 ].to_csv(path,sep=';')
                        logging.error(f"============= Имеются нераспознанные счетчики в Date_value (новая база) - {bad_count}")

            else:
                logging.info('============= Нет данных в date_value')

            del data

            # Теплосчетчики
            teplo.insert(1, "id_klemma", 0)
            teplo.insert(1, "id_counter", 0)
            print(teplo.shape)
            for j, pok in teplo.iterrows():
                ser_num = pok["SER_NUM"]
                
                # ищем счетчик в старой БД
                id_klemma=0
                counter = counters[(counters["id_entr_"] == id_entr)&
                    (counters["serial_number"] == ser_num)]
                if counter.shape[0] > 0:
                    id_klemma = counter.iloc[0]['id_klemma']
                    teplo["id_klemma"][j] =  id_klemma
                
                # ищем счетчик в новой БД
                id_counter=0
                counter = counters_new[(counters_new["id_complex"] == id_complex)&
                    (counters_new["serial_number"] == ser_num)]
                if counter.shape[0] > 0:
                    id_counter = counter.iloc[0]['id_counter']
                    teplo["id_counter"][j] =  id_counter

            if teplo.shape[0] != 0:
                logging.info(f'============= Имеется {teplo.shape[0]} записей в teplo_value')
                
                # Записываем показания в старую БД
                good_count = teplo[teplo["id_klemma"] != 0 ].shape[0]
                if good_count != 0:
                    logging.info(f'============= Имеется {good_count} распознанных записей в teplo_value')
                    teplo_old = teplo[teplo["id_klemma"] != 0 ][["id_klemma","DATE_VAL","ENERGY"]]
                    teplo_old.rename(columns = {'ENERGY':'value_zn'}, inplace = True) 
                    teplo_old.rename(columns = {'DATE_VAL':'date_val'}, inplace = True) 
                    teplo_old.insert(1, "empty", 0)
                    teplo_old.insert(1, "line_state", 0)
                    try:
                        teplo_old.to_sql('date_value',con=engine,schema='tmp',index =False,if_exists ='append',method='multi')
                        logging.info('============= Teplo_value сохранено в старой базе (energy)')
                    except:
                        path = file_name + '.teplo.txt'
                        teplo_old.to_csv(path,sep=';')
                        print('teplo date_value все плохо')
                        logging.error("============= Teplo_value не сохранено в старой базе(energy)")
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
                        teplo_old.to_sql('heat_indication',con=engine,schema='tmp',index =False,if_exists ='append',method='multi')
                        logging.info('============= Teplo_value сохранено в старой базе (all data)')
                    except Exception as ex:
                        path = file_name + '.teplo_all.txt'
                        teplo_old.to_csv(path,sep=';')
                        print('heat_indication все плохо' + str(ex))
                        logging.error("============= Teplo_value не сохранено в старой базе (all data)")
                        logging.error("============= " + str(ex))
                    del teplo_old
                
                bad_count = teplo[teplo["id_klemma"] == 0 ].shape[0]
                if bad_count != 0:                    
                    path = file_name + '.teplo_bad.txt'
                    teplo[teplo["id_klemma"] == 0 ].to_csv(path,sep=';')
                    logging.error(f"============= Имеются нераспознанные счетчики в teplo_value (старая база) - {bad_count}")
                
                # Записываем показания в новую БД
                if id_complex != 0:
                    good_count = teplo[teplo["id_counter"] != 0 ].shape[0]
                    if good_count != 0:
                        logging.info(f'============= Имеется {good_count} распознанных записей teplo_value в новой базе')
                        """
                        teplo_old = teplo[teplo["id_counter"] != 0 ][["id_counter","DATE_VAL","ENERGY"]]
                        teplo_old.rename(columns = {'ENERGY':'value_indication'}, inplace = True) 
                        teplo_old.rename(columns = {'DATE_VAL':'date_value'}, inplace = True) 
                        teplo_old.insert(1, "empty", 0)
                        teplo_old.insert(1, "line_state", 0)
                        try:
                            teplo_old.to_sql('indication',con=engine_new,schema='tmp',index =False,if_exists ='append',method='multi')
                            logging.info('============= Teplo_value сохранено в новой базе (energy)')
                        except:
                            path = file_name1 +'.teplo_new.txt'
                            teplo_old.to_csv(path,sep=';')
                            print('teplo date_value все плохо new')
                            logging.error("============= Teplo_value не сохранено в новой базе (energy)")
                        del teplo_old
                        """

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
                            teplo_old.to_sql('heat_indication',con=engine_new,schema='tmp',index =False,if_exists ='append',method='multi')
                            logging.info('============= Teplo_value сохранено в новой базе (all data)')
                        except Exception as ex:   
                            print('heat_indication все плохо new')
                            print(ex)
                            path = file_name1 +'.teplo_all.new.txt'
                            teplo_old.to_csv(path,sep=';')
                            logging.error("============= Teplo_value не сохранено в новой базе(all data)")
                            logging.error("============= " + str(ex))
                        del teplo_old
                    
                    bad_count = teplo[teplo["id_counter"] == 0 ].shape[0]
                    if bad_count != 0:                    
                        path = file_name1 +'.teplo_bad.new.txt'
                        teplo[teplo["id_counter"] == 0 ].to_csv(path,sep=';')
                        logging.error(f"============= Имеются нераспознанные счетчики в teplo_value (новая база) - {bad_count}")
                
            else:
                logging.info('============= Нет данных в teplo_value')

            del teplo

logging.info("Конец работы программы")


"""
all_files = glob.glob(path + "/*.csv") 
li = [] 
for filename in all_files: 
df = pd.read_csv(filename,sep=';', index_col=None, header=0,encoding='latin-1') 
#df = pd.read_csv(filename, sep='\t', index_col=None, header=0) 
li.append(df) 
ERP_Data = pd.concat(li, axis=0, ignore_index=True) 
# rename the columns name 
ERP_Data.columns = ['Client_ID', 'Client_Name', 'FORME_JURIDIQUE_CLIENT', 'CODE_ACTIVITE_CLIENT', 'LIB_ACTIVITE_CLIENT', 'NACE', 'Company_Type', 'Number_of_Collected_Bins', 'STATUT_TI', 'TYPE_TI', 'HEURE_PASSAGE_SOUHAITE', 'FAMILLE_AFFAIRE', 'CODE_AFFAIRE_MOUVEMENT', 'TYPE_MOUVEMENT_PIECE', 'Freq_Collection', 'Waste_Type', 'CDNO', 'CDQTE', 'BLNO', 'Collection_Date', 'Weight_Ton', 'Bin_Capacity', 'REF_SS_REF_CONTENANT_BL', 'REF_DECHET_PREVU_TI', 'Site_ID', 'Site_Name', 'Street', 'ADRCPL1_SITE', 'ADRCPL2_SITE', 'Post_Code', 'City', 'Country','ZONE_POLYGONE_SITE' ,'OBSERVATION_SITE', 'OBSERVATION1_SITE', 'HEURE_DEBUT_INTER_MATIN_SITE', 'HEURE_FIN_INTER_MATIN_SITE', 'HEURE_DEBUT_INTER_APREM_SITE', 'HEURE_DEBUT_INTER_APREM_SITE', 'JOUR_PASSAGE_INTERDIT', 'PERIODE_PASSAGE_INTERDIT', 'JOUR_PASSAGE_IMPERATIF', 'PERIODE_PASSAGE_IMPERATIF'] # extracting specific columns 
Client_Table=ERP_Data[['Client_ID','Client_Name','NACE']].copy() 
# removing duplicate rows 
Client_Table1=Client_Table.drop_duplicates(subset=[ "Client_ID","Client_Name" , "NACE"])

"""
