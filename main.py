from timer_generator import init_timer, stop_timer, add_timer_listener
from md_client import MDClient
from bar_processor import BarProcessor
from time import sleep
import postgresql
import configparser
import os.path

gMD_client = None
gBarProcessor = None
gDatabase = None
gDatabasePath = None
gType_id = None
gServer = None
gPort = None
gINI = 'my.ini'

def init():
    global gMD_client, gBarProcessor, gDatabase, gINI, gType_id, gServer, gPort
    defaul_db_path = 'pq://postgres:password@db_server:5432/bar_service_base'

    if os.path.isfile(gINI):
        config = configparser.ConfigParser()
        config.read(gINI)
        gType_id = 0 if not config.has_option('settings', 'Type_id') else config.getint('settings', 'Type_id')
        gServer = 'localhost' if not config.has_option('settings', 'Server') else config.get('settings', 'Server')
        gPort = 2016 if not config.has_option('settings', 'Port') else config.get('settings', 'Port')
        gDatabasePath = defaul_db_path if not config.has_option('settings', 'DB') else config.get('settings', 'DB')
    else:
        gType_id = 0
        gServer = 'localhost'
        gPort = 2016
        gDatabasePath = defaul_db_path


    init_timer()

    gDatabase = postgresql.open(gDatabasePath)

    gBarProcessor = BarProcessor(gDatabase)
    gBarProcessor.start()
    add_timer_listener(gBarProcessor.timer_callback)

    gMD_client = MDClient(gBarProcessor.add_tick, gServer, gPort)
    gMD_client.start()

    #foreach ticker from db
    for ticker, time_quant in gDatabase.query('SELECT ticker,time_quant FROM bar_collector where type_id = ' + str(gType_id) + ' and ab_bar=1;'):
        lTablename = 'ab_bar_' + ticker.lower()
        print( ticker + '\t' + lTablename)
        ltb = gDatabase.query("SELECT lower(tablename) FROM pg_tables where tablename='"+lTablename+"';")
        if len(ltb) == 0:
            lSQL = "CREATE TABLE \"" + lTablename + "\" ( datetime timestamp without time zone NOT NULL, ask real, bid real, CONSTRAINT \""+lTablename+"_pkey\" PRIMARY KEY (datetime)) WITH (OIDS=FALSE);"
            print(lSQL)
            gDatabase.execute(lSQL)
            lSQL = "ALTER TABLE \"" + lTablename + "\" OWNER TO postgres;"
            # print(lSQL)
            gDatabase.execute(lSQL)

        gMD_client.subscribeMD(ticker)

    for ticker, time_quant in gDatabase.query('SELECT ticker,time_quant FROM bar_collector where type_id = ' + str(gType_id) + ';'):
        lTablename = 'bar_' + ticker.lower()
        print( ticker + '\t' + lTablename)
        ltb = gDatabase.query("SELECT lower(tablename) FROM pg_tables where tablename='"+lTablename+"';")
        if len(ltb) == 0:
            lSQL = "CREATE TABLE \"" + lTablename + "\" ( datetime timestamp without time zone NOT NULL, open real, high real, low real, close real, volume real, trades integer, vmp real, CONSTRAINT \""+lTablename+"_pkey\" PRIMARY KEY (datetime)) WITH (OIDS=FALSE);"
            # print(lSQL)
            gDatabase.execute(lSQL)
            lSQL = "ALTER TABLE \"" + lTablename + "\" OWNER TO postgres;"
            # print(lSQL)
            gDatabase.execute(lSQL)

        gMD_client.subscribe(ticker)

def main_loop():
    while True:
        sleep(1)

def stop_all():
    global gMD_client, gBarProcessor, gDatabase
    print("stopping")

    gMD_client.stop()
    gMD_client.join()

    gBarProcessor.stop()
    gBarProcessor.join()

    gDatabase.close()
    stop_timer()


try:
    init()
    main_loop()
finally:
    stop_all()
