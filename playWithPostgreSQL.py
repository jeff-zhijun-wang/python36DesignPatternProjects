import sys
from configparser import ConfigParser
import json, ast, yaml, psycopg2
from shapely.geometry import Point, LineString, Polygon
from shapely import wkb
import shapely.wkt
#import GeoPandas
import os

#This is added to test
# it is used to create yaml file header when input yml file does not exist
from copy import deepcopy

def config(filename='postgreSQL_database.ini', section='postgresql'):
    '''Reads the configuration file and returns a dictionary of the parameters

    Parameters
    ----------
    filename, optional
    	The name of the configuration file.
    section, optional
    	The section name in the .ini file.

    Returns
    -------
    	A dictionary of the parameters in the config file.

    '''
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def connect(tablename, configFile=''):
    '''
    Connect to postgresql table and return a list of the records. When no record, then return []
    Parameters
    ----------
    tablename
    	the name of the table to be queried
    configFile
    	the name of the configuration file.

    Returns
    -------
    	A list of tuples.

    '''
    conn = None
    try:
        # read connection parameters
        params = config(filename=configFile)

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        _sqlcommand = "SELECT ST_AsText(geom) FROM {table}".format(table=tablename)
#        _sqlcommand = "SELECT geom FROM {table}".format(table=tablename)
        cur.execute(_sqlcommand)
        # print("The number of parts: ", cur.rowcount)
        for columnDesc in cur.description:  #namedTuple
            print(type(columnDesc), columnDesc, columnDesc.name, columnDesc.type_code)
        rows = cur.fetchall()
       # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
    return rows #in case of no record, return []



if __name__ == '__main__':
    if len(sys.argv) == 1:
        print("Please add the configuration name as well")
        print("for example:")
        print("createYMLfromPostgresql.py postgreSQL_database.ini")
    else:
        configFilename=sys.argv[1]
        tableList=["towns_subset"]
        for x in tableList:
            data = connect(x, configFile=configFilename)
            print("There are {0} active rows in {1}!".format(len(data),x))
            for rowdata in data:
                polys=shapely.wkt.loads(rowdata[0])
                listpolygons = list(polys)
                if len(listpolygons) == 1:
                    for lp in listpolygons:
                        print(lp)





