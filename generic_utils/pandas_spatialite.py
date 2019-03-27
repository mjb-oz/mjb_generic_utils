import numpy as np
import pandas as pd
import geopandas as gpd
import sqlite3
import shapely

def DFtoDB(df, con, tablename, epsg = None):
    '''
    This function takes a dataframe or geodataframe, a connection object to a spatialite database, and a name for the new table (as a string),
    and creates a new table in database with the data of the dataframe.
    
    There are many small convenience functions contained within, some are more compliated string manipulation for creating the tables
    and inserting the values, others are very thing wrappers arouund single lines of SQL.
    '''
    
    def isna(obj):
        if isinstance(obj, float):
            return np.isnan(obj)
        else:
            return False
    
    def createTable(df, tablename, columns = 'all'):
        '''
        Create a new SQLite table with the correct datatypes to reflect all the existing data in the dataframe.
        This is designed to work for non-geometry columns, but this is assumed, rather than explict in this function.
        The construction of the columns variable for the function calls (when called below), explictly exclude geometry
        '''
        if columns == 'all':
            columns = df.columns
        # Lookup of pandas dtypes to SQLite dtypes
        dtype_mapping = {'int' : 'INTEGER', 'float': 'REAL', 'object':'TEXT', 'date':'DATETIME'}
        #start of the table creation string
        s = df.index.name + ' INTEGER PRIMARY KEY NOT NULL'
        # Loop through all the columns in the dataframe, ignore if not in the specified list, and find its SQLite dtype
        for col, dtype in df.dtypes.iteritems():
            if col not in columns:
                continue
            for key, value in dtype_mapping.items():
                if key in str(dtype):
                    dbdtype = value
                    break
            # Add the column name and the SQL dtype to the string
            s += ', ' + col + ' ' + dbdtype
        # Prepend the SQL keywords to the string
        maketable_string = 'CREATE TABLE ' + tablename + '(' + s + ')'
        print(maketable_string)
        cur.execute(maketable_string)
        con.commit()
        
    def createIndex(df, tablename):
        make_pk_index_string = 'CREATE INDEX ' + tablename + '_' + df.index.name + '_idx ON ' + tablename + '(' + df.index.name + ')'
        print(make_pk_index_string)
        cur.execute(make_pk_index_string)  
        con.commit()

    def populateTable(df, tablename, columns = 'all'):
        '''
        This code populates a newly created table with the data from the corresponding dataframe.
        Done as a loop, one row at a time.
        If the value is a shapely geometry (ie geopandas geometry), save the values as the well known text shape format
        '''
        print('Inserting rows into %s' %(tablename))
        # Define columns if not passed explictly
        if columns == 'all':
            columns = df.columns
        # Do some funky string manip to get it SQL ready
        cols = df.index.name + ', ' + str(list(columns)).replace('[','').replace(']','').replace("'",'')
        # Loop through all the columns for each row, testing the inputs.
        # If it's not a string, turn it into one, if it is, wrap it in quotes
        # Add the result onto the string for execution.
        for i, row in df[columns].iterrows():
            vals = [str(i)]
            ncols = len(columns) + 1
            for col, val in row.iteritems():
                if isinstance(val, str):
                    vals.append(val) # needs quotes around it?
                elif isna(val):
                    vals.append('NULL')
                elif val is None:
                    vals.append('NULL')
                elif 'shapely' in str(type(val)):
                    vals.append(val.wkt)
                else:
                    vals.append(str(val))
            qmarks = str([',?' if i > 0 else '?' for i in range(ncols)])[1:-1].replace("'",'').replace(', ','')
            s = 'INSERT INTO ' + tablename + ' (' + cols + ') VALUES (' + qmarks + ')'
            # print(s, vals)
            cur.execute(s, vals)
            
        con.commit()

    def makeGeomColumn(df, tablename, geom_col):
        '''
        Makes a geometry column based on the type of geometry and CRS within the dataframe.
        '''
        geomtype = df.iloc[0]['temp_geom'].wkt.split(' ')[0].strip()
        crs = df.crs['init'].split(':')[1]
        makegeomcol_string = 'SELECT AddGeometryColumn("' + tablename + '","' + geom_col + '",' + str(crs) + ',"' + geomtype + '","XY");'
        print(makegeomcol_string)
        cur.execute(makegeomcol_string)       
        con.commit()
    
    def createGeometries(df, tablename, geom_col):
        '''
        Creates the actual geometries from the temp_geom columns WKT format geometries
        '''
        crs = df.crs['init'].split(':')[1]
        makeshapes = 'UPDATE ' + tablename + ' SET ' + geom_col + '=GeomFromText(temp_geom,' + crs + ')'
        print(makeshapes)
        cur.execute(makeshapes)
        con.commit()
        
    def insertColumnInOtherTable(existing_table, table_tochange, column_name):
        '''
        Copies the values from one column into another table.
        Assumes the destination table column already exists and is identically named.
        '''
        new_col_string = 'UPDATE ' + table_tochange + ' SET ' + column_name + ' =  (SELECT ' + column_name + ' FROM ' + existing_table + ')'
        print(new_col_string)
        cur.execute(new_col_string)
        con.commit()
        
    def makeSpatialIndex(tablename, geom_col):
        '''
        Creates a spatial index on a table with spatial geometries.
        '''
        makespatialindex = 'SELECT CreateSpatialIndex("' + tablename + '","' + geom_col + '")'
        print(makespatialindex)
        cur.execute(makespatialindex)          
        con.commit()
    
    def dropTable(tablename):
        '''
        Drop a table from the database
        '''
        drop_temp_table_string = 'DROP TABLE ' + tablename
        print(drop_temp_table_string)
        cur.execute(drop_temp_table_string)
    
    def updateTable(df, tablename, columns):
        '''
        Adds the data from the dataframe to the table name. Only operates on the columns specificed
        '''
        print('Updating %s in %s' %(columns, tablename))
        for rownum, row in df.iterrows():
            update_string = ''
            vals = []
            for col in columns:
                val = row[col]
                if isinstance(val, str):
                    vals.append(val)
                elif isna(val):
                    vals.append('NULL')
                elif val is None:
                    vals.append('NULL')
                else:
                    vals.append(str(val))
                if update_string == '':
                    update_string = 'UPDATE ' + tablename + ' SET ' + col + ' = ?'
                else:
                    update_string += ', ' + col + ' =  ?'

            update_string += ' WHERE ' + df.index.name + ' = ' + str(rownum)
            # print(update_string)
            cur.execute(update_string, vals)
            
    def intraSQLinsert(src_table, dst_table, columns):
        cols = str(columns).replace('[','').replace(']','').replace("'",'')
        insert_string = 'INSERT INTO '+ dst_table + ' (' + cols + ') SELECT ' + cols + ' FROM ' + src_table
        print(insert_string)
        cur.execute(insert_string)

    # Test if the geodataframe or a normal dataframe.
    # If its a geodataframe, check the geometry and CRS are valid
    # If so, move the geometry to a temp column and save the name for future reassignment.
    if isinstance(df, gpd.GeoDataFrame):
        # save this name for use later on
        geom_col = df.geometry.name
        if not isinstance(df[geom_col], gpd.GeoSeries):
            raise Exception('GeoDataFrame geometry column is not setup correctly')
        elif df.crs is None:
            raise Exception('GeoDataFrame coordinate reference system is not setup correctly')
        else:
            isSpatial = True
            df = df.rename(columns = {geom_col:'temp_geom'}).set_geometry('temp_geom')
        if epsg is not None:
            df.crs['init'] = 'epsg:' + str(epsg)
        try: 
            epsg = df.crs['init'].split(':')[1]
        except KeyError:
            raise Exception('The CRS are present, but no EPSG code is defined. Please establish correct EPSG and pass to function')
    elif isinstance(df, pd.DataFrame):
        isSpatial = False
    else:
        raise Exception('Please pass a valid pandas Dataframe or a valid Geopandas GeoDataFrame as the first argument')        
        
    # Create cursor object
    cur = con.cursor()
    # Name the df.index if it isn't already named
    if df.index.name is None:
        print('DataFrame index/primary key renamed to "OID"')
        df.index.name = "OID"
    # Make the destination table, excluding any geometry columns
    good_table_cols = [col for col in df.columns if col != 'temp_geom']
    createTable(df, tablename, good_table_cols)
    createIndex(df, tablename)
    # If not spatial, populate the table completely now.
    # Note: doing this before getting the final geometries into the table seemed to cause errors with the index when read in Arc.
    if not isSpatial:
        populateTable(df, tablename, good_table_cols)   

    if isSpatial:
        # Make a temporary table, with only the primary key and temp geom (ie well known text)
        temp_table_name = tablename + "_temp"
        temp_table_cols = ['temp_geom'] # the index/primary key is also done automatically
        createTable(df,  temp_table_name, temp_table_cols)
        populateTable(df, temp_table_name, temp_table_cols)
        # Make a geomcolumn from temp table
        makeGeomColumn(df, temp_table_name, geom_col)
        # Populate geometry column in temp table
        createGeometries(df, temp_table_name, geom_col)
        # Make a geom column in actual table
        makeGeomColumn(df, tablename, geom_col)
        # Populate actual table geometry and primary key from temp table geometry
        intraSQLinsert(temp_table_name, tablename, ['OID','geometry'])
#         cur.execute('INSERT INTO test(OID, geometry) SELECT OID, geometry from test_temp')
        # Now add all the non-spatial data into the table
        updateTable(df, tablename, good_table_cols)
        # drop temp table
        dropTable(temp_table_name)
        # make SpatialIndex
        # makeSpatialIndex(tablename, geom_col)   
    con.commit()
    cur.close()