# -*- coding: utf-8 -*-
"""
Created on Tue Jul 11 21:25:58 2023

@author: kanupriyag
"""


import requests
import json
from pathling import PathlingContext
import pandas as pd
from sqlalchemy import create_engine
import os
import psycopg2
from sqlalchemy.dialects.postgresql import insert

#%%

resourceArray = ["Consent"]
columns_in_error={'Consent':['identifier.type.coding.id',
 'identifier.type.coding.system',
 'identifier.type.coding.version',
 'identifier.type.coding.code',
 'identifier.type.coding.display',
 'identifier.type.coding.userSelected',
 'scope.coding.userSelected',
 'category.coding.id',
 'category.coding.system',
 'category.coding.version',
 'category.coding.code',
 'category.coding.display',
 'category.coding.userSelected',
 'policyRule.coding.userSelected',
 'provision.actor.role.coding.id',
 'provision.actor.role.coding.system',
 'provision.actor.role.coding.version',
 'provision.actor.role.coding.code',
 'provision.actor.role.coding.display',
 'provision.actor.role.coding.userSelected',
 'provision.action.coding.id',
 'provision.action.coding.system',
 'provision.action.coding.version',
 'provision.action.coding.code',
 'provision.action.coding.display',
 'provision.action.coding.userSelected',
 'provision.code.coding.id',
 'provision.code.coding.system',
 'provision.code.coding.version',
 'provision.code.coding.code',
 'provision.code.coding.display',
 'provision.code.coding.userSelected',
 'provision.provision.actor.id',
 'provision.provision.actor.role',
 'provision.provision.actor.role.id',
 'provision.provision.actor.role.coding',
 'provision.provision.actor.role.coding.id',
 'provision.provision.actor.role.coding.system',
 'provision.provision.actor.role.coding.version',
 'provision.provision.actor.role.coding.code',
 'provision.provision.actor.role.coding.display',
 'provision.provision.actor.role.coding.userSelected',
 'provision.provision.actor.role.text',
 'provision.provision.actor.reference',
 'provision.provision.actor.reference.reference',
 'provision.provision.actor.reference.display',
 'provision.provision.action.id',
 'provision.provision.action.coding',
 'provision.provision.action.coding.id',
 'provision.provision.action.coding.system',
 'provision.provision.action.coding.version',
 'provision.provision.action.coding.code',
 'provision.provision.action.coding.display',
 'provision.provision.action.coding.userSelected',
 'provision.provision.action.text',
 'provision.provision.securityLabel.id',
 'provision.provision.securityLabel.system',
 'provision.provision.securityLabel.version',
 'provision.provision.securityLabel.code',
 'provision.provision.securityLabel.display',
 'provision.provision.securityLabel.userSelected',
 'provision.provision.purpose.id',
 'provision.provision.purpose.system',
 'provision.provision.purpose.version',
 'provision.provision.purpose.code',
 'provision.provision.purpose.display',
 'provision.provision.purpose.userSelected',
 'provision.provision.class.id',
 'provision.provision.class.system',
 'provision.provision.class.version',
 'provision.provision.class.code',
 'provision.provision.class.display',
 'provision.provision.class.userSelected',
 'provision.provision.code.id',
 'provision.provision.code.coding',
 'provision.provision.code.coding.id',
 'provision.provision.code.coding.system',
 'provision.provision.code.coding.version',
 'provision.provision.code.coding.code',
 'provision.provision.code.coding.display',
 'provision.provision.code.coding.userSelected',
 'provision.provision.code.text',
 'provision.provision.data.id',
 'provision.provision.data.meaning',
 'provision.provision.data.reference',
 'provision.provision.data.reference.reference',
 'provision.provision.data.reference.display',
 'provision.provision.provision.id',
 'provision.provision.provision.type',
 'provision.provision.provision.period',
 'provision.provision.provision.period.id',
 'provision.provision.provision.period.start',
 'provision.provision.provision.period.end',
 'provision.provision.provision.actor',
 'provision.provision.provision.actor.id',
 'provision.provision.provision.actor.role',
 'provision.provision.provision.actor.role.id',
 'provision.provision.provision.actor.role.coding',
 'provision.provision.provision.actor.role.coding.id',
 'provision.provision.provision.actor.role.coding.system',
 'provision.provision.provision.actor.role.coding.version',
 'provision.provision.provision.actor.role.coding.code',
 'provision.provision.provision.actor.role.coding.display',
 'provision.provision.provision.actor.role.coding.userSelected',
 'provision.provision.provision.actor.role.text',
 'provision.provision.provision.actor.reference',
 'provision.provision.provision.actor.reference.reference',
 'provision.provision.provision.actor.reference.display',
 'provision.provision.provision.action',
 'provision.provision.provision.action.id',
 'provision.provision.provision.action.coding',
 'provision.provision.provision.action.coding.id',
 'provision.provision.provision.action.coding.system',
 'provision.provision.provision.action.coding.version',
 'provision.provision.provision.action.coding.code',
 'provision.provision.provision.action.coding.display',
 'provision.provision.provision.action.coding.userSelected',
 'provision.provision.provision.action.text',
 'provision.provision.provision.securityLabel',
 'provision.provision.provision.securityLabel.id',
 'provision.provision.provision.securityLabel.system',
 'provision.provision.provision.securityLabel.version',
 'provision.provision.provision.securityLabel.code',
 'provision.provision.provision.securityLabel.display',
 'provision.provision.provision.securityLabel.userSelected',
 'provision.provision.provision.purpose',
 'provision.provision.provision.purpose.id',
 'provision.provision.provision.purpose.system',
 'provision.provision.provision.purpose.version',
 'provision.provision.provision.purpose.code',
 'provision.provision.provision.purpose.display',
 'provision.provision.provision.purpose.userSelected',
 'provision.provision.provision.class',
 'provision.provision.provision.class.id',
 'provision.provision.provision.class.system',
 'provision.provision.provision.class.version',
 'provision.provision.provision.class.code',
 'provision.provision.provision.class.display',
 'provision.provision.provision.class.userSelected',
 'provision.provision.provision.code',
 'provision.provision.provision.code.id',
 'provision.provision.provision.code.coding',
 'provision.provision.provision.code.coding.id',
 'provision.provision.provision.code.coding.system',
 'provision.provision.provision.code.coding.version',
 'provision.provision.provision.code.coding.code',
 'provision.provision.provision.code.coding.display',
 'provision.provision.provision.code.coding.userSelected',
 'provision.provision.provision.code.text',
 'provision.provision.provision.dataPeriod',
 'provision.provision.provision.dataPeriod.id',
 'provision.provision.provision.dataPeriod.start',
 'provision.provision.provision.dataPeriod.end',
 'provision.provision.provision.data',
 'provision.provision.provision.data.id',
 'provision.provision.provision.data.meaning',
 'provision.provision.provision.data.reference',
 'provision.provision.provision.data.reference.reference',
 'provision.provision.provision.data.reference.display',
 'provision.provision.provision.provision',
 'provision.provision.provision.provision.id',
 'provision.provision.provision.provision.type',
 'provision.provision.provision.provision.period',
 'provision.provision.provision.provision.period.id',
 'provision.provision.provision.provision.period.start',
 'provision.provision.provision.provision.period.end',
 'provision.provision.provision.provision.actor',
 'provision.provision.provision.provision.actor.id',
 'provision.provision.provision.provision.actor.role',
 'provision.provision.provision.provision.actor.role.id',
 'provision.provision.provision.provision.actor.role.coding',
 'provision.provision.provision.provision.actor.role.coding.id',
 'provision.provision.provision.provision.actor.role.coding.system',
 'provision.provision.provision.provision.actor.role.coding.version',
 'provision.provision.provision.provision.actor.role.coding.code',
 'provision.provision.provision.provision.actor.role.coding.display',
 'provision.provision.provision.provision.actor.role.coding.userSelected',
 'provision.provision.provision.provision.actor.role.text',
 'provision.provision.provision.provision.actor.reference',
 'provision.provision.provision.provision.actor.reference.reference',
 'provision.provision.provision.provision.actor.reference.display',
 'provision.provision.provision.provision.action',
 'provision.provision.provision.provision.action.id',
 'provision.provision.provision.provision.action.coding',
 'provision.provision.provision.provision.action.coding.id',
 'provision.provision.provision.provision.action.coding.system',
 'provision.provision.provision.provision.action.coding.version',
 'provision.provision.provision.provision.action.coding.code',
 'provision.provision.provision.provision.action.coding.display',
 'provision.provision.provision.provision.action.coding.userSelected',
 'provision.provision.provision.provision.action.text',
 'provision.provision.provision.provision.securityLabel',
 'provision.provision.provision.provision.securityLabel.id',
 'provision.provision.provision.provision.securityLabel.system',
 'provision.provision.provision.provision.securityLabel.version',
 'provision.provision.provision.provision.securityLabel.code',
 'provision.provision.provision.provision.securityLabel.display',
 'provision.provision.provision.provision.securityLabel.userSelected',
 'provision.provision.provision.provision.purpose',
 'provision.provision.provision.provision.purpose.id',
 'provision.provision.provision.provision.purpose.system',
 'provision.provision.provision.provision.purpose.version',
 'provision.provision.provision.provision.purpose.code',
 'provision.provision.provision.provision.purpose.display',
 'provision.provision.provision.provision.purpose.userSelected',
 'provision.provision.provision.provision.class',
 'provision.provision.provision.provision.class.id',
 'provision.provision.provision.provision.class.system',
 'provision.provision.provision.provision.class.version',
 'provision.provision.provision.provision.class.code',
 'provision.provision.provision.provision.class.display',
 'provision.provision.provision.provision.class.userSelected',
 'provision.provision.provision.provision.code',
 'provision.provision.provision.provision.code.id',
 'provision.provision.provision.provision.code.coding',
 'provision.provision.provision.provision.code.coding.id',
 'provision.provision.provision.provision.code.coding.system',
 'provision.provision.provision.provision.code.coding.version',
 'provision.provision.provision.provision.code.coding.code',
 'provision.provision.provision.provision.code.coding.display',
 'provision.provision.provision.provision.code.coding.userSelected',
 'provision.provision.provision.provision.code.text',
 'provision.provision.provision.provision.dataPeriod',
 'provision.provision.provision.provision.dataPeriod.id',
 'provision.provision.provision.provision.dataPeriod.start',
 'provision.provision.provision.provision.dataPeriod.end',
 'provision.provision.provision.provision.data',
 'provision.provision.provision.provision.data.id',
 'provision.provision.provision.provision.data.meaning',
 'provision.provision.provision.provision.data.reference',
 'provision.provision.provision.provision.data.reference.reference',
 'provision.provision.provision.provision.data.reference.display']}
base_url='http://3.136.23.81:8000/fhir/'
output_file='out.json'
pc = PathlingContext.create()
engine = create_engine('postgresql+psycopg2://eform_user:eform%40123$@3.22.85.62:5432/realtime_export_fhirtest2', pool_recycle=3600)
conn = engine.connect()

# df.to_sql('patient', conn, if_exists='append', chunksize=4096, method=insert_on_duplicate)
# # Save the DataFrame to a text file
# df.to_txt('c:\\python_data\\AA.txt', index=False)
def postgres_upsert(table, conn, keys, data_iter):
    

    data = [dict(zip(keys, row)) for row in data_iter]
    #print(f"{table.table.name}_pkey")

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)
def load_df(df1,table_name,conn):
    
    # Insert data into PostgreSQL database using pandas
    try:
        
        df1.to_sql(
            name=table_name,
            con=conn,
            if_exists='append',
            index=False,
            chunksize=4096, 
            method=postgres_upsert
        )
        
        
    except psycopg2.Error as error:
        print(f"PostgreSQL Error: {error}")


#%%
table_name = 'consent'
column_name = 'source_resource_id, version'

# Create the primary key constraint using an ALTER TABLE statement
try:
    with engine.connect() as conn:
        alter_table_query = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({column_name})"
        conn.execute(alter_table_query)
    print("Primary key created successfully!")

except Exception as e:
    print(f"Error creating primary key: {str(e)}")
#%%
# On duplicate update
#https://stackoverflow.com/questions/30337394/pandas-to-sql-fails-on-duplicate-primary-key          
#%%


def get_resource_columns(data, Valtype, Valstr, ConcatStr,fhirPathValstr,ConcatStr1):
    queryFile=[]
    for key, value in data.items():
        if isinstance(value, dict):
            # print (str(key)+'->'+str(value)+ "*")
            if (str(key) == 'type'):
                Valtype = 'conct'
            else:
                Valtype = 'skip'
            get_resource_columns(value, Valtype, Valstr, ConcatStr,fhirPathValstr,ConcatStr1)
        elif isinstance(value, list):
            if (str(key) == 'fields'):
                ConcatStr = Valstr
                ConcatStr1 = fhirPathValstr
            else:
                ConcatStr = ''
                ConcatStr1 = ''
            for val in value:
                typeOfVal = ""
                try:
                    # print("val====>", val['name'], "val type====>", val["type"]["type"])
                    trial = val["type"]["type"]
                    typeOfVal = 'json'
                    trial = 'BLOB'
                except:
                    # print("val====>", val['name'], "val type====>", val["type"])
                    # trial = val["type"]
                    if (val["type"] == 'string'):
                        typeOfVal = 'varchar'
                        trial='STRING'
                    elif (val["type"] == 'array'):
                        typeOfVal = 'ARRAY'
                        trial = 'BLOB'
                    elif (val["type"] == 'boolean'):
                        typeOfVal = 'bool'
                        trial = 'BOOLEAN'
                    elif (val["type"] == 'struct'):
                        typeOfVal = 'json'
                        trial = 'BLOB'
                    elif (val["type"] == 'integer'):
                        typeOfVal = 'int'
                        trial = 'INT'
                    else:
                        typeOfVal = 'varchar'
                        trial = 'STRING'
                if isinstance(val, str):
                    pass
                elif isinstance(val, list):
                    pass
                else:

                    if (Valtype == 'conct'):
                        if (ConcatStr == ""):
                            Valstr = val['name']
                            fhirPathValstr = val['name']
                        else:
                            Valstr = ConcatStr + "_" + val['name']
                            fhirPathValstr = ConcatStr1 + "." + val['name']
                    else:
                        if (ConcatStr == ""):
                            Valstr = val['name']
                            fhirPathValstr = val['name']
                        else:
                            Valstr = ConcatStr + "_" + val['name']
                            fhirPathValstr = ConcatStr1 + "." + val['name']
                    finalListForIndividualResource=[]
                    newfhirPathValstr = resourceName+"." + fhirPathValstr
                    finalListForIndividualResource.append(Valstr)
                    finalDictionary[str(Valstr)] = typeOfVal
                    columnlist.append(fhirPathValstr)

                    # for toinsert dictionary
                    toinsert = {"columnName": "", "fhirPath": "", "columnType": ""}

                    toinsert["columnName"]=Valstr
                    toinsert["fhirPath"]=newfhirPathValstr
                    toinsert["fhir_Path"]=fhirPathValstr
                    # toinsert["columnType"]=typeOfVal
                    toinsert["columnType"] = trial
                    # print("toinsert==>",toinsert)
                    if(typeOfVal == 'json'):
                        toinsert["columnName"] = Valstr
                        toinsert["fhirPath"] = newfhirPathValstr + ".first()"
                        toinsert["columnType"] = "STRING"
                        print("toinsert==>", toinsert)
                        queryFile.append(toinsert)

                    if (trial != 'BLOB'):
                        queryFile.append(toinsert)
                    # print(Valstr)
                    get_resource_columns(val, Valtype, Valstr, ConcatStr,fhirPathValstr,ConcatStr1)

#func1(data, Valtype, Valstr, ConcatStr,fhirPathValstr,ConcatStr1)


#%%

# data = []
# #base_url='https://fhirtest.intercorpvt.com:18000/fhir/'
# #base_url='https://fhirdemo.intercorpvt.com:18000/fhir/'
# #base_url='http://3.145.76.255:8000/fhir/'
# base_url='http://3.136.23.81:8000/fhir/'
# output_file='c:\\python_data\\Aobservation12.json'
# # resource = resourceArray[0]
# resourceArray = ["Consent"]
for resourceName in resourceArray:
    tableName = resourceName.lower()
    page_url = f"{base_url}{resourceName}"
    headers = {'Accept': 'application/fhir+json'}
    data = {"resourceType": "Bundle", "entry": []}
        
    # response = requests.get(page_url, headers=headers)
    # f = open(output_file, "w", encoding="utf-8")
    # f.write(response.text)
    # f.close()
    
    while page_url:
        #print("Getting URL "+ page_url)
        response = requests.get(page_url, headers=headers)

        if response.status_code != 200:
            print(f"Failed to retrieve data for {resourceName}. Status code: {response.status_code}")
            break

        json_bundle = json.loads(response.text)
        entries = json_bundle.get("entry", [])
        data["entry"].extend(entries)

        # Check if there are more pages
        link_header = json_bundle.get("link", [])
        next_url = None
        for link in link_header:
            if link.get("relation") == "next":
                next_url = link.get("url")
                break

        # Update the page URL for the next iteration
        page_url = next_url
        # Save the data as a  JSON file
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        
        
        # Read the Bundle into Spark.    
        bundle = pc.spark.read.text(output_file, wholetext=True)
       


    # url = f"{base_url}{resourceName}?_count=8000"
    # headers = {'Accept': 'application/fhir+json'}
    # response = requests.get(url, headers=headers)
    
    # bundle = json.loads(response.text)
    # entries = bundle.get("entry", [])

    # f = open(output_file, "w", encoding="utf-8")
    # f.write(response.text)
    # f.close()
    
    # # Read the Bundle into Spark.
    # pc = PathlingContext.create()
    # bundle = pc.spark.read.text(output_file, wholetext=True)
    
    # Encode it using Pathling.
    EncodedBundle = pc.encode_bundle(bundle, resourceName)
    schema = json.loads(EncodedBundle.schema.json())
    #EncodedBundle = pc.encode_bundle(bundle, resourceName)
    # JSON is the default format, XML Bundles can be encoded using input type.
    # patients = pc.encodeBundle(bundles, 'Patient', inputType=MimeType.FHIR_XML)
  #%%  
    #pt = EncodedBundle.schema
    #schema = json.loads(EncodedBundle.schema.json())
    # with open('c://python_data//BA_1_3_4utput.json', 'w') as f:
    #     json.dump(schema, f)
    
    finalDictionary = {}
    
    
    data = schema
    Valstr = ""
    Valtype = ""
    ConcatStr = ""
    fhirPathValstr=""
    ConcatStr1=""
    columnlist = []
    
    get_resource_columns(data, Valtype, Valstr, ConcatStr,fhirPathValstr,ConcatStr1)
    print("resource====>",resourceName)
    # print("finalListForIndividualResource=====>",finalListForIndividualResource)
    print("finalDictionary===>",finalDictionary)
    
    column1 = pd.json_normalize(finalDictionary).columns.tolist()
    print(column1)
    #from pyspark.sql import SparkSession
#%%
#selected_columns =columnlist[262:263]+columnlist[252:257]+columnlist[242:243]+columnlist[235:236]+columnlist[228:229]+columnlist[221:222]+columnlist[211:212]+columnlist[178:197]+columnlist[147:172]+columnlist[134:141]+columnlist[108:128]+columnlist[0:35]+columnlist[41:60]+columnlist[61:65]+columnlist[71:107]
#not_selected_columns = [column for column in columnlist if column not in selected_columns]

    selected_columns = [x for x in columnlist if x not in columns_in_error[resourceName]]
    
    new_columns = selected_columns.copy()
    #new_columns.extend(additional_col)
    new_columns = [item.replace('.', '_').lower() for item in new_columns]
    
    
    
    data_df = EncodedBundle.select(selected_columns).toPandas()
    
    data_df.columns = new_columns
    
    data_df['version']= data_df['meta_versionid']
    data_df['source_resource_id'] = 'Patient/'+ data_df['id'].astype(str)
    for col in data_df.columns:
        data_df[col]=data_df[col].apply(lambda y: [ele for ele in y if ele != []] if type(y) == list else y)
        data_df[col]=data_df[col].apply(lambda y: None if str(y)=='[]' else y)
        data_df[col]=data_df[col].apply(lambda y: None if str(y)=='[None]' else y)
    load_df(data_df, tableName, conn)
    
    # for col in data_df.columns:
    #     data_df[col]=data_df[col].apply(lambda y: None if str(y)=='[]' else y)
    
    #data_df = data_df.drop(['index'], axis=1).copy(0
    
    
    
    
    #%%
    #data_df.to_csv('c:\\python_data\\Aobservation1b12data_df.csv')
    
    #conn.close()
    
    
        
