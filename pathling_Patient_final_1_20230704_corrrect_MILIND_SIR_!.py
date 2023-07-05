
import requests
import json
from pathling import PathlingContext
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from sqlalchemy.dialects.postgresql import insert



#%%

resourceArray = ["Patient"]


#%%

engine = create_engine('postgresql+psycopg2://eform_user:eform%40123$@3.22.85.62:5432/realtime_export_fhirtest2')
conn = engine.connect()

#%%
  
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
# table_name = 'patient'
# column_name = 'source_resource_id, version'

# # Create the primary key constraint using an ALTER TABLE statement
# try:
#     with engine.connect() as conn:
#         alter_table_query = f"ALTER TABLE {table_name} ADD PRIMARY KEY ({column_name})"
#         conn.execute(alter_table_query)
#     print("Primary key created successfully!")

# except Exception as e:
#     print(f"Error creating primary key: {str(e)}")
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
data = []
base_url='https://fhirtest.intercorpvt.com:18000/fhir/'
#base_url='https://fhirdemo.intercorpvt.com:18000/fhir/'
#base_url='http://3.145.77.124:8000/fhir/'
#output_file='c:\\python_data\\APatient1.json'
output_file='out.json'
# resourceName = resourceArray[0]
pc = PathlingContext.create()

for resourceName in resourceArray:
    

    url = f"{base_url}{resourceName}?_count=250"
    headers = {'Accept': 'application/fhir+json'}
    response = requests.get(url, headers=headers)
    
    #bundle = json.loads(response.text)
    #entries = bundle.get("entry", [])

    f = open(output_file, "w", encoding="utf-8")
    f.write(response.text)
    f.close()
    
    # Read the Bundle into Spark.
    
    bundle = pc.spark.read.text(output_file, wholetext=True)
    
    # Encode it using Pathling.
    EncodedBundle = pc.encode_bundle(bundle, resourceName)
    # JSON is the default format, XML Bundles can be encoded using input type.
    # patients = pc.encodeBundle(bundles, 'Patient', inputType=MimeType.FHIR_XML)
  
    pt = EncodedBundle.schema
    schema = json.loads(EncodedBundle.schema.json())
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
    #print("resource====>",resourceName)
    # print("finalListForIndividualResource=====>",finalListForIndividualResource)
    #print("finalDictionary===>",finalDictionary)
    
    column1 = pd.json_normalize(finalDictionary).columns.tolist()
    #print(column1)
    #from pyspark.sql import SparkSession

#%%
#selected_columns = columnlist[0:10] + columnlist[10:20]+columnlist[20:30]+columnlist[30:34]+columnlist[41:118]+columnlist[127:139]+columnlist[149:167]+columnlist[167:177]+columnlist[183:197]
selected_columns = columnlist[0:10]+columnlist[10:15]+columnlist[16:22]+columnlist[23:34]+columnlist[42:58]+columnlist[60:101]+columnlist[102:118]+columnlist[127:140]+columnlist[149:177]+columnlist[183:184]+columnlist[185:199]
#t=EncodedBundle.select((selected_columns))
#print(t.columns)

new_columns = selected_columns.copy()
#new_columns.extend(additional_col)
new_columns = [item.replace('.', '_').lower() for item in new_columns]


data_df = EncodedBundle.select(selected_columns).toPandas()

data_df.columns = new_columns

data_df['version']= data_df['meta_versionid']
data_df['source_resource_id'] = 'Patient/'+ data_df['id'].astype(str)

for col in data_df.columns:
    data_df[col]=data_df[col].apply(lambda y: None if str(y)=='[]' else y)

#data_df = data_df.drop(['index'], axis=1).copy()
#%%

#data_df.to_csv('c:\\python_data\\A_1data_df.csv')

load_df(data_df, 'patient',conn)
        
#%%




conn.close()
#data_df.to_sql('patient', conn, if_exists='append', chunksize=4096, method=insert_on_duplicate)
#%%


