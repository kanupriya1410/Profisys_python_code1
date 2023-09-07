# -*- coding: utf-8 -*-
"""
Created on Tue Jul 18 16:51:04 2023

@author: kanupriyag
"""

"""
Created on Wed May 24 12:51:59 2023

@author: Kanupriya


This code gets FHIR data from the URL and uploads that to FHIR datalake
It uses Pathlink python repository to get columns understood and then inserted
"""
import requests
import json
from pathling import PathlingContext
#import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from sqlalchemy.dialects.postgresql import insert




#%%

#resourceArray = ["Consent"]
resourceArray = ["Patient"]
columns_in_errors = {'Patient':['identifier.type.coding.id',
                      'identifier.type.coding.system',
                      'identifier.type.coding.version',
                      'identifier.type.coding.code',
                      'identifier.type.coding.display',
                     'identifier.type.coding.userSelected',
                      'identifier.type.text',
                      'contact.relationship.id',
                      'contact.relationship.coding',
                      'contact.relationship.coding.id',
                      'contact.relationship.coding.system',
                      'contact.relationship.coding.version',
                      'contact.relationship.coding.code',
                      'contact.relationship.coding.display',
                      'contact.relationship.coding.userSelected',
                      'contact.relationship.text',
                      'contact.telecom.id',
                      'contact.telecom.system',
                      'contact.telecom.value',
                      'contact.telecom.use',
                      'contact.telecom.rank',
                      'contact.telecom.period',
                      'contact.telecom.period.id',
                      'contact.telecom.period.start',
                      'contact.telecom.period.end',
                      'communication.language.coding.id',
                      'communication.language.coding.system',
                      'communication.language.coding.version',
                      'communication.language.coding.code',
                      'communication.language.coding.display',
                      'communication.language.coding.userSelected'],
                    'Observation':['identifier.type.coding.id',
                      'identifier.type.coding.system',
                      'identifier.type.coding.version',
                      'identifier.type.coding.code',
                      'identifier.type.coding.display',
                      'identifier.type.coding.userSelected',
                      'category.coding.id',
                      'category.coding.system',
                      'category.coding.version',
                      'category.coding.code',
                      'category.coding.display',
                      'category.coding.userSelected',
                      'code.coding.userSelected',
                      'valueCodeableConcept.coding.userSelected',
                      'interpretation.coding.id',
                      'interpretation.coding.system',
                      'interpretation.coding.version',
                      'interpretation.coding.code',
                      'interpretation.coding.display',
                      'interpretation.coding.userSelected',
                      'referenceRange.type.coding.id',
                      'referenceRange.type.coding.system',
                      'referenceRange.type.coding.version',
                      'referenceRange.type.coding.code',
                      'referenceRange.type.coding.display',
                      'referenceRange.type.coding.userSelected',
                      'referenceRange.appliesTo.id',
                      'referenceRange.appliesTo.coding',
                      'referenceRange.appliesTo.coding.id',
                      'referenceRange.appliesTo.coding.system',
                      'referenceRange.appliesTo.coding.version',
                      'referenceRange.appliesTo.coding.code',
                      'referenceRange.appliesTo.coding.display',
                      'referenceRange.appliesTo.coding.userSelected',
                      'referenceRange.appliesTo.text',
                      'component.code.coding.id',
                      'component.code.coding.system',
                      'component.code.coding.version',
                      'component.code.coding.code',
                      'component.code.coding.display',
                      'component.code.coding.userSelected',
                      'component.valueBoolean',
                      'component.valueCodeableConcept.coding.id',
                      'component.valueCodeableConcept.coding.system',
                      'component.valueCodeableConcept.coding.version',
                      'component.valueCodeableConcept.coding.code',
                      'component.valueCodeableConcept.coding.display',
                      'component.valueCodeableConcept.coding.userSelected',
                      'component.valueInteger',
                      'component.valueQuantity.value_scale',
                      'component.valueQuantity._value_canonicalized.value',
                      'component.valueQuantity._value_canonicalized.scale',
                      'component.valueRange.low.value_scale',
                      'component.valueRange.low._value_canonicalized.scale',
                      'component.valueRange.high.value_scale',
                      'component.valueRange.high._value_canonicalized.scale',
                      'component.valueRatio.numerator.value',
                      'component.valueRatio.numerator.value_scale',
                      'component.valueRatio.numerator._value_canonicalized.scale',
                      'component.valueRatio.denominator.value_scale',
                      'component.valueRatio.denominator._value_canonicalized.scale',
                      'component.valueSampledData.origin.value_scale',
                      'component.valueSampledData.origin._value_canonicalized.scale',
                      'component.valueSampledData.period_scale',
                      'component.valueSampledData.factor_scale',
                      'component.valueSampledData.lowerLimit_scale',
                      'component.valueSampledData.upperLimit_scale',
                      'component.valueSampledData.dimensions',
                      'component.dataAbsentReason.coding',
                      'component.dataAbsentReason.coding.id',
                      'component.dataAbsentReason.coding.system',
                      'component.dataAbsentReason.coding.version',
                      'component.dataAbsentReason.coding.code',
                      'component.dataAbsentReason.coding.display',
                      'component.dataAbsentReason.coding.userSelected',
                      'component.interpretation',
                      'component.interpretation.id',
                      'component.interpretation.coding',
                      'component.interpretation.coding.id',
                      'component.interpretation.coding.system',
                      'component.interpretation.coding.version',
                      'component.interpretation.coding.code',
                      'component.interpretation.coding.display',
                      'component.interpretation.coding.userSelected',
                      'component.interpretation.text',
                      'component.referenceRange',
                      'component.referenceRange.id',
                      'component.referenceRange.low',
                      'component.referenceRange.low.id',
                      'component.referenceRange.low.value',
                      'component.referenceRange.low.value_scale',
                      'component.referenceRange.low.comparator',
                      'component.referenceRange.low.unit',
                      'component.referenceRange.low.system',
                      'component.referenceRange.low.code',
                      'component.referenceRange.low._value_canonicalized',
                      'component.referenceRange.low._value_canonicalized.value',
                      'component.referenceRange.low._value_canonicalized.scale',
                      'component.referenceRange.low._code_canonicalized',
                      'component.referenceRange.high',
                      'component.referenceRange.high.id',
                      'component.referenceRange.high.value',
                      'component.referenceRange.high.value_scale',
                      'component.referenceRange.high.comparator',
                      'component.referenceRange.high.unit',
                      'component.referenceRange.high.system',
                      'component.referenceRange.high.code',
                      'component.referenceRange.high._value_canonicalized',
                      'component.referenceRange.high._value_canonicalized.value',
                      'component.referenceRange.high._value_canonicalized.scale',
                      'component.referenceRange.high._code_canonicalized',
                      'component.referenceRange.type',
                      'component.referenceRange.type.id',
                      'component.referenceRange.type.coding',
                      'component.referenceRange.type.coding.id',
                      'component.referenceRange.type.coding.system',
                      'component.referenceRange.type.coding.version',
                      'component.referenceRange.type.coding.code',
                      'component.referenceRange.type.coding.display',
                      'component.referenceRange.type.coding.userSelected',
                      'component.referenceRange.type.text',
                      'component.referenceRange.appliesTo',
                      'component.referenceRange.appliesTo.id',
                      'component.referenceRange.appliesTo.coding',
                      'component.referenceRange.appliesTo.coding.id',
                      'component.referenceRange.appliesTo.coding.system',
                      'component.referenceRange.appliesTo.coding.version',
                      'component.referenceRange.appliesTo.coding.code',
                      'component.referenceRange.appliesTo.coding.display',
                      'component.referenceRange.appliesTo.coding.userSelected',
                      'component.referenceRange.appliesTo.text',
                      'component.referenceRange.age',
                      'component.referenceRange.age.id',
                      'component.referenceRange.age.low',
                      'component.referenceRange.age.low.id',
                      'component.referenceRange.age.low.value',
                      'component.referenceRange.age.low.value_scale',
                      'component.referenceRange.age.low.comparator',
                      'component.referenceRange.age.low.unit',
                      'component.referenceRange.age.low.system',
                      'component.referenceRange.age.low.code',
                      'component.referenceRange.age.low._value_canonicalized',
                      'component.referenceRange.age.low._value_canonicalized.value',
                      'component.referenceRange.age.low._value_canonicalized.scale',
                      'component.referenceRange.age.low._code_canonicalized',
                      'component.referenceRange.age.high',
                      'component.referenceRange.age.high.id',
                      'component.referenceRange.age.high.value',
                      'component.referenceRange.age.high.value_scale',
                      'component.referenceRange.age.high.comparator',
                      'component.referenceRange.age.high.unit',
                      'component.referenceRange.age.high.system',
                      'component.referenceRange.age.high.code',
                      'component.referenceRange.age.high._value_canonicalized',
                      'component.referenceRange.age.high._value_canonicalized.value',
                      'component.referenceRange.age.high._value_canonicalized.scale',
                      'component.referenceRange.age.high._code_canonicalized',
                    'component.referenceRange.text'],'Consent':['identifier.type.coding.id',
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
 'provision.provision.provision.provision.data.reference.display']
                        
                  }
#%%

# Saving the dictionary to a file
file_path = r'columns_in_error.json'
with open(file_path, 'w') as file:
    json.dump(columns_in_errors, file)

# Loading the dictionary from the file
with open(file_path, 'r') as file:
    columns_in_error_loaded = json.load(file)
columns_in_error=columns_in_error_loaded

# Printing the loaded dictionary
print(columns_in_error)

#%%

#columns_in_error ='C:\Users\kanupriyag\Desktop\pathling\datalake-fhir-python/resouce_data.txt'
#base_url='https://fhirtest.intercorpvt.com:18000/fhir/'
#base_url='https://fhirdemo.intercorpvt.com:18000/fhir/'
base_url='http://3.136.23.81:8000/fhir/'
output_file='out.json'

inProduction = False

# This variable is used during testing to get only few pages worth of data
# Works only if inProduction  is false
fetchdata_pages = 2

pc = PathlingContext.create()


#%%

engine = create_engine('postgresql+psycopg2://eform_user:eform%40123$@3.22.85.62:5432/realtime_export_fhirtest2')
conn = engine.connect()


#%%
# On duplicate update
#https://stackoverflow.com/questions/30337394/pandas-to-sql-fails-on-duplicate-primary-key          

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
# To be run once to create primary key -
# This primary key is used to update record
# table_name = 'patient'
#table_name="observation
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
                        #print("toinsert==>", toinsert)
                        queryFile.append(toinsert)

                    if (trial != 'BLOB'):
                        queryFile.append(toinsert)
                    # print(Valstr)
                    get_resource_columns(val, Valtype, Valstr, ConcatStr,fhirPathValstr,ConcatStr1)


#%%
#data = []

# resourceName = resourceArray[0]
for resourceName in resourceArray:
    tableName = resourceName.lower()
    page_url = f"{base_url}{resourceName}"
    headers = {'Accept': 'application/fhir+json'}
    data = {"resourceType": "Bundle", "entry": []}
    pagenumber = 0
        
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
        pagenumber =+1
        
        if(inProduction == False and fetchdata_pages > pagenumber ):
            break
    
        
    # Save the data as a  JSON file
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    
    
    # Read the Bundle into Spark.    
    bundle = pc.spark.read.text(output_file, wholetext=True)
    
    # Encode it using Pathling.
    EncodedBundle = pc.encode_bundle(bundle, resourceName)
    schema = json.loads(EncodedBundle.schema.json())
    
    #temp_df = EncodedBundle.select('id', 'gender', 'birthDate',"meta").toPandas()

    finalDictionary = {}
    data = schema
    Valstr = ""
    Valtype = ""
    ConcatStr = ""
    fhirPathValstr=""
    ConcatStr1=""
    columnlist = []
      
    
    #This is a recursive function with no response. But output is in columnlist variable
    get_resource_columns(data, Valtype, Valstr, ConcatStr,fhirPathValstr,ConcatStr1)    
    #%%
    #Read columns in error from a file
    # select all columns except those in error
    selected_columns = [x for x in columnlist if x not in columns_in_error[resourceName]]
    new_columns = selected_columns.copy()
    new_columns = [item.replace('.', '_').lower() for item in new_columns]
    
    #EncodedBundle.printSchema()
    pd_df = EncodedBundle.select(selected_columns).toPandas()
    pd_df.columns = new_columns
     
    data_df = pd_df.copy()
    
    data_df['version']= data_df['meta_versionid']
    data_df['source_resource_id'] = 'Patient/'+ data_df['id'].astype(str)
    
    
    for col in data_df.columns:
        data_df[col]=data_df[col].apply(lambda y: [ele for ele in y if ele != []] if type(y) == list else y)
        data_df[col]=data_df[col].apply(lambda y: None if str(y)=='[]' else y)
        data_df[col]=data_df[col].apply(lambda y: None if str(y)=='[None]' else y)
        
    #data_df.to_csv('A_1data_df.csv')
    
    load_df(data_df, tableName, conn)
    

        
#%%
conn.close()
#%%


