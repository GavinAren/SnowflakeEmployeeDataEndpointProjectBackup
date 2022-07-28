import logging
from wsgiref import validate

import azure.functions as func
# import snowflake - in snowflake deatils need to pass in 3 things
import snowflake.connector
from datetime import datetime
import json
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

def validate_dateandtime(date_text):
        try:
            datetime.strptime(date_text, '%Y-%m-%dT%H:%M:%S')
            return True
        except ValueError:
            return False
        
def validate_date(date_text):
        try:
            datetime.strptime(date_text, '%Y-%m-%d')
            return True
        except ValueError:
            return False
        
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    ### ESTABLISHING SNOWFLAKE CONNECTION AND USING THE RIGHT TABLE
        
    credential = DefaultAzureCredential(managed_identity_client_id = "47d7abef-645e-4f73-9e31-e9572d4cd420")
    secret_client = SecretClient(vault_url="https://gavinarenkeyvault.vault.azure.net/", credential=credential)

    con = snowflake.connector.connect(
        user=(secret_client.get_secret("snowflakeusername")).value,
        password=(secret_client.get_secret("snowflakepassword")).value, 
        account=(secret_client.get_secret("accountidentifier")).value
    )

    mycursor = con.cursor()
    print("Connection eshtablished with Snwoflake and got the cursor")
    ###CONNECTION ESTABLISHED

    ###GETTING INPUT PARAMETERS FROM THE DELETE REQUEST
    inpMeterNumber = req.get_json().get('meterNumber')
    finalResponse = {} #global string to hold the final formulated response  

    ### PROCESSING THE INPUT PARAMETERS FROM THE DELETE REQUEST 
    if inpMeterNumber:  # checks if it is provided
        try:
            mycursor.execute("USE WAREHOUSE project_warehouse")
            mycursor.execute("USE DATABASE project_database")
            mycursor.execute("USE SCHEMA project_schema")
        
            mycursor.execute (f"DELETE FROM MeterData WHERE MeterNumber =  '{inpMeterNumber}' ")                           
            for row in mycursor.fetchall():                
                #output['Number of Rows Deleted'] = row                                  
                #finalResponse['output'] = {'Number of Rows Deleted': row[0]}
                
                if row[0] <1: #if nothing is deleted then show error
                    finalResponse['error_code'] = 501
                    finalResponse['message'] = "DELETE request unsuccessful, no such entry exists in the table!"
                    return func.HttpResponse(json.dumps(finalResponse))                                 
                
                else:
                    finalResponse['status_code'] = 202
                    finalResponse['message'] = "DELETE request successful!" 
                    finalResponse['output']= (f"Number Of Rows Deleted: {row[0]}") 
                    return func.HttpResponse(json.dumps(finalResponse ))
            
        finally: 
            mycursor.close()
                
     
    else:
        return func.HttpResponse(
             "You have reached a custom endpoint executed successfully. Pass in a 'MeterNumber' with this DELETE request to receive the interval register for a personalized response.",
             status_code=200
        )
        
