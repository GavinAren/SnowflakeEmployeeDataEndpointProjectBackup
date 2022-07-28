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

    ###GETTING INPUT PARAMETERS FROM THE PUT REQUEST
    inpMeterNumber = req.get_json().get('MeterNumber')
    inpIntervalDate = req.get_json().get('IntervalDate')
    inpIntervalRegister = req.get_json().get('IntervalRegister')
    finalResponse = {} #global string to hold the final formulated response  

    ### PROCESSING THE INPUT PARAMETERS FROM THE PUT REQUEST 
    if inpMeterNumber:  # checks if it is provided
        
        if inpIntervalDate:#  #Checks if both are provided in the first place
        
            if validate_date(inpIntervalDate):
                try:
                    mycursor.execute("USE WAREHOUSE project_warehouse")

                    if not inpIntervalRegister:
                            finalResponse['error_code'] = 505
                            finalResponse['message'] = " No 'intervalRegister' list provided in the request body, UPDATE request unsuccessful!"
                            return func.HttpResponse(json.dumps(finalResponse))                                         
                    else:
                        intervalRegisterDict = inpIntervalRegister
                        print(intervalRegisterDict)
                        if len(intervalRegisterDict) < 25:
                            finalResponse['error_code'] = 506
                            finalResponse['message'] = " 'intervalRegister' list provided in the request body does not hold values for all comlunm INT_00_VAL to INT_24_VAL, UPDATE request unsuccessful!"
                            return func.HttpResponse(json.dumps(finalResponse))                                 
                        
                        else:
                            mycursor.execute ("UPDATE project_database.project_schema.MeterData SET "+
                                            f"INT_00_VAL = '{intervalRegisterDict['INT_00_VAL']}',INT_01_VAL = '{intervalRegisterDict['INT_01_VAL']}', INT_02_VAL= '{intervalRegisterDict['INT_02_VAL']}',INT_03_VAL = '{intervalRegisterDict['INT_03_VAL']}',INT_04_VAL = '{intervalRegisterDict['INT_04_VAL']}',INT_05_VAL = '{intervalRegisterDict['INT_05_VAL']}', INT_06_VAL = '{intervalRegisterDict['INT_06_VAL']}', INT_07_VAL = '{intervalRegisterDict['INT_07_VAL']}', INT_08_VAL = '{intervalRegisterDict['INT_08_VAL']}', INT_09_VAL = '{intervalRegisterDict['INT_09_VAL']}', INT_10_VAL = '{intervalRegisterDict['INT_10_VAL']}', INT_11_VAL = '{intervalRegisterDict['INT_11_VAL']}', INT_12_VAL = '{intervalRegisterDict['INT_12_VAL']}', INT_13_VAL = '{intervalRegisterDict['INT_13_VAL']}', INT_14_VAL = '{intervalRegisterDict['INT_14_VAL']}', INT_15_VAL = '{intervalRegisterDict['INT_15_VAL']}', INT_16_VAL = '{intervalRegisterDict['INT_16_VAL']}',INT_17_VAL = '{intervalRegisterDict['INT_17_VAL']}', INT_18_VAL = '{intervalRegisterDict['INT_18_VAL']}',INT_19_VAL = '{intervalRegisterDict['INT_19_VAL']}',INT_20_VAL = '{intervalRegisterDict['INT_20_VAL']}', INT_21_VAL = '{intervalRegisterDict['INT_21_VAL']}',INT_22_VAL = '{intervalRegisterDict['INT_22_VAL']}',INT_23_VAL = '{intervalRegisterDict['INT_23_VAL']}', INT_24_VAL = '{intervalRegisterDict['INT_24_VAL']}' " + 
                                            f"WHERE MeterNumber = '{inpMeterNumber}' AND IntervalDate = '{inpIntervalDate}'")
                        
                            for row in mycursor.fetchall():                                           
                                
                                if row[0] <1: #if nothing is deleted then show error
                                    finalResponse['error_code'] = 501
                                    finalResponse['message'] = "UPDATE request unsuccessful, no such entry exists in the table!"
                                    return func.HttpResponse(json.dumps(finalResponse))                             

                                else:
                                    finalResponse['status_code'] = 202
                                    finalResponse['message'] = f"UPDATE request successful."
                                    return func.HttpResponse(json.dumps(finalResponse))                      
                    
                finally: 
                    mycursor.close() 
            else:
                finalResponse['error_code'] = 502
                finalResponse['message'] = "UPDATE request unsuccessful, start date needs to be in 'YYYY-MM-DD' format!"
                return func.HttpResponse(json.dumps(finalResponse))                
        else:
            finalResponse['error_code'] = 503
            finalResponse['message'] = "UPDATE request unsuccessful, no start date provided!"
            return func.HttpResponse(json.dumps(finalResponse))           
     
    else:
        return func.HttpResponse(
             "You have reached a custom endpoint executed successfully. Pass in a 'MeterNumber' along with a 'start_intervaldate' and 'end_intervaldate' in 'YYYY-MM-DD' format, in the input parameters with this UPDATE request to receive the interval register for a personalized response.",
             status_code=200
        )
        
