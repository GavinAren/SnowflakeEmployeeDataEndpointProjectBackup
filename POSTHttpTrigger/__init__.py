import logging

import azure.functions as func
import snowflake.connector
import json
from datetime import datetime

def validate_dateandtime(date_text):
        try:
            datetime.strptime(date_text, '%Y-%m-%dT%H:%M:%S')
            return True
        except ValueError:
            return False
        
def validate_date(date_text):
        try:
            datetime.strptime(date_text, '%d/%m/%Y')
            return True
        except ValueError:
            return False

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

### ESTABLISHING SNOWFLAKE CONNECTION AND USING THE RIGHT TABLE
        
    con = snowflake.connector.connect(
        user='gavinaren',
        password='BrokenLaptop123', #need to implement keyvault
        account='mc24391.switzerland-north.azure'
    )

    mycursor = con.cursor()
    print("Connection eshtablished with Snowflake and got the cursor")
    ###CONNECTION ESTABLISHED

    ###GETTING INPUT PARAMETERS FROM THE GET REQUEST  
    inpID = req.get_json().get('ID')
    inpName = req.get_json().get('name')
    inpDoB = req.get_json().get('DoB')
    finalResponse = {} #global string to hold the final formulated response
        
    if inpID: 
        try:
            mycursor.execute("USE WAREHOUSE project_warehouse")
            mycursor.execute("USE DATABASE project_database")
            mycursor.execute("USE SCHEMA project_schema")
            mycursor.execute (f"SELECT * FROM EmployeeData WHERE ID = '{inpID}'")
          
            output = {}
            for (ID, Name, DoB) in mycursor:
                output['ID'] = ID
                output['Name'] = Name
                output['DoB'] = DoB     
            
            if len(output)>2:
                finalResponse['error_code'] = 511
                finalResponse['message'] = (f"POST request unsuccessful, an entry already exists for ID: {inpID}, please choose a new ID! ")
                return func.HttpResponse(json.dumps(finalResponse))         
            
            else:
                if not inpName: 
                    finalResponse['error_code'] = 503
                    finalResponse['message'] = "No 'name' provided in the request body, POST request unsuccesfull!"
                    return func.HttpResponse(json.dumps(finalResponse))
            
                elif not inpDoB: 
                    finalResponse['error_code'] = 504
                    finalResponse['message'] = "No (Date Of Birth) 'DoB' provided in the request body, POST request unsuccesfull!"
                    return func.HttpResponse(json.dumps(finalResponse))
                
                elif not validate_date(inpDoB):
                    finalResponse['error_code'] = 505
                    finalResponse['message'] = "(Date Of Birth) DoB is not in DD/MM/YYYY format, POST request unsuccesfull!"
                    return func.HttpResponse(json.dumps(finalResponse))  
                
                else:
                    mycursor.execute (f"INSERT INTO project_schema.EmployeeData(ID, Name, DoB) VALUES ({inpID}, '{inpName}', '{inpDoB}') ")
                    
                    finalResponse['status_code'] = 202
                    finalResponse['message'] = f"POST request successful. Insertion of following entry ID: {inpID},Name: {inpName}, DoB: {inpDoB} successful!"
                    return func.HttpResponse(json.dumps(finalResponse))
    
        finally: 
            mycursor.close()
    else:
        finalResponse['error_code'] = 502
        finalResponse['message'] = "No 'ID' provided in the request body, POST request unsuccesfull!"
        return func.HttpResponse(json.dumps(finalResponse))