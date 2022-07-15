import logging

import azure.functions as func
# import snowflake - in snowflake deatils need to pass in 3 things
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
        password='BrokenLaptop123',
        account='mc24391.switzerland-north.azure'
    )

    mycursor = con.cursor()
    print("Connection eshtablished with Snwoflake and got the cursor")
    ###CONNECTION ESTABLISHED

    ###GETTING INPUT BODY FROM THE PUT REQUEST
    originalID = req.get_json().get('OriginalID')
    newName = req.get_json().get('NewName')
    newDoB = req.get_json().get('NewDoB')    
                                                        
    finalResponse = {} #global string to hold the final formulated response  

    ### PROCESSING THE INPUT PARAMETERS FROM THE PUT REQUEST 
    if originalID: ##if ID is provided
        
        try: ##checking IF ID EXISTS IN THE TABLE
            mycursor.execute("USE WAREHOUSE project_warehouse")
            mycursor.execute("USE DATABASE project_database")
            mycursor.execute("USE SCHEMA project_schema")
            mycursor.execute (f"SELECT * FROM EmployeeData WHERE ID = '{originalID}'")
          
            output = {}
            for (ID, Name, DoB) in mycursor:
                output['ID'] = ID
                output['Name'] = Name
                output['DoB'] = DoB     
            
            if len(output)!=3:
                finalResponse['error_code'] = 508
                finalResponse['message'] = (f"PUT request unsuccessful, no entry with ID: {originalID} exists in the table!")
                return func.HttpResponse(json.dumps(finalResponse)) 
            
            else:
                if newName and newDoB: ##both have to be provided to proceed
                    
                    if validate_date(newDoB):
                        mycursor.execute (f"UPDATE EmployeeData SET Name = '{newName}', DoB = '{newDoB}'  WHERE ID = {originalID}")
                        finalResponse['status_code'] = 204
                        finalResponse['message'] = "PUT REQUEST succesful. Record updated!"
                        return func.HttpResponse(json.dumps(finalResponse))
                    
                    else:
                        finalResponse['error_code'] = 510
                        finalResponse['message'] = "PUT request unsuccessful, (Date Of Birth) DoB is not in DD/MM/YYYY format"
                        return func.HttpResponse(json.dumps(finalResponse)) 
                    
                else:
                    finalResponse['error_code'] = 509
                    finalResponse['message'] = "PUT request unsuccessful, both the new Name and new DoB need to be included in the request body!"
                    return func.HttpResponse(json.dumps(finalResponse))               
        finally: 
            mycursor.close()
    else:
        finalResponse['error_code'] = 507
        finalResponse['message'] = "THIS IS A CUSOTM ENDPOINT MADE BY GAVIN executed successfully. Pass in the ID number of the table entry you wish to edit  using the tag 'originalID' along with the new name 'newName' and the new Date Of Birth 'newDoB'!"
        return func.HttpResponse(json.dumps(finalResponse))         
    
    
    
    
    
    
    