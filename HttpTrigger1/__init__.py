import logging

import azure.functions as func
# import snowflake - in snowflake deatils need to pass in 3 things
import snowflake.connector
import json


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

    ###GETTING INPUT PARAMETERS FROM THE GET REQUEST
    inpName = req.params.get('name')
    inpID = req.params.get('ID')
    inpDoB = req.params.get('DoB')
    finalResponse = {} #global string to hold the final formulated response  
    
    if not inpName:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            inpName = req_body.get('name')

    if not inpID:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            inpID = req_body.get('ID')
            
    if not inpDoB:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            inpDoB = req_body.get('DoB')

    ### PROCESSING THE INPUT PARAMETERS FROM THE GET REQUEST 
    if inpName: # IF INPUT NAME IS PROVIDED :

        try:
            mycursor.execute("USE WAREHOUSE project_warehouse")
            mycursor.execute("USE DATABASE project_database")
            mycursor.execute("USE SCHEMA project_schema")
            mycursor.execute (f"SELECT * FROM EMPLOYEEDATA WHERE Name =  '{inpName}' ")
          
            output = {}
            for (ID, Name, DoB) in mycursor:
                output['ID'] = ID
                output['Name'] = Name
                output['DoB'] = DoB     
            
            if len(output)!=3:
                finalResponse['error_code'] = 501
                finalResponse['message'] = "GET request unsuccessful, no such entry exists in the table!"
                return func.HttpResponse(json.dumps(finalResponse)) 
            else:
                finalResponse['status_code'] = 202
                finalResponse['message'] = "GET request successful"
                finalResponse['output'] = output
                return func.HttpResponse(json.dumps(finalResponse))

        finally: 
            mycursor.close()
        return func.HttpResponse(f"Hello, {inpName}. CUSTOM MESSAGE WRITTEN BY GAVIN.")


    elif inpID: #IF ID PARAMETER IS PROVIDED:
        
        try:
            mycursor.execute("USE WAREHOUSE project_warehouse")
            mycursor.execute("USE DATABASE project_database")
            mycursor.execute("USE SCHEMA project_schema")
            mycursor.execute (f"SELECT * FROM EMPLOYEEDATA WHERE ID =  '{inpID}' ORDER BY Name") 
            
            output = {}
            for (ID, Name, DoB) in mycursor:
                output['ID'] = ID
                output['Name'] = Name
                output['DoB'] = DoB               
            
            if len(output)!=3:
                finalResponse['error_code'] = 501
                finalResponse['message'] = "GET request unsuccessful, no such entry exists in the table!"
                return func.HttpResponse(json.dumps(finalResponse)) 
            else:
                finalResponse['status_code'] = 202
                finalResponse['message'] = "GET request successful"
                finalResponse['output'] = output
                return func.HttpResponse(json.dumps(finalResponse))
            #for row in mycursor.fetchall():
            #    finalResponse += "\n" 
            #    for item in row:          
            #        finalResponse += (json.dumps(item).strip('[""]')) + " "
        
        finally: 
            mycursor.close()
            
        return func.HttpResponse(f"Hello, {inpID}, CUSTOM MESSAGE")

    elif inpDoB: #IF ID PARAMETER IS PROVIDED:
        
        try:
            mycursor.execute("USE WAREHOUSE project_warehouse")
            mycursor.execute("USE DATABASE project_database")
            mycursor.execute("USE SCHEMA project_schema")
            mycursor.execute (f"SELECT * FROM EMPLOYEEDATA WHERE DoB =  '{inpDoB}' ") 
            
            output = {}
            for (ID, Name, DoB) in mycursor:
                output['ID'] = ID
                output['Name'] = Name
                output['DoB'] = DoB            
            
            if len(output)!=3:
                finalResponse['error_code'] = 501
                finalResponse['message'] = "GET request unsuccessful, no such entry exists in the table!"
                return func.HttpResponse(json.dumps(finalResponse)) 
            else:
                finalResponse['status_code'] = 202
                finalResponse['message'] = "GET request successful!"
                finalResponse['output'] = output
                return func.HttpResponse(json.dumps(finalResponse))
        finally: 
            mycursor.close()
            
        return func.HttpResponse(f"Hello, {inpID}, CUSTOM MESSAGE")
    
    else:
        return func.HttpResponse(
             "THIS IS A CUSOTM ENDPOINT MADE BY GAVIN executed successfully. Pass an employee Name OR id in the query string or in the request body for a personalized response.",
             status_code=200
        )
        
''' INSTRUCTIONS
# GET - Given a query param, eg emplyee ID, should return employee details, return from snowflake
# POST - insert details of a new employee (inpName, dob, emp no.) insert into snowflakwe table
# SET - Update employee details in the snowflake table
# DELETE - delete employee details from snowflake table
'''
#OLD CODE TO DISPLAY DATA IN A READABLE FORMAT
#finalResponse += '{0} {1} {2}'.format(ID, Name, DoB) + "\n"
#return func.HttpResponse(json.dumps(finalResponse))