import logging

import azure.functions as func
import snowflake.connector
import json

##UNABLE TO DELETE UNIQUE ROWS RIGHT NOW------------
##SHOULD MAKE ID UNIQUE AT LEAST 
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

    ###GETTING INPUT PARAMETERS FROM THE DELETE REQUEST
    inpID = req.get_json().get('ID')
    inpName = req.get_json().get('name')
    inpDoB = req.get_json().get('DoB')
    finalResponse = {} #global string to hold the final formulated response  

    ### PROCESSING THE INPUT PARAMETERS FROM THE DELETE REQUEST 
    if inpName: # IF INPUT NAME IS PROVIDED :

        try:
            mycursor.execute("USE WAREHOUSE project_warehouse")
            mycursor.execute("USE DATABASE project_database")
            mycursor.execute("USE SCHEMA project_schema")
            mycursor.execute (f"SELECT * FROM EmployeeData WHERE Name =  '{inpName}' ")

            output = {}
            for (ID, Name, DoB) in mycursor:
                output['ID'] = ID
                output['Name'] = Name
                output['DoB'] = DoB     
            
            if len(output)!=3:
                finalResponse['error_code'] = 506
                finalResponse['message'] = "DELETE request unsuccessful, no such entry exists in the table!"
                return func.HttpResponse(json.dumps(finalResponse)) 
            else:
                mycursor.execute (f"DELETE FROM EmployeeData WHERE Name ='{inpName}' ")
                finalResponse['status_code'] = 203
                finalResponse['message'] = "DELETE request successful!"
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
            mycursor.execute (f"SELECT * FROM EmployeeData WHERE ID =  '{inpID}' ") 
            
            output = {}
            for (ID, Name, DoB) in mycursor:
                output['ID'] = ID
                output['Name'] = Name
                output['DoB'] = DoB               
            
            if len(output)!=3:
                finalResponse['error_code'] = 506
                finalResponse['message'] = "DELETE request unsuccessful, no such entry exists in the table!"
                return func.HttpResponse(json.dumps(finalResponse)) 
            else:
                mycursor.execute (f"DELETE FROM EmployeeData WHERE ID ='{inpID}' ")
                finalResponse['status_code'] = 203
                finalResponse['message'] = "DELETE request successful!"
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
            mycursor.execute (f"SELECT * FROM EmployeeData WHERE DoB =  '{inpDoB}' ") 
            
            output = {}
            for (ID, Name, DoB) in mycursor:
                output['ID'] = ID
                output['Name'] = Name
                output['DoB'] = DoB            
            
            if len(output)!=3:
                finalResponse['error_code'] = 506
                finalResponse['message'] = "DELETE request unsuccessful, no such entry exists in the table!"
                return func.HttpResponse(json.dumps(finalResponse)) 
            else:
                mycursor.execute (f"DELETE FROM EmployeeData WHERE Name ='{inpDoB}' ")
                finalResponse['status_code'] = 203
                finalResponse['message'] = "DELETE request successful!"
                finalResponse['output'] = output
                return func.HttpResponse(json.dumps(finalResponse))
        finally: 
            mycursor.close()
            
        return func.HttpResponse(f"Hello, {inpID}, CUSTOM MESSAGE")
    
    else:
        return func.HttpResponse(
             "THIS IS A CUSTOM DELETION ENDPOINT MADE BY GAVIN executed unsuccessfully. Pass an employee Name OR id in the query string or in the request body for a personalized response.",
             status_code=200
        )
        
