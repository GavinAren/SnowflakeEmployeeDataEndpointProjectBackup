import logging

import azure.functions as func
# import snowflake - in snowflake deatils need to pass in 3 things
import snowflake.connector
import json


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        
        #establish connection to snowflake
        con = snowflake.connector.connect(
            user='gavinaren',
            password='BrokenLaptop123',
            account='mc24391.switzerland-north.azure'
        )

        mycursor = con.cursor()
        print("got the cursor")

        print("Reading from the employee table..")
        print("_________________")
        finalResponse = ""
        try:
            mycursor.execute("USE WAREHOUSE project_warehouse")
            mycursor.execute("USE DATABASE project_database")
            mycursor.execute("USE SCHEMA project_schema")
            mycursor.execute ("SELECT Name FROM EMPLOYEEDATA")
            for row in mycursor.fetchall():           
                finalResponse += (json.dumps(row).strip('[""]')) + " "
            return func.HttpResponse(finalResponse)
        finally: 
            mycursor.close()
        
        return func.HttpResponse(f"Hello, {name}. CUSTOM MESSAGE WRITTEN BY GAVIN.")

    else:
        return func.HttpResponse(
             "THIS IS CUSTom ENDPOINT MADE FOR GAVIN executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
# GET - Given a query param, eg emplyee ID, should return employee details, return from snowflake
# POST - insert details of a new employee (name, dob, emp no.) insert into snowflakwe table
# SET - Update employee details in the snowflake table
# DELETE - delete employee details from snowflake table