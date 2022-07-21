import logging
from wsgiref import validate

import azure.functions as func
# import snowflake - in snowflake deatils need to pass in 3 things
import snowflake.connector
from datetime import datetime
import json

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
        
    con = snowflake.connector.connect(
        user='gavinaren',
        password='BrokenLaptop123',
        account='mc24391.switzerland-north.azure'
    )

    mycursor = con.cursor()
    print("Connection eshtablished with Snwoflake and got the cursor")
    ###CONNECTION ESTABLISHED

    ###GETTING INPUT PARAMETERS FROM THE GET REQUEST
    inpMeterNumber = req.params.get('meterNumber')
    inpStart_IntervalDate = req.params.get('start_intervalDate')
    inpEnd_IntervalDate = req.params.get('end_intervalDate')
    finalResponse = {} #global string to hold the final formulated response  

    ### PROCESSING THE INPUT PARAMETERS FROM THE GET REQUEST 
    if inpMeterNumber:  # checks if it is provided
        
        if inpStart_IntervalDate:#  #Checks if both are provided in the first place
        
            if validate_date(inpStart_IntervalDate):
                
                if not inpEnd_IntervalDate:
                    inpEnd_IntervalDate = inpStart_IntervalDate
                    
                if validate_date(inpEnd_IntervalDate):

                    try:
                        mycursor.execute("USE WAREHOUSE project_warehouse")
                        mycursor.execute("USE DATABASE project_database")
                        mycursor.execute("USE SCHEMA project_schema")

                        
                        mycursor.execute (f"SELECT * FROM MeterData WHERE MeterNumber =  '{inpMeterNumber}' AND IntervalDate BETWEEN '{inpStart_IntervalDate}' AND '{inpEnd_IntervalDate}' ")
                        finalResponse['status_code'] = 202
                        finalResponse['message'] = "GET request successful!"
                        multiple = []
                        number = 0
                        
                        for row in mycursor.fetchall():    
                            output = {}          
                            metereventdatetime, meternumber, devicenumber,metertype,intervaldate, INT_00_VAL,INT_01_VAL,INT_02_VAL,INT_03_VAL,INT_04_VAL,INT_05_VAL,INT_06_VAL,INT_07_VAL,INT_08_VAL,INT_09_VAL,INT_10_VAL,INT_11_VAL,INT_12_VAL,INT_13_VAL,INT_14_VAL,INT_15_VAL,INT_16_VAL,INT_17_VAL,INT_18_VAL,INT_19_VAL,INT_20_VAL,INT_21_VAL,INT_22_VAL,INT_23_VAL,INT_24_VAL = row;
                            output['MeterNumber' ] = meternumber
                            output['IntervalDate'] = intervaldate
                            output['INT_00_VAL'] = INT_00_VAL
                            output['INT_01_VAL'] = INT_01_VAL
                            output['INT_02_VAL'] = INT_02_VAL
                            output['INT_03_VAL'] = INT_03_VAL
                            output['INT_04_VAL'] = INT_04_VAL
                            output['INT_05_VAL'] = INT_05_VAL
                            output['INT_06_VAL'] = INT_06_VAL   
                            output['INT_07_VAL'] = INT_07_VAL
                            output['INT_08_VAL'] = INT_08_VAL
                            output['INT_09_VAL'] = INT_09_VAL
                            output['INT_10_VAL'] = INT_10_VAL
                            output['INT_11_VAL'] = INT_11_VAL
                            output['INT_12_VAL'] = INT_12_VAL
                            output['INT_13_VAL'] = INT_13_VAL
                            output['INT_14_VAL'] = INT_14_VAL
                            output['INT_15_VAL'] = INT_15_VAL
                            output['INT_16_VAL'] = INT_16_VAL
                            output['INT_17_VAL'] = INT_17_VAL
                            output['INT_18_VAL'] = INT_18_VAL
                            output['INT_19_VAL'] = INT_19_VAL
                            output['INT_20_VAL'] = INT_20_VAL
                            output['INT_21_VAL'] = INT_21_VAL
                            output['INT_22_VAL'] = INT_22_VAL
                            output['INT_23_VAL'] = INT_23_VAL
                            output['INT_24_VAL'] = INT_24_VAL    
                                                
                            number += 1                                   
                            
                            multiple.insert(number, output)
                        finalResponse['output'] = multiple                       
                        if number <1: #if nothing is fetched then show error
                            finalResponse['error_code'] = 501
                            finalResponse['message'] = "GET request unsuccessful, no such entry exists in the table!"
                            return func.HttpResponse(json.dumps(finalResponse))                                 
                        
                        #else:
                        correctOrder = {}
                        correctOrder['status_code'] = 202
                        correctOrder['message'] = "GET request successful!" 
                        correctOrder['output']= finalResponse['output'] 
                        return func.HttpResponse(json.dumps(correctOrder ))                        
                        
                    finally: 
                        mycursor.close()
                else: 
                    finalResponse['error_code'] = 506
                    finalResponse['message'] = "GET request unsuccessful, end date needs to be in 'YYYY-MM-DD' format!"
                    return func.HttpResponse(json.dumps(finalResponse))                          
                
            else:
                finalResponse['error_code'] = 502
                finalResponse['message'] = "GET request unsuccessful, start date needs to be in 'YYYY-MM-DD' format!"
                return func.HttpResponse(json.dumps(finalResponse))                
        else:
            finalResponse['error_code'] = 503
            finalResponse['message'] = "GET request unsuccessful, no start date provided!"
            return func.HttpResponse(json.dumps(finalResponse))           
     
    else:
        return func.HttpResponse(
             "You have reached a custom endpoint executed successfully. Pass in a 'MeterNumber' along with a 'start_intervaldate' and 'end_intervaldate' in 'YYYY-MM-DD' format, in the input parameters with this GET request to receive the interval register for a personalized response.",
             status_code=200
        )
        
