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
    #inpEnd_IntervalDate = req.params.get('end_intervaldate')
    finalResponse = {} #global string to hold the final formulated response  

    ### PROCESSING THE INPUT PARAMETERS FROM THE GET REQUEST 
    if inpMeterNumber:  # checks if it is provided
        
        if inpStart_IntervalDate:# and inpEnd_IntervalDate: #Checks if both are provided in the first place
        
            if validate_date(inpStart_IntervalDate):
                
                #if validate_date(inpEnd_IntervalDate):

                try:
                    mycursor.execute("USE WAREHOUSE project_warehouse")
                    mycursor.execute("USE DATABASE project_database")
                    mycursor.execute("USE SCHEMA project_schema")
                    mycursor.execute (f"SELECT * FROM MeterData WHERE MeterNumber =  {inpMeterNumber} ")
                
                    output = {}
                    for (INT__00__VAL,INT__01__VAL,INT__02__VAL,INT__03__VAL,INT__04__VAL,INT__05__VAL,INT__06__VAL,INT__07__VAL,INT__08__VAL,INT__09__VAL,INT__10__VAL,INT__11__VAL,INT__12__VAL,INT__13__VAL,INT__14__VAL,INT__15__VAL,INT__16__VAL,INT__17__VAL,INT__18__VAL,INT__19__VAL,INT__20__VAL,INT__21__VAL,INT__22__VAL,INT__23__VAL,INT__24__VAL ) in mycursor:
                        output['INT__00__VAL'] = INT__00__VAL
                        output['INT__01__VAL'] = INT__01__VAL
                        output['INT__02__VAL'] = INT__02__VAL
                        output['INT__03__VAL'] = INT__03__VAL
                        output['INT__04__VAL'] = INT__04__VAL
                        output['INT__05__VAL'] = INT__05__VAL
                        output['INT__06__VAL'] = INT__06__VAL   
                        output['INT__07__VAL'] = INT__07__VAL
                        output['INT__08__VAL'] = INT__08__VAL
                        output['INT__09__VAL'] = INT__09__VAL
                        output['INT__10__VAL'] = INT__10__VAL
                        output['INT__11__VAL'] = INT__11__VAL
                        output['INT__12__VAL'] = INT__12__VAL
                        output['INT__13__VAL'] = INT__13__VAL
                        output['INT__14__VAL'] = INT__14__VAL
                        output['INT__15__VAL'] = INT__15__VAL
                        output['INT__16__VAL'] = INT__16__VAL
                        output['INT__17__VAL'] = INT__17__VAL
                        output['INT__18__VAL'] = INT__18__VAL
                        output['INT__19__VAL'] = INT__19__VAL
                        output['INT__20__VAL'] = INT__20__VAL
                        output['INT__21__VAL'] = INT__21__VAL
                        output['INT__22__VAL'] = INT__22__VAL
                        output['INT__23__VAL'] = INT__23__VAL
                        output['INT__24__VAL'] = INT__24__VAL
                        
                    
                    if len(output)<25:
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
        
