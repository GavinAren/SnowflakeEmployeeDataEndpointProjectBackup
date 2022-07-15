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
            datetime.strptime(date_text, '%Y-%m-%d')
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
    inpMeterEventDateTime = req.get_json().get('meterEventDateTime')
    inpMeterNumber = req.get_json().get('meterNumber')
    inpDeviceType = req.get_json().get('deviceType')
    inpMeterType = req.get_json().get('meterType')
    inpIntervalDate = req.get_json().get('intervalDate')
    inpIntervalRegister = req.get_json().get('intervalRegister')
    finalResponse = {} #global string to hold the final formulated response
        
    if inpMeterNumber: 
        
        if inpIntervalDate:
            
            if validate_date(inpIntervalDate):
        
                try:
                    mycursor.execute("USE WAREHOUSE project_warehouse")
                    mycursor.execute("USE DATABASE project_database")
                    mycursor.execute("USE SCHEMA project_schema")
                    mycursor.execute (f"SELECT * FROM MeterData WHERE MeterNumber =  {inpMeterNumber} AND IntervalDate = '{inpIntervalDate}' ")
                
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
                    
                    if len(output)>1:
                        finalResponse['error_code'] = 511
                        finalResponse['message'] = (f"POST request unsuccessful, an entry already exists for MeterNumber: {inpMeterNumber} at date : {inpIntervalDate}, please choose a new date or meter! ")
                        return func.HttpResponse(json.dumps(finalResponse))         
                    
                    else:
                        if not inpMeterEventDateTime: 
                            finalResponse['error_code'] = 503
                            finalResponse['message'] = "No 'meterEventDateTime' provided in the request body, POST request unsuccesfull!"
                            return func.HttpResponse(json.dumps(finalResponse))
                    
                        elif not validate_dateandtime(inpMeterEventDateTime): 
                            finalResponse['error_code'] = 504
                            finalResponse['message'] = " 'meterEventDateTime' is not in DD/MM/YYYY format, POST request unsuccesfull!"
                            return func.HttpResponse(json.dumps(finalResponse))
                        
                        elif not inpMeterType:
                            finalResponse['error_code'] = 505
                            finalResponse['message'] = "No 'meterType' provided in the request body, POST request unsuccesfull!"
                            return func.HttpResponse(json.dumps(finalResponse))  
                        
                        elif not inpDeviceType:
                            finalResponse['error_code'] = 505
                            finalResponse['message'] = " No 'deviceType' provided in the request body, POST request unsuccesfull!"
                            return func.HttpResponse(json.dumps(finalResponse))    
                                 
                        elif not inpIntervalRegister:
                            finalResponse['error_code'] = 505
                            finalResponse['message'] = " No 'intervalRegister' list provided in the request body, POST request unsuccesfull!"
                            return func.HttpResponse(json.dumps(finalResponse))                                         
                        else:
                            mycursor.execute (f"INSERT INTO project_schema.MeterData( MeterEventDateTime, MeterNumber, DeviceNumber, MeterType, IntervalDate, ) VALUES ('inpMeterEventDateTime', 'inpMeterNumber', 'inpDeviceNumber', 'inpMeterType', 'inpIntervalDate') ")
                            ##need to enumerate teh 24 values our of the last variable
                            ##could do IntervalRegister.get[] for each position
                            finalResponse['status_code'] = 202
                            finalResponse['message'] = f"POST request successful. Insertion of following entry ID: {inpID},Name: {inpName}, DoB: {inpDoB} successful!"
                            return func.HttpResponse(json.dumps(finalResponse))
            
                finally: 
                    mycursor.close()
            else:
                finalResponse['error_code'] = 504
                finalResponse['message'] = "POST request unsuccessful, interval date needs to be in 'YYYY-MM-DD' format!"
                return func.HttpResponse(json.dumps(finalResponse)) 
        else:
            finalResponse['error_code'] = 505
            finalResponse['message'] = "POST request unsuccessful, no interval date provided!"
            return func.HttpResponse(json.dumps(finalResponse)) 
    else:
        finalResponse['error_code'] = 506
        finalResponse['message'] = "No 'MeterNumber' provided in the request body, POST request unsuccesfull!"
        return func.HttpResponse(json.dumps(finalResponse))