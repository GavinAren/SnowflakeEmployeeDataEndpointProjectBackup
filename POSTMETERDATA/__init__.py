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
    inpMeterEventDateTime = req.get_json().get('MeterEventDateTime')
    inpMeterNumber = req.get_json().get('MeterNumber')
    inpDeviceNumber = req.get_json().get('DeviceNumber')
    inpMeterType = req.get_json().get('MeterType')
    inpIntervalDate = req.get_json().get('IntervalDate')
    inpIntervalRegister = req.get_json().get('IntervalRegister')
    finalResponse = {} #global string to hold the final formulated response
        
    if inpMeterNumber: 
        
        if inpIntervalDate:
            
            if validate_date(inpIntervalDate):
        
                try:
                    mycursor.execute("USE WAREHOUSE project_warehouse")
                    mycursor.execute("USE DATABASE project_database")
                    mycursor.execute("USE SCHEMA project_schema")
                    mycursor.execute (f"SELECT * FROM MeterData WHERE MeterNumber =  '{inpMeterNumber}' AND IntervalDate = '{inpIntervalDate}' ")
                    
                    #finalResponse['message'] = (f"{mycursor.fetchall()}")
                    #return func.HttpResponse(json.dumps(finalResponse)) 
                    
                    output = {}
                    for (metereventdatetime, meternumber, devicenumber,metertype,intervaldate, INT_00_VAL,INT_01_VAL,INT_02_VAL,INT_03_VAL,INT_04_VAL,INT_05_VAL,INT_06_VAL,INT_07_VAL,INT_08_VAL,INT_09_VAL,INT_10_VAL,INT_11_VAL,INT_12_VAL,INT_13_VAL,INT_14_VAL,INT_15_VAL,INT_16_VAL,INT_17_VAL,INT_18_VAL,INT_19_VAL,INT_20_VAL,INT_21_VAL,INT_22_VAL,INT_23_VAL,INT_24_VAL ) in mycursor:
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
                    
                    if len(output)>1:
                        finalResponse['error_code'] = 511
                        finalResponse['message'] = (f"POST request unsuccessful, an entry already exists for MeterNumber: {inpMeterNumber} at date : {inpIntervalDate}, please choose a new date or meter! ")
                        return func.HttpResponse(json.dumps(finalResponse))         
                    
                    else:
                        if not inpMeterEventDateTime: 
                            finalResponse['error_code'] = 503
                            finalResponse['message'] = "No 'meterEventDateTime' provided in the request body, POST request unsuccesful!"
                            return func.HttpResponse(json.dumps(finalResponse))
                    
                        elif not validate_dateandtime(inpMeterEventDateTime): 
                            finalResponse['error_code'] = 504
                            finalResponse['message'] = " 'meterEventDateTime' is not in DD/MM/YYYY format, POST request unsuccesful!"
                            return func.HttpResponse(json.dumps(finalResponse))
                        
                        elif not inpMeterType:
                            finalResponse['error_code'] = 505
                            finalResponse['message'] = "No 'meterType' provided in the request body, POST request unsuccesful!"
                            return func.HttpResponse(json.dumps(finalResponse))  
                        
                        elif not inpDeviceNumber:
                            finalResponse['error_code'] = 505
                            finalResponse['message'] = " No 'deviceType' provided in the request body, POST request unsuccesful!"
                            return func.HttpResponse(json.dumps(finalResponse))    
                                 
                        elif not inpIntervalRegister:
                            finalResponse['error_code'] = 505
                            finalResponse['message'] = " No 'intervalRegister' list provided in the request body, POST request unsuccesfull!"
                            return func.HttpResponse(json.dumps(finalResponse))                                         
                        else:
                            intervalRegisterDict = inpIntervalRegister
                            print(intervalRegisterDict)
                            if len(intervalRegisterDict) < 25:
                                finalResponse['error_code'] = 506
                                finalResponse['message'] = " 'intervalRegister' list provided in the request body does not hold values for all comlunm INT_00_VAL to INT_24_VAL, POST request unsuccesfull!"
                                return func.HttpResponse(json.dumps(finalResponse))                                 
                            
                            else:
                                mycursor.execute ("INSERT INTO project_schema.MeterData( MeterEventDateTime, MeterNumber, DeviceNumber, MeterType, IntervalDate, " + 
                                                "INT_00_VAL,INT_01_VAL,INT_02_VAL,INT_03_VAL,INT_04_VAL,INT_05_VAL,INT_06_VAL,INT_07_VAL,INT_08_VAL,INT_09_VAL,INT_10_VAL,INT_11_VAL,INT_12_VAL,INT_13_VAL,INT_14_VAL,INT_15_VAL,INT_16_VAL,INT_17_VAL,INT_18_VAL,INT_19_VAL,INT_20_VAL,INT_21_VAL,INT_22_VAL,INT_23_VAL,INT_24_VAL) VALUES "+
                                                f"('{inpMeterEventDateTime}', '{inpMeterNumber}', '{inpDeviceNumber}', '{inpMeterType}', '{inpIntervalDate}', '{intervalRegisterDict['INT_00_VAL']}','{intervalRegisterDict['INT_01_VAL']}','{intervalRegisterDict['INT_02_VAL']}','{intervalRegisterDict['INT_03_VAL']}','{intervalRegisterDict['INT_04_VAL']}','{intervalRegisterDict['INT_05_VAL']}','{intervalRegisterDict['INT_06_VAL']}','{intervalRegisterDict['INT_07_VAL']}','{intervalRegisterDict['INT_08_VAL']}','{intervalRegisterDict['INT_09_VAL']}','{intervalRegisterDict['INT_10_VAL']}','{intervalRegisterDict['INT_11_VAL']}','{intervalRegisterDict['INT_12_VAL']}','{intervalRegisterDict['INT_13_VAL']}','{intervalRegisterDict['INT_14_VAL']}','{intervalRegisterDict['INT_15_VAL']}','{intervalRegisterDict['INT_16_VAL']}','{intervalRegisterDict['INT_17_VAL']}','{intervalRegisterDict['INT_18_VAL']}','{intervalRegisterDict['INT_19_VAL']}','{intervalRegisterDict['INT_20_VAL']}','{intervalRegisterDict['INT_21_VAL']}','{intervalRegisterDict['INT_22_VAL']}','{intervalRegisterDict['INT_23_VAL']}','{intervalRegisterDict['INT_24_VAL']}') ")
                                ##need to enumerate teh 24 values our of the last variable
                                ##could do IntervalRegister.get[] for each position
                                finalResponse['status_code'] = 202
                                finalResponse['message'] = f"POST request successful."
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