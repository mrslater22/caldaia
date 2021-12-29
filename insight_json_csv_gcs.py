'''
Based on a given date range, this script loops through a list of Insight Tag Names
and retrieves the daily Tag data from the Insight API in JSON format, converts the
data to CSV format saves to a CSV file with the tag name and date.
'''
try:
    from google.cloud import storage
    import google.cloud.storage
    import json
    import csv
    import requests
    import datetime
    import os
    import io
except Exception as e:
    print("Some modules are missing {}".format(e))

PATH = os.path.join(os.getcwd(),'enduring-range-306216-1f21d938cbe4.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = PATH

storage_client = storage.Client(PATH)
bucket = storage_client.get_bucket('insight-tag-data-uwgp')

tag_name_list = ["UWGP.AAL235", "UWGP.AC135_CV", "UWGP.AC135_PV", "UWGP.AC135_SP", "UWGP.AIR_BAD_QUALITY",
                 "UWGP.AIR_BAD_QUALITY_FO", "UWGP.AIR_FLOW_AUTO", "UWGP.AIR_FLOW_CV", "UWGP.AIR_FLOW_DEV_ALARM",
                 "UWGP.AIR_FLOW_PV", "UWGP.AIR_FLOW_SP", "UWGP.AT135_O2", "UWGP.B1_ALWCO", "UWGP.B1_LOW_O2",
                 "UWGP.B1_POST_PURGE", "UWGP.B1_PURGE_COMPLETE", "UWGP.B1_PURGE_IN_PROGRESS", "UWGP.B1_RESET_REQUIRED",
                 "UWGP.B1_XY172", "UWGP.B21_IGNITION", "UWGP.B21_LOW_O2", "UWGP.B21_MAIN_TRIAL",
                 "UWGP.B21_OPERATING_LIMITS", "UWGP.B21_PILOT_TRIAL", "UWGP.B21_POST_PURGE", "UWGP.B21_PURGE_COMPLETE",
                 "UWGP.B21_PURGE_IN_PROGRESS", "UWGP.B21_PURGE_TIMER", "UWGP.B21_RESET_REQUIRED", "UWGP.B21_XY172",
                 "UWGP.B21_XY173", "UWGP.BM_OPMS", "UWGP.BM_PV_SP_DEV", "UWGP.BOILER_FLUE_GAS_OUTLET_TEMP_BQ",
                 "UWGP.BOILERMASTER_AUTO", "UWGP.BOILERMASTER_CV", "UWGP.BOILERMASTER_SP", "UWGP.BYPASS_BUTTON",
                 "UWGP.CCS1_ComCounterAlarm", "UWGP.CCS1_WATCHDOG_PULSE", "UWGP.COMPENSATED_GAS_FLOW_HMI",
                 "UWGP.COOKWATER_HX_GAS_INLET_TEMP", "UWGP.COOKWATER_HX_GAS_OUTLET_TEMP", "UWGP.DCS_CMD2",
                 "UWGP.DL_OPMS", "UWGP.DL_OPMS_TRACK", "UWGP.DRIVE_TO_PURGE", "UWGP.DRUM_LEVEL_AMTOGGLE",
                 "UWGP.DRUM_LEVEL_AUTO", "UWGP.DRUM_LEVEL_BAD_QUALITY", "UWGP.DRUM_LEVEL_CV", "UWGP.DRUM_LEVEL_PV",
                 "UWGP.DRUM_PRESSURE_BAD_QUALITY", "UWGP.DRUM_PRESSURE_PV", "UWGP.ECON_FLU_GAS_INLET_TEMP",
                 "UWGP.ECON_FLU_GAS_OUTLET_TEMP", "UWGP.ECON_WATER_INLET_TEMP", "UWGP.ECON_WATER_OUTLET_TEMP",
                 "UWGP.ECONOMIZER_BFW_OUTLET_TEMP_BQ", "UWGP.ECONOMIZER_BFW_TEMP_BAD_QUALITY",
                 "UWGP.EXCESS_O2_TRANSMITTER", "UWGP.FC110_AUTO", "UWGP.FC110_CV", "UWGP.FC110_PV", "UWGP.FC110_SP",
                 "UWGP.FC120_AUTO", "UWGP.FC120_CV", "UWGP.FC120_HMI", "UWGP.FC120_SP", "UWGP.FC170_AUTO",
                 "UWGP.FC170_CV", "UWGP.FC170_PV", "UWGP.FC170_SP", "UWGP.FC190", "UWGP.FC190_AUTO", "UWGP.FC190_CV",
                 "UWGP.FC190_PV", "UWGP.FC190_SP", "UWGP.FDFAN_ON", "UWGP.FEEDWATER_CONTROL_VALVE",
                 "UWGP.FEEDWATER_FLOW", "UWGP.FP_SP_PV_DEV", "UWGP.FT120", "UWGP.FT120A", "UWGP.FT140", "UWGP.FT160",
                 "UWGP.FUEL_GAS_FLOW_BAD_QUALITY", "UWGP.FUEL_GAS_PIDE_AUTO", "UWGP.FUEL_GAS_PIDE_CV",
                 "UWGP.FUEL_GAS_PIDE_SP", "UWGP.FUEL_GAS_PRESSURE_BAD_QUALITY", "UWGP.FUEL_GAS_TEMPERATURE_BAD_QUALITY",
                 "UWGP.FURNACE_PRESSURE", "UWGP.FURNACE_PRESSURE_AUTO", "UWGP.FURNACE_PRESSURE_BAD_QUALITY",
                 "UWGP.FURNACE_PRESSURE_CV", "UWGP.FURNACE_PRESSURE_PIDE_PV", "UWGP.FURNACE_PRESSURE_PIDE_SP",
                 "UWGP.FURNACE_TEMP", "UWGP.FURNACE_TEMPERATURE_BAD_QUALITY", "UWGP.FV110", "UWGP.FV120",
                 "UWGP.FV137_FGR_POS", "UWGP.FV140", "UWGP.FV170", "UWGP.GAS_FLOW_%", "UWGP.GAS_FLOW_EU",
                 "UWGP.GAS_FLOW_VALVE", "UWGP.GAS_SP_PV_DEV", "UWGP.GAS_SUPPLY_PRESSURE",
                 "UWGP.GAS_TEMPERATURE_HIGH_LOW", "UWGP.General_Alarm_Ack", "UWGP.HGP", "UWGP.HIGH_DRUM_PRESSURE_ALARM",
                 "UWGP.HIGH_LOW_GAS_PSI", "UWGP.HIGH_WATER_ALARM", "UWGP.HSP", "UWGP.ID_FAN_DAMPER",
                 "UWGP.ID_FAN_IB_BEARING_TEMP", "UWGP.ID_FAN_IB_BEARING_TEMP_TRIP", "UWGP.ID_FAN_INLET_PRESSURE_BQ",
                 "UWGP.ID_FAN_INLET_PSI_HIGH_LOW", "UWGP.ID_FAN_INLET_TEMPERATURE_BQ",
                 "UWGP.ID_FAN_OB_BEARING_TEMP_TRIP", "UWGP.IDFAN_ON", "UWGP.LC150_AMTOGGLE", "UWGP.LC150_AUTO",
                 "UWGP.LC150_CV", "UWGP.LC150_CV_ADJUST", "UWGP.LC150_PV", "UWGP.LCAP", "UWGP.LGP", "UWGP.LIAP",
                 "UWGP.LOW_FIRE_RELEASE", "UWGP.LOW_O2", "UWGP.LOW_WATER_ALARM", "UWGP.LWCO", "UWGP.MAIN_FLAME",
                 "UWGP.O2_CV", "UWGP.O2_PV", "UWGP.O2_SP", "UWGP.OP_OPMS", "UWGP.OPERATING_LIMITS",
                 "UWGP.OVER_PRESSURE_CV_OPER", "UWGP.OVERPRESSURE_CV", "UWGP.OVERPRESSURE_PIDE_AUTO",
                 "UWGP.OVERPRESSURE_VALVE_AUTO", "UWGP.PC160_AUTO", "UWGP.PC160_CV", "UWGP.PC160_PV", "UWGP.PC160_SP",
                 "UWGP.PC190_AUTO", "UWGP.PC191_CV", "UWGP.PILOT", "UWGP.PSH120", "UWGP.PT170",
                 "UWGP.PT220_BRN_GAS_PSI", "UWGP.PT240_FW_PSI", "UWGP.PT340_FW_PSI", "UWGP.SCANNER_1", "UWGP.SCANNER_2",
                 "UWGP.ST105_VFD_SPEED", "UWGP.STEAM_FLOW", "UWGP.STEAM_FLOW_BAD_QUALITY", "UWGP.STEAMFLOW_OUTLET_TEMP",
                 "UWGP.STEAMHEADER_OVERPRESSURE_CV", "UWGP.STEAMHEADER_OVERPRESSURE_PV",
                 "UWGP.STEAMHEADER_OVERPRESSURE_SP", "UWGP.SUPPLY_GAS_TEMPERATURE", "UWGP.TE135A", "UWGP.TE135B",
                 "UWGP.TE140A", "UWGP.TE140B", "UWGP.TE160A_STEAM_TEMP", "UWGP.TE170", "UWGP.TE180A", "UWGP.TE180B",
                 "UWGP.TE260_STEAM_TEMP", "UWGP.TE335B_ECON_OUT_TEMP", "UWGP.TE340A_ECON_IN_TEMP",
                 "UWGP.TE340B_ECON_OUT_TEMP", "UWGP.TE360_STEAM_TEMP", "UWGP.VIV_VARIABLE_INLET_VANE"]
tag_name_list1 = ["UWGP.AAL235"]

#Loop date rage
loopdate = datetime.date(2021, 3, 1)
loopedate = datetime.date(2021, 3, 10)

#Retrieve the list of Tag Names to process
with open("uwgp-fqn.csv") as tag_names:
    tag_data = csv.reader(tag_names, delimiter = ",")
    for row in tag_data:
        tag_data_list = row

    #Begin outer date range loop
    while loopdate <= loopedate:
        bdate = loopdate
        edate = bdate + datetime.timedelta(days=1)
        fdate = bdate.strftime("%Y%m%d")

        #Begin Tag Name loop
        for i in tag_data_list:
            tag_split = i.split(".")
            blob_path = "insight_" + str.lower(tag_split[1]) + "_"+fdate+".csv"

            #Check if file exists
            file_check = storage.Blob(bucket=bucket,name=blob_path).exists(storage_client)

            print(i, str(bdate), str(edate), blob_path, file_check)

            if file_check == False:

                blob = bucket.blob(blob_path)

                #Retrieve Insight JSON data
                url = "https://online.wonderware.com/s/344f0d96-67b7-4a4f-8edc-e584b73f28b5/apis/Historian/v2/ProcessValues?$filter=FQN+eq+'"+i+"'+and+DateTime+ge+"+str(bdate)+"T00:00:00Z+and+DateTime+lt+"+str(edate)+"T00:00:00Z&Resolution=3600000"
                request = requests.get(url, auth=('bobwilson@caldaiacontrols.com', 'Triage2020!'))
                request_text = request.text

                data = json.loads(request_text)

                rowcount = len(data["value"])

                print(i, str(bdate), str(edate), blob_path, rowcount,"records loaded")

                #Loop through results, convert JSON to CSV and save CSV file
                if rowcount > 0:
                    output = io.StringIO()
                    fieldnames = data["value"][0].keys()
                    writer = csv.DictWriter(output, fieldnames=fieldnames)
                    writer.writeheader()
                    for item in data["value"]:
                        writer.writerow(item)

                #print(output.getvalue())
                blob.upload_from_string(data=output.getvalue(),content_type='text/csv')

        # Increment Loop Date
        loopdate = loopdate + datetime.timedelta(days=1)