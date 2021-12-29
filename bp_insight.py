from google.cloud import storage
import google.cloud.storage
import psycopg2
import json
import csv
import requests
import datetime
import os.path

con = psycopg2.connect(
    host="127.0.0.1",
    dbname="triage_ibec",
    user="postgres",
    password="/+mwAG:4Ln`zfWEb",
    port="3306"
)

cur = con.cursor()
cur2 = con.cursor()
\''

batch_seconds = 86400
batch_minutes = 1440
batch_hours = 24
batch_edate = datetime.datetime(2021, 8, 13, 0, 0, 0)
batch_bdate = batch_edate + datetime.timedelta(days=-1)

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

# Create column headers for per second import array
ps_fields = ["date_time","dtk","dtk_ch","dtk_cm","dtk_ph","dtk_pm"]
for i in tag_name_list:
    tag_split = i.split(".")
    ps_fields.append(tag_split[1].lower())

# Create column headers for per minute average import array
pma_fields = ["date_day","dtk_cm","dtk_pm"]
for i in tag_name_list:
    tag_split = i.split(".")
    pma_fields.append(tag_split[1].lower())

# Create column headers for per hour average import array
pha_fields = ["date_day","dtk_ch","dtk_ph"]
for i in tag_name_list:
    tag_split = i.split(".")
    pha_fields.append(tag_split[1].lower())

# Create Insight data per second batch import array
batch_field_count = len(ps_fields)
w, h = batch_field_count, batch_seconds;
ps_batch_arr = [[0 for x in range(w)] for y in range(h)]

# Create Insight data per minute average batch import array
batch_field_count = len(pma_fields)
w, h = batch_field_count, batch_minutes;
pma_batch_arr = [[0 for x in range(w)] for y in range(h)]

# Create Insight data per hour average batch import array
batch_field_count = len(pha_fields)
w, h = batch_field_count, batch_hours;
pha_batch_arr = [[0 for x in range(w)] for y in range(h)]

# Populate Insight Batch Processing date_time values
loop_count = 0
loop_date = batch_bdate
while loop_count < batch_seconds:
    ps_batch_arr[loop_count][0] = str(loop_date)
    ps_batch_arr[loop_count][1] = datetime.datetime.strftime(loop_date, "%Y%m%d%H%M%S")
    ps_batch_arr[loop_count][2] = datetime.datetime.strftime(loop_date, "%Y%m%d%H") + "0000"
    ps_batch_arr[loop_count][3] = datetime.datetime.strftime(loop_date, "%Y%m%d%H%M") + "00"
    ps_batch_arr[loop_count][4] = datetime.datetime.strftime(loop_date + datetime.timedelta(hours=-1), "%Y%m%d%H") + "0000"
    ps_batch_arr[loop_count][5] = datetime.datetime.strftime(loop_date + datetime.timedelta(minutes=-1), "%Y%m%d%H%M") + "00"

    loop_count += 1
    loop_date = loop_date + datetime.timedelta(seconds=1)

# Create API querystring begin and end dates
batch_bdate_split = str(batch_bdate).split(" ")
batch_edate_split = str(batch_edate).split(" ")
bdate = batch_bdate_split[0] + "T" + batch_bdate_split[1] + "Z"
edate = batch_edate_split[0] + "T" + batch_edate_split[1] + "Z"
print(bdate, edate)

# Retrieve Insight JSON Tag Data and update batch import array with tag values
tn_ps_loop_count = 6
tn_pma_loop_count = 3
tn_pha_loop_count = 3
for i in tag_name_list:
    url = "https://online.wonderware.com/s/344f0d96-67b7-4a4f-8edc-e584b73f28b5/apis/Historian/v2/ProcessValues?$filter=FQN+eq+'" + i + "'+and+DateTime+ge+" + bdate + "+and+DateTime+lt+" + edate + "&Resolution=3600000"
    request = requests.get(url, auth=('bobwilson@caldaiacontrols.com', 'Triage2020!'))
    request_text = request.text
    data = json.loads(request_text)
    rowcount = len(data["value"])

    # Get Tag Name
    tag_name_split = i.split(".")
    tag_name = tag_name_split[1].lower()

    if rowcount > 0:
        w, h = 2, rowcount;
        result_arr = [[0 for x in range(w)] for y in range(h)]

        loop_count = 0
        for item in data["value"]:
            # Get Date Time
            row_date_split = item["DateTime"].split("T")
            row_date = row_date_split[0] + " " + row_date_split[1][0:8]

            # Get Tag Value
            if item["Value"] == None:
                tag_value = 99999
            else:
                tag_value = item["Value"]

            # Append Insight Result Tag Data Into Result Array
            result_arr[loop_count][0] = row_date
            result_arr[loop_count][1] = tag_value

            loop_count += 1

    # Update Insight Batch Processing tag values
    hour_total = 0
    min_total = 0
    hour_count = 1
    min_count = 1
    loop_count = 0
    loop_count2 = 0
    loop_count_pma = 0
    loop_count_pha = 0
    loop_value = 99999
    loop_date = batch_bdate

    # Populate batch array with tag values
    while loop_count < batch_seconds:
        if loop_count2 < rowcount:
            if str(loop_date) == str(result_arr[loop_count2][0]):
                loop_value = result_arr[loop_count2][1]
                loop_count2 += 1
        # Populate per second value
        ps_batch_arr[loop_count][tn_ps_loop_count] = loop_value
        # Populate per minute average value
        min_total = min_total + loop_value
        if min_count == 60:
            pma_batch_arr[loop_count_pma][0] = datetime.datetime.strftime(loop_date, "%Y-%m-%d")
            pma_batch_arr[loop_count_pma][1] = datetime.datetime.strftime(loop_date, "%Y%m%d%H%M") + "00"
            pma_batch_arr[loop_count_pma][2] = datetime.datetime.strftime(loop_date + datetime.timedelta(minutes=-1), "%Y%m%d%H%M") + "00"
            pma_batch_arr[loop_count_pma][tn_pma_loop_count] = min_total / min_count
            min_count = 0
            min_total = 0
            loop_count_pma += 1
        # Populate per hour average value
        hour_total = hour_total + loop_value
        if hour_count == 3600:
            pha_batch_arr[loop_count_pha][0] = datetime.datetime.strftime(loop_date, "%Y-%m-%d")
            pha_batch_arr[loop_count_pha][1] = datetime.datetime.strftime(loop_date, "%Y%m%d%H") + "0000"
            pha_batch_arr[loop_count_pha][2] = datetime.datetime.strftime(loop_date + datetime.timedelta(hours=-1), "%Y%m%d%H") + "0000"
            pha_batch_arr[loop_count_pha][tn_pha_loop_count] = hour_total / hour_count
            hour_count = 0
            hour_total = 0
            loop_count_pha += 1

        loop_count += 1
        min_count += 1
        hour_count += 1
        loop_date = loop_date + datetime.timedelta(seconds=1)

    tn_pma_loop_count += 1
    tn_pha_loop_count += 1
    tn_ps_loop_count += 1

print(f"Batch data array for {bdate} to {edate} generated successfully.")

bdate_str = str(datetime.datetime.strftime(batch_bdate, "%Y%m%d%H%M%S"))
edate_str = str(datetime.datetime.strftime(batch_edate, "%Y%m%d%H%M%S"))

ps_file_path = f"insight_ibec_{bdate_str}-{edate_str}.csv"
with open(ps_file_path,"w",newline="\n") as f:
    csvWriter = csv.writer(f,delimiter=',')
    csvWriter.writerows([ps_fields])
    csvWriter.writerows(ps_batch_arr)

pma_file_path = f"insight_ibec_pma_{bdate_str}-{edate_str}.csv"
with open(pma_file_path,"w",newline="\n") as f:
    csvWriter = csv.writer(f,delimiter=',')
    csvWriter.writerows([pma_fields])
    csvWriter.writerows(pma_batch_arr)

pha_file_path = f"insight_ibec_pha_{bdate_str}-{edate_str}.csv"
with open(pha_file_path,"w",newline="\n") as f:
    csvWriter = csv.writer(f,delimiter=',')
    csvWriter.writerows([pha_fields])
    csvWriter.writerows(pha_batch_arr)

cur.close()
con.close()
print(f"Batch data CSV for {bdate} to {edate} generated successfully.")
