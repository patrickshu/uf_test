# -*- coding: utf-8 -*-
"""
Created by Patrick Shu
"""

#PYTHON 2.7

import math
import urllib
import ssl
import smtplib
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from collections import OrderedDict
from subprocess import Popen, PIPE

DEBUG = 1
TEST = 0 #change this to 1 to run test for an earlier date

days_to_subtract = 2
further_subtract = 14
todaysdate = datetime.today() - timedelta(days=days_to_subtract) # today - 2
two_weeks_back = datetime.today() - timedelta(days=days_to_subtract+further_subtract) # today - 2 - 14
todaysdate_formated = todaysdate.strftime("%d-%b-%Y")
two_weeks_back_formated = two_weeks_back.strftime("%d-%b-%Y")
todaysdate_formated_2 = todaysdate.strftime("%d/%m/%Y")


#================================TEST======================================
if TEST:
    test_date = '06-jun-2017'#change to the date to test
    todaysdate_formated = test_date
    todaysdate_formated_2 = test_date + " TEST_AN_EARLIER_DAY"
    two_weeks_back_formated = '22-may-2017' #change to the date(-14) to test
#================================TEST======================================

date_prefix = "&date=" + todaysdate_formated
url_prefix_starter = "https://www.model.dataprocessors.com.au/monitor/php/starter_level_factor_day.php?"
url_prefix_race = "https://www.model.dataprocessors.com.au/monitor/php/race_level_factor_day.php?"
all_good_html = """\
                <!DOCTYPE html>
                <html>
                <head>
                  <title>Page Title</title>
                </head>
                <body>
                
                <p>No mismatch found.</p>
                <p>The Update Framework is all good on """+ todaysdate_formated + """.</p>               
                </body>
                </html>
                """

raw_dict = {"brjp0042": [["BR_WIND_SPEED", None], ["BR_WAVE_AMP", None], ["BR_RUNNER_WEIGHT", None], ["BR_WEIGHT_ADDED", None],
                                   ["BR_PRACTICE_POS", None], ["BR_TILT", None], ["BR_EQUIP", None], ["BR_WIND_DIRECTION", None],
                                   ["BR_DISPLAY_LAP", None], ["BR_PRACTICE_TIME", None], ["BR_WEATHER", None], ["BR_DISTANCE", None]],
                      "hnde": [["JOCKEY", None], ["TRACK_COND", None]],
                      "hnfi": [["SHOE", None]],
                      "hnfr": [["JOCKEY", None]],
                      "gpfr": [["JOCKEY", None]],
                      "gphk": [["JOCKEY", None], ["TRACK_COND", None]],        
                      "gphk0185": [["JOCKEY", None], ["TRACK_COND", None]], 
                      "gpkr": [["TRACK_COND", None], ["JOCKEY", None],],  #["WEIGHT_CARRIED", None]
                      "gpkr0121": [["TRACK_COND", None], ["JOCKEY", None],],  #["WEIGHT_CARRIED", None]
                      "gpuk": [["JOCKEY", None], ["TRACK_COND", None], ["SST", None]],           
                      "gpjpc": [["JOCKEY", None], ["TRACK_COND", None], ["RUNNER_WEIGHT", None], ["SST", None]], 
                      #"gpza": [["JOCKEY", None], ["TRACK_COND", None], ["TRACK_SURFACE", None], ["JOCKEY_ALLOWANCE", None]], 
                      "gpza0142": [["JOCKEY", None], ["TRACK_COND",  None], ["TRACK_SURFACE", None], ["JOCKEY_ALLOWANCE", None]], 
                      "pykr": [["JOCKEY", None], ["HUMIDITY", None]], #["WEIGHT_CARRIED", None], 
                      "pykr0031": [["JOCKEY", None], ["HUMIDITY", None]], #["WEIGHT_CARRIED", None], 
                      "gpsg": [["JOCKEY", None], ["TRACK_COND", None]],
                      "hnit0017": [["JOCKEY", None], ["TRACK_COND", None]]
                      }

marketAndVariables = OrderedDict(sorted(raw_dict.items(), key=lambda t: t[0]))

raceLevelVariables = ["TRACK_COND", "TRACK_SURFACE", "SST", "HUMIDITY", "BR_WIND_SPEED", "BR_WAVE_AMP", "BR_WIND_DIRECTION", "BR_WEATHER", "BR_DISTANCE"] 

variable_look_up = {"BR_WIND_SPEED": "r20794",	
                    "BR_WAVE_AMP": "r20796",
                    "BR_RUNNER_WEIGHT": "r20930",
                    "BR_WEIGHT_ADDED": "r20931",
                    "BR_PRACTICE_POS": "r20891",
                    "BR_TILT": "r20881",
                    "BR_EQUIP": "equipment",
                    "BR_WIND_DIRECTION": "r20795",
                    "BR_DISPLAY_LAP": "r20799",
                    "BR_PRACTICE_TIME": "r20892",
                    "BR_WEATHER": "r281",
                    "BR_DISTANCE": "r31",
                    "JOCKEY": "r6",
                    "TRACK_COND": "r17",
                    "SHOE": "r973, r974",
                    "WEIGHT_CARRIED": "r30",
                    "SST": "r340",
                    "RUNNER_WEIGHT": "r265",
                    "TRACK_SURFACE": "r18",
                    "JOCKEY_ALLOWANCE": "r30",
                    "HUMIDITY": "r17v2"           
                    }

dpdb_log_in = {"brjp0042": 'BRJP_CLIENT/brjp2015@dpdb',
               "hnde": 'HNDE_CLIENT/hnde2014@dpdb',
               "hnfi": 'HNFI_CLIENT/hnfi2014@dpdb',
               "hnfr": 'FRH_CLIENT/loader@dpdb',
               "gpfr": 'FRG_CLIENT/loader@dpdb',
               "gphk": 'GPHK_CLIENT/gphk2014@dpdb',        
               "gphk0185": 'GPHK_CLIENT/gphk2014@dpdb', 
               "gpkr": 'GPKR_CLIENT/gpkr2014@dpdb',
               "gpkr0121": 'GPKR_CLIENT/gpkr2014@dpdb',
               "gpuk": 'UKG_CLIENT2/nerkez@dpdb',           
               "gpjpc": 'GPJP_CLIENT2/gpjp2015@dpdb', 
               "gpza": 'ZAG_CLIENT/gpza2013@dpdb', 
               "gpza0142": 'ZAG_CLIENT/gpza2013@dpdb',
               "pykr": 'PYKR_CLIENT/pykr20150401@dpdb', 
               "pykr0031": 'PYKR_CLIENT/pykr20150401@dpdb',
               "gpsg": 'GPSGMY_CLIENT/gpsgmy2014@dpdb',
               "hnit0017": 'HNIT_CLIENT/hnit2017@dpdb', 
               }

dpdb_base_t = {"brjp0042": 'rfile_base',
               "hnde": 'rfile_combined',
               "hnfi": 'rfile_base_v2',
               "hnfr": 'rfile_pt',
               "gpfr": 'rfile_base',
               "gphk": 'rfile_base_v163',        
               "gphk0185": 'rfile_base_2016', 
               "gpkr": 'rfile_base',
               "gpkr0121": 'rfile_base_201611',
               "gpuk": 'ukg_client2.rfile_form@DPDB',           
               "gpjpc": 'rfile_combined', 
               "gpza": 'rfile_table', 
               "gpza0142": 'rfile_table',
               "pykr": 'rfile_base', 
               "pykr0031": 'rfile_base',
               "gpsg": 'rfile_base',
               "hnit0017": 'ec_rfile_base_0005'
               }

def generate_urls(marketAndVariables_dict):
    for key in marketAndVariables_dict:
    	#print key
        market_prefix = "market=" + key
        for value in marketAndVariables_dict[key]:
            variable_prefix = "&changeType=" + value[0]
            #print(value[1])
            if value[0] in raceLevelVariables:
                value[1] = url_prefix_race + market_prefix + date_prefix + variable_prefix + "&debug=1"
            else:
                value[1] = url_prefix_starter + market_prefix + date_prefix + variable_prefix + "&debug=1"       
    return marketAndVariables_dict



        
def check_diff(marketAndVariables_dict):
    result = []
    for market in generate_urls(marketAndVariables_dict):
        for variable in marketAndVariables_dict[market]:
	    print "="*15, market, variable[0], "="*(100-(len(market)+len(variable[0])))
            mismatch = int(find_mismatch(variable[1], parse_html(variable[1])))
            if mismatch > 0:
                result.append((market, variable[0], variable_look_up[variable[0]], str(mismatch), normal_url(variable[1])))
    #print(result)    
    return result
    

def normal_url(url):
    if 'race_level' in url:
        new_url = url.replace("race_level_factor_day", "router")
    else:
        new_url = url.replace("starter_level_factor_day", "router")
    new_url = new_url.replace("&debug=1", "")
    
    return new_url
    

def parse_html(url):
    #ctx = ssl.create_default_context()
    #ctx.check_hostname = False
    #ctx.verify_mode = ssl.CERT_NONE
    
    response = urllib.urlopen(url)
    html = response.read()

    
    soup = BeautifulSoup(html, "html.parser")
    if 'race_level' in url:
        table = soup.find("table", attrs={"class":"table table-striped table-bordered table-hover table-condensed "})
    else:
        table = soup.find("table", attrs={"class":"table table-striped table-bordered table-hover table-condensed"})
    
    headings = [th.get_text() for th in table.find("tr").find_all("th")]
    
    datasets = []
    for row in table.find_all("tr")[1:]:
        dataset = zip(headings, (td.get_text() for td in row.find_all("td")))
        datasets.append(dataset)
    
    return datasets

def parse_html_latest_date(url):
    #ctx = ssl.create_default_context()
    #ctx.check_hostname = False
    #ctx.verify_mode = ssl.CERT_NONE
    
    response = urllib.urlopen(url)
    html = response.read()
    
    soup = BeautifulSoup(html, "html.parser")
    if 'race_level' in url:
        table = soup.find("table", attrs={"class":"table table-striped table-bordered table-hover table-condensed "})    
        indicator = 0
        for i in table.find("tbody"): #find the first div
            if indicator == 1:
                result = str(i)
                indicator = 0
            if "rfile table source:" in str(i):
                indicator = 1
    else:
        table = soup.find("table", attrs={"class":"table table-striped table-bordered table-hover table-condensed"})
        #print table.find("div", [" float-left"])
        for i in table.find("div", attrs={'class':' float-left'}):
            if "<b>" not in str(i) and len(str(i)) > 2:
                result = str(i)

    return result

def find_mismatch(url, datasets):
    numOfMismatch = 0    
    for data in datasets:
        r5_indicator = 0
        for d in data:
            if ('starter_level' in url and 'S2 LIVE' in d[0]) or ('race_level' in url and 'S2_R' in d[0]):
                s2 = d[1].split('(')[0].strip()
            if 'S3 LIVE' in d[0]:
                s3 = d[1].split('(')[0].strip()
            if 'VALUE IN RESULTS' == d[0]:
                result = d[1].split('(')[0].strip()
            if 'R3' == d[0]:
                r3 = d[1].split('(')[0].strip()    
            if 'R4' == d[0]:
                r4 = d[1].split('(')[0].strip()    
            if 'R5' == d[0]:
                r5 = d[1].split('(')[0].strip()
                r5_indicator = 1
        try: (float(s2) == float(s3) == float(result))       
        except ValueError:
            if not (s2 == s3 == result):
                numOfMismatch += 1
                if r5_indicator:
                    print " R3: ", r3, " R4: ", r4, " R5: ", r5, " S2: ", s2.encode('ascii', 'ignore').decode(), \
                    " S3: ", s3.encode('ascii', 'ignore').decode(), " RESULT: ", result.encode('ascii', 'ignore').decode()
                else:
                    print " R3: ", r3, " R4: ", r4, " S2: ", s2.encode('ascii', 'ignore').decode(), \
                    " S3: ", s3.encode('ascii', 'ignore').decode(), " RESULT: ", result.encode('ascii', 'ignore').decode()
		
            continue        
        if not (float(s2) == float(s3) == float(result)):
            numOfMismatch += 1
            if r5_indicator:
                print " R3: ", r3, " R4: ", r4, " R5: ", r5, " S2: ", s2.encode('ascii', 'ignore').decode(), \
                " S3: ", s3.encode('ascii', 'ignore').decode(), " RESULT: ", result.encode('ascii', 'ignore').decode()
            else:
                print " R3: ", r3, " R4: ", r4, " S2: ", s2.encode('ascii', 'ignore').decode(), \
                " S3: ", s3.encode('ascii', 'ignore').decode(), " RESULT: ", result.encode('ascii', 'ignore').decode()
	    
    return str(numOfMismatch)

def check_latest_report(market, variable):
    global current_markets_table
    market_prefix = "market=" + market
    variable_prefix = "&changeType=" + variable
    i = 0
    while i < 15:
        days_to_subtract = i
        real_todaysdate = datetime.today() - timedelta(days=days_to_subtract)
        real_todaysdate_formated = real_todaysdate.strftime("%d-%b-%Y")
        today_prefix = "&date=" + real_todaysdate_formated
        if variable in raceLevelVariables:
            url = url_prefix_race + market_prefix + today_prefix + variable_prefix + "&debug=1"
        else:
            url = url_prefix_starter + market_prefix + today_prefix + variable_prefix + "&debug=1"    
        extracted_string = parse_html_latest_date(url)
        
        if extracted_string != "UNKNOWN":
            latest_date = real_todaysdate_formated
            current_markets_table = extracted_string
            print current_markets_table
            return latest_date
        i += 1
    
    return "No reports in last 14 days" 

#function that takes the sqlCommand and connectString and returns the queryReslut and errorMessage (if any)
def runSqlQuery(sqlCommand, connectString):
   session = Popen(['sqlplus', '-S', connectString], stdin=PIPE, stdout=PIPE, stderr=PIPE)
   session.stdin.write(sqlCommand.encode())
   return session.communicate()

def compareQueries(market):
    if market in ("hnfr", "gpfr", "gpuk"):
        meeting_date = "race_date"
    else:
        meeting_date = "meeting_date"
        
    sql_1 = "select distinct r2,r3 from " + current_markets_table + \
            " where " + meeting_date + " between '" + two_weeks_back_formated+ "' and '"+ todaysdate_formated + "' order by r2 desc;"
            
    sql_2 = "select distinct r2,r3 from " + dpdb_base_t[market] + \
            " where " + meeting_date + " between '" + two_weeks_back_formated+ "' and '"+ todaysdate_formated + "' order by r2 desc;"
    result_tuple1 = runSqlQuery(sql_1, dpdb_log_in[market]) 
    result_tuple2 = runSqlQuery(sql_2, dpdb_log_in[market])
    if result_tuple1 == result_tuple2:
        return "None"
    if ("ERROR" in result_tuple1[0]) and ("ERROR" in result_tuple1[0]):
        return "No events found in both tables"
    if ("ERROR" in result_tuple1[0]):
        return "No events found in RTMH 14-day table"
    if ("ERROR" in result_tuple2[0]):
        return "No events found in the base rfile table"
    return find_missing_meets(result_tuple1, result_tuple2)
    
def find_missing_meets(result_tuple1, result_tuple2):
    missing_meets_raw = create_set(result_tuple2).difference(create_set(result_tuple1))
    result_string = "missing meets: " + "\n"
    for i in missing_meets_raw:
        year, month, day = jd_to_date(float(i[0]))
        result_string += "R3 = "+ str(i[1]) + " on " + str(year) + "." + str(month) + "." + str(int(day)) + "; "
    if result_string.strip() == "missing meets:":
        extra_meets_raw = create_set(result_tuple1).difference(create_set(result_tuple2))
        result_string = "extra meets in RTMH report but not in rfile: " + "\n"
        for i in extra_meets_raw:
            year, month, day = jd_to_date(float(i[0]))
            result_string += "R3 = "+ str(i[1]) + " on " + str(year) + "." + str(month) + "." + str(int(day)) + "; "
    return result_string[:-2]        
    
def create_set(the_tuple):    
    queryResult, errorMessage = the_tuple
    split_list = queryResult.decode().split("\r\n")
    result_set = set()
    for i in split_list:
        if "\t" in i and "R" not in i:
            result_set.add(tuple(i.split()))
    return result_set

def jd_to_date(jd):
    """
    Convert Julian Day to date.
    
    Algorithm from 'Practical Astronomy with your Calculator or Spreadsheet', 
        4th ed., Duffet-Smith and Zwart, 2011.
    
    Parameters
    ----------
    jd : float
        Julian Day
        
    Returns
    -------
    year : int
        Year as integer. Years preceding 1 A.D. should be 0 or negative.
        The year before 1 A.D. is 0, 10 B.C. is year -9.
        
    month : int
        Month as integer, Jan = 1, Feb. = 2, etc.
    
    day : float
        Day, may contain fractional part.
        
    Examples
    --------
    Convert Julian Day 2446113.75 to year, month, and day.
    
    >>> jd_to_date(2446113.75)
    (1985, 2, 17.25)
    
    """
    jd = jd + 0.5   
    F, I = math.modf(jd)
    I = int(I)    
    A = math.trunc((I - 1867216.25)/36524.25)   
    if I > 2299160:
        B = I + 1 + A - math.trunc(A / 4.)
    else:
        B = I
    C = B + 1524    
    D = math.trunc((C - 122.1) / 365.25)    
    E = math.trunc(365.25 * D)    
    G = math.trunc((C - E) / 30.6001)    
    day = C - E + F - math.trunc(30.6001 * G)    
    if G < 13.5:
        month = G - 1
    else:
        month = G - 13       
    if month > 2.5:
        year = D - 4716
    else:
        year = D - 4715       
    return year, month, day

def check_latest_date_and_missing_meets(marketAndVariables):
    print "\n"
    print "/"*15 + "latest_date_and_missing_meets"+ "/"*(100-len("latest_date_and_missing_meets"))
    result = []
    for market in marketAndVariables:
        latestDate = check_latest_report(market, marketAndVariables[market][0][0])
        comparisionResult = compareQueries(market)
        print market, latestDate, comparisionResult
        result.append((market,latestDate, comparisionResult))
        #using the first variable for each market to check the latst date, as the date should be the same for any given market    
    return result

def generate_output_html(input_list):
    s = """\
    <!DOCTYPE html>
    <html>
    <head>
    <style>
    table {
        font-family: arial, sans-serif;
        border-collapse: collapse;
        table-layout: fixed;
    }
    
    td, th {
        border: 1px solid #dddddd;
        text-align: left;
        padding: 8px;
    }
    h3 {
    	font-family: arial, sans-serif;
    }
    tr:nth-child(even) {
        background-color: #dddddd;
    }
    </style>
    </head>
    <body>
    <h3>Compare S2 VALUE, S3 VALUE and VALUE IN RESULT</h3> 
    <table>
    <tr>
        <th>Market</th>
        <th>Variable Name</th>
        <th>Variable Code</th>
        <th>Mismatch</th>
        <th>Detail</th>
  </tr>
    """
    for item in input_list:
        s += "<tr>"
        s += '\n'
        for value in item:
            #print(value)
            if "https://" in value:
                #print("<td><a href=" + value + ">Link</a></td>")
                s += ("<td><a href=" + value + ">Link</a></td>")
                s += '\n'
            else:
                #print("<td>" + value + "</td>")
                s += ("<td>" + value + "</td>")
                s += '\n'
        s += "</tr>"
        s += '\n'
    s += """
    </table>
    
    </body>
    </html>
    """
    
    return s

def generate_output_html_2(input_list):
    s = """\
    <!DOCTYPE html>
    <html>
    <head>
    <style>
    table {
        font-family: arial, sans-serif;
        border-collapse: collapse;
        table-layout: fixed;
    }
    
    td, th {
        border: 1px solid #dddddd;
        border-collapse: collapse;
        text-align: left;
        padding: 8px;
    }
    
    tr:nth-child(even) {
        background-color: #dddddd;
    }
    h3 {
    	font-family: arial, sans-serif;
    }
    </style>
    </head>
    <body>
    <h3>Latest Report----[phase2 test]----</h3> 
    <table>
    <tr>
        <th>Market</th>
        <th>Latest Report Date</th>
        <th>Events missing in the last """ + str(further_subtract) + """ days?</th>
  </tr>
    """
    for item in input_list:
        s += "<tr>"
        s += '\n'
        for value in item:
            s += ("<td>" + value + "</td>")
            s += '\n'
        s += "</tr>"
        s += '\n'
    s += """
    </table>
    
    </body>
    </html>
    """
    
    return s                
    
def send_email():    

    # me == my email address
    # you == recipient's email address
    me = "Patrick.Shu@dataprocessors.com.au"
    #me = "OnCall@dataprocessors.com.au"
    you = "stage3qasupport@dataprocessors.com.au"
    you = "Patrick.Shu@dataprocessors.com.au"
    
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "Update Framework Daily Summary " + todaysdate_formated_2
    msg['From'] = me
    msg['To'] = you
    
    # Create the body of the message (a plain-text and an HTML version).
    s1 = check_diff(marketAndVariables)
    if s1:
        html_mismatch = generate_output_html(s1)
    else:
        html_mismatch = all_good_html
    html1 =  html_mismatch
    # Record the MIME types of both parts - text/plain and text/html.
    
    s2 = check_latest_date_and_missing_meets(marketAndVariables)
    html2 = generate_output_html_2(s2)
    
    part1 = MIMEText(html1 + html2, 'html')
    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(part1)
    
    # Send the message via local SMTP server.
    sendEmail = smtplib.SMTP('smtp.dataprocessors.com.au')
    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    sendEmail.sendmail(me, you, msg.as_string())
    sendEmail.quit()


if __name__ == "__main__":  
    send_email()
    
#===================================TEST=======================================

date_prefix_past = "&date=" + todaysdate_formated

def generate_urls_past(marketAndVariables_dict):
    for key in marketAndVariables_dict:
        market_prefix = "market=" + key
        for value in marketAndVariables_dict[key]:
            variable_prefix = "&changeType=" + value[0]
            #print(value[1])
            if value[0] in raceLevelVariables:
                value[1] = url_prefix_race + market_prefix + date_prefix_past + variable_prefix + "&debug=1"
            else:
                value[1] = url_prefix_starter + market_prefix + date_prefix_past + variable_prefix + "&debug=1"
            
    return marketAndVariables_dict

def check_diff_past(marketAndVariables_dict):
    result = []
    for market in generate_urls_past(marketAndVariables_dict):
        for variable in marketAndVariables_dict[market]:
            print(market, variable)
            if int(find_mismatch_test(parse_html(variable[1]))) > 0:
                result.append((market, variable[0], find_mismatch_test(parse_html(variable[1])), normal_url(variable[1])))
        
    return result

def find_mismatch_test(datasets):
    numOfMismatch = 0    
    for data in datasets:
        #print()
        for d in data:
            if 'S2 LIVE' in d[0]:
                s2 = d[1].split('(')[0].strip()
            if 'S3 LIVE' in d[0]:
                s3 = d[1].split('(')[0].strip()
            if 'VALUE' in d[0] and 'RESULTS' in d[0]:
                result = d[1].split('(')[0].strip()
    
        if not (s2 == s3 == result):
            print(s2,' ',s3,' ',result)
            numOfMismatch += 1
            print("numOfMismatch: ", numOfMismatch)
    
    return str(numOfMismatch)
