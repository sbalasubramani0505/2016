#!/usr/bin/env python
#Imports
import sys
import os
import re
import datetime

#File operation
file = sys.stdin
file.next()

#Iterate through file
for line in file:
    elements = line.strip().split("\t")
    if elements.__len__() == 7 :
        Error_Msg = ""
        if ":" in elements[0]:  ##Split first field into Orderid and Date
            Order_id_dt = re.sub('\:+', ':',elements[0]).split(":")
            Order_id = Order_id_dt[0]
            ## Reformat the date if valid
            if Order_id_dt[1] and datetime.datetime.strptime(Order_id_dt[1], '%Y%m%d').strftime('%Y-%m-%d'):
                Order_dt = datetime.datetime.strptime(Order_id_dt[1], '%Y%m%d').strftime('%Y-%m-%d')
            else:
                Order_dt = Order_id_dt[1]
                if (Error_Msg.__len__() != 0):
                    Error_Msg = Error_Msg + "| Invalid Date"
                else:
                    Error_Msg = Error_Msg + "Invalid URL"
        else:
            Order_id = elements[0]
            Order_dt = "null"
        URL = ""
        if (not elements[6].startswith('http://www.insacart.com/')):
            if ( Error_Msg.__len__() != 0):
                Error_Msg = Error_Msg + "| Invalid URL"
            else:
                Error_Msg = Error_Msg + "Invalid URL"
        else:
            URL = elements[6] ## Valid URL
        Sum_Item = 0
        Item_Cnt = 0
	## Find average of items
        for i in range(2, 6):
            Sum_Item = Sum_Item + float((elements[i] or 0))
            if (elements[i] and elements[i] != "0"):
                Item_Cnt = Item_Cnt + 1
	if (Item_Cnt != 0):
        	Avg_Item = Sum_Item / Item_Cnt
        print Order_id + "\t" + Order_dt + "\t" + elements[1] + "\t" + str(Avg_Item) + "\t" + URL + "\t" + Error_Msg
    else:
        print line.rstrip('\n') + "\t" + "Malformed Record" ## If number of fields not equal to 7 -- Add malformed record msg