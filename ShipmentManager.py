import pandas as pd
import numpy as np

worldship = pd.read_csv(r'C:\NSWMS\worldship.csv')
ScannedTickets = pd.read_excel('ScannedTickets.xlsx')

for a in range (len(ScannedTickets)):
    ScannedTickets.at[a,'Index'] = int(a)

merged = pd.merge(worldship, ScannedTickets, on = 'OrderId', how = 'inner')
sort = merged.sort_values(by = 'Index')

duplicated1 = ScannedTickets[ScannedTickets.duplicated('OrderId')]
duplicated2 = sort[sort.duplicated('OrderId')]
internationals = sort[sort.ShipmentInformation_ServiceType == 'FedEx International Economy']

merged = pd.merge(worldship, ScannedTickets, on = 'OrderId', how = 'inner')
sort = merged.sort_values(by = 'Index')

sort['ShipTo_ZipCode']= sort['ShipTo_ZipCode'].astype(str).str.zfill(5)
sort['Phone_len'] = sort['ShipTo_Telephone'].astype(str).map(len)

missing = ScannedTickets[~ScannedTickets['OrderId'].isin(worldship['OrderId'])]

phoneNumbers = sort[sort.Phone_len < 10]

sort['po_box_Index'] = sort['ShipTo_StreetAddress'].str.find('box')
sort['po_Box_Index'] = sort['ShipTo_StreetAddress'].str.find('Box')
sort['po_BOX_Index'] = sort['ShipTo_StreetAddress'].str.find('BOX')
sort['PO_box_Index'] = sort['ShipTo_RoomFloorAddress2'].str.find('box')
sort['PO_Box_Index'] = sort['ShipTo_RoomFloorAddress2'].str.find('Box')
sort['PO_BOX_Index'] = sort['ShipTo_RoomFloorAddress2'].str.find('BOX')

sort['Guam_index'] = sort['ShipTo_State'].str.find('Guam')
sort['guam_index'] = sort['ShipTo_State'].str.find('guam')
sort['Puerto_index'] = sort['ShipTo_State'].str.find('Puerto')
sort['puerto_index'] = sort['ShipTo_State'].str.find('puerto')
sort['Virgin_index'] = sort['ShipTo_State'].str.find('Virgin ')
sort['virgin_index'] = sort['ShipTo_State'].str.find('virgin ')

po_box = sort[sort.po_box_Index > 0]
po_Box = sort[sort.po_Box_Index > 0]
po_BOX = sort[sort.po_BOX_Index > 0]
PO_box = sort[sort.PO_box_Index > 0]
PO_Box = sort[sort.PO_Box_Index > 0]
PO_BOX = sort[sort.PO_BOX_Index > 0]

Guam_index = sort[sort.Guam_index > 0]
guam_index = sort[sort.guam_index > 0]
Puerto_index = sort[sort.Puerto_index > 0]
puerto_index = sort[sort.puerto_index > 0]
Virgin_index = sort[sort.Virgin_index > 0]
virgin_index = sort[sort.virgin_index > 0]

Total_PO_Box = pd.concat([po_box,po_Box,po_BOX,PO_box,PO_Box,PO_BOX])
Total_territories = pd.concat([Guam_index,guam_index,Puerto_index,puerto_index,Virgin_index,virgin_index])

x=0

if (len(Total_PO_Box) > 0):
    print('Order(s) with PO box found')
    Total_PO_Box_sorted = Total_PO_Box.sort_values(by = 'Index')
    print(Total_PO_Box_sorted[['OrderId','Index']])
    x=1

if (len(Total_territories) != 0):
    print('Order(s) going to Territories')
    territory_sorted = Total_territories.sort_values(by = 'Index')
    print(territory_sorted[['OrderId','Index']])
    x=1

if(len(duplicated1) != 0):
    print('Duplicate(s) in Scanned Tickets')
    print(duplicated1)
    x=1

if(len(duplicated2 != 0)):
    print('Duplicate(s) in worldship')
    print(duplicated2)
    x=1

if(len(internationals) != 0):
    print('International Order(s) found')
    print(internationals[['OrderId']])
    x=1

if(len(missing) != 0):
    print('Orders Scanned but not in worldship file')
    print(missing['OrderId'])
    x=1

if(len(sort) == len(ScannedTickets)):
    if(len(phoneNumbers) != 0):
        for c in range(len(sort)):
                if (sort.at[c,'Phone_len'] < 10):          
                    sort.at[c,'ShipTo_Telephone'] = 5127210424
    if (x==0):
        print("Good to ship")
        sort.to_csv('Labels.csv',index=False)
        sort.to_csv('Labels-copy.csv',index=False)