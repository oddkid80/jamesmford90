import sys, os, openpyxl, csv, datetime
from random import random

database_inputs = os.getcwd()+'/scripts/random_database_inputs'

write_location = os.path.dirname(os.path.abspath(__file__))

first_name_gender_file = database_inputs+'/first_names_and_gender.xlsx'
last_names_file = database_inputs+'/last_names.xlsx'

wb = openpyxl.load_workbook(filename=first_name_gender_file,read_only=False)

open_sheet = wb["Sheet1"]

max_column = open_sheet.max_column
max_row = open_sheet.max_row

data_set = []
for r in range(2,max_row+1):
    data_insert = []
    for c in range(1,max_column+1):
        data_insert.insert(c,open_sheet.cell(row=r,column=c).value)
    data_set.insert(r,data_insert)             
wb.close()

first_name = data_set.copy()

wb = openpyxl.load_workbook(filename=last_names_file,read_only=False)

open_sheet = wb["Sheet1"]

max_column = open_sheet.max_column
max_row = open_sheet.max_row

data_set = []
for r in range(2,max_row+1):
    data_insert = []
    for c in range(1,max_column+1):
        data_insert.insert(c,open_sheet.cell(row=r,column=c).value)
    data_set.insert(r,data_insert)             
wb.close()

last_name = data_set.copy()

country = [
    ('United States',0,.3)
    ,('Canada',.3,.4)
    ,('United Kingdom',.4,.5)
    ,('China',.5,.75)
    ,('Italy',.75,.85)
    ,('Spain',.85,1)
]

random_members = []

for i in range(1,75523):
    random_member = []
    age = 18+(random()*99)
    date_of_birth = datetime.date.today() - datetime.timedelta(round(age*365.25))
    country_rand = random()
    for c in country:
        if country_rand >= c[1] and country_rand < c[2]:
            member_country = c[0]
    first_name_random = round(random()*len(first_name))-1
    last_name_random = round(random()*len(last_name))-1
    random_member = [i,first_name[first_name_random][1].upper(),last_name[last_name_random][0],first_name[first_name_random][0],date_of_birth,member_country]
    random_members.append(random_member)

charge_categories = [
    ["Travel",0,.05]
    ,["Meals",.05,.3]
    ,["Lodging",.3,.35]
    ,["Groceries",.35,.6]
    ,["Entertainment",.6,.7]
    ,["Clothing",.7,.78]
    ,["Electronics",.78,.85]
    ,["Home Supplies",.85,.9]
    ,["Communication",.9,.93]
    ,["Misc",.93,1]
    ]

member_charges = []
for i in random_members:
    if random() <= .75:
        random_charge_count = round((random()*150)+10)
        for c in range(1,random_charge_count):
            charge_category_rand = random()
            
            for charge_cat in charge_categories:
                if charge_category_rand >= charge_cat[1] and charge_category_rand < charge_cat[2]:
                    charge_category = charge_cat[0]        

            if charge_category == 'Travel':
                charge_amount = (random()*(2000-300+1)+300)*(random()*(2-random()+1)+random())
            if charge_category == 'Meals':
                charge_amount = (random()*(425-6+1)+6)*(random()*(2-random()+1)+random())
            if charge_category == 'Lodging':
                charge_amount = (random()*(750-100+1)+100)*(random()*(2-random()+1)+random())
            if charge_category == 'Groceries':
                charge_amount = (random()*(450-5+1)+5)*(random()*(2-random()+1)+random())
            if charge_category == 'Entertainment':
                charge_amount = (random()*(200-20+1)+20)*(random()*(2-random()+1)+random())
            if charge_category == 'Clothing':
                charge_amount = (random()*(350-10+1)+10)*(random()*(2-random()+1)+random())
            if charge_category == 'Electronics':
                charge_amount = (random()*(3500-100+1)+100)*(random()*(2-random()+1)+random())
            if charge_category == 'Home Supplies':
                charge_amount = (random()*(1000-10+1)+10)*(random()*(2-random()+1)+random())
            if charge_category == 'Communication':
                charge_amount = (random()*(400-30+1)+30)*(random()*(2-random()+1)+random())
            if charge_category == 'Misc':
                charge_amount = (random()*(3000-10+1)+10)*(random()*(2-random()+1)+random())	

            charges = [i[0],charge_category,round(charge_amount,2),datetime.date.today()-datetime.timedelta(round(random()*730))]
            member_charges.append(charges)

random_members.insert(0,['member_id','first_name','last_name','gender','dob','country'])
member_charges.insert(0,['member_id','charge_category','charge_amount','charge_date'])

outfile = write_location+'/member.txt'
try:
    os.remove(outfile)
except:
    print("File doesn't exist.")

with open(outfile,'w',newline='') as opencsv:
    writer = csv.writer(opencsv,delimiter="|",quotechar='"',quoting=csv.QUOTE_ALL)
    for i in random_members:
        writer.writerow(i)

outfile = write_location+'/charge.txt'
try:
    os.remove(outfile)
except:
    print("File doesn't exist.")

with open(outfile,'w',newline='') as opencsv:
    writer = csv.writer(opencsv,delimiter="|",quotechar='"',quoting=csv.QUOTE_ALL)
    for i in member_charges:
        writer.writerow(i)