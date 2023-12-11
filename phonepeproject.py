#Required packages
import os
import json
import pandas as pd
import psycopg2
import streamlit as st
import requests
import plotly.express as px
import plotly.graph_objects as go

#aggregated_transaction

path1 = "C:/Users/kavin/OneDrive/Desktop/guvi projects/phonepe_data/pulse/data/aggregated/transaction/country/india/state/"
agg_tr_list = os.listdir(path1)

columns_aggtr={"States":[],"Year":[],"Quarter":[],"Transaction_Name":[],"Transaction_Count":[],"Transaction_Amount":[]}

for state in agg_tr_list:
    statedata=path1+state+"/"
    aggtr_year_list=os.listdir(statedata)

    
    for year in aggtr_year_list:
        yeardata=statedata+year+"/"
        aggtryear_file_list=os.listdir(yeardata)

        
        for files in aggtryear_file_list:
            filedata=yeardata+files
            needed_data=open(filedata,"r")


            File_at=json.load(needed_data)

            for i in File_at["data"]["transactionData"]:
                 Name=i["name"]
                 Count=i["paymentInstruments"][0]["count"]
                 Amount=i["paymentInstruments"][0]["amount"]
                 columns_aggtr["Transaction_Name"].append(Name)
                 columns_aggtr["Transaction_Count"].append(Count)
                 columns_aggtr["Transaction_Amount"].append(Amount)
                 columns_aggtr["States"].append(state)
                 columns_aggtr["Year"].append(year)
                 columns_aggtr["Quarter"].append(int(files.strip(".json")))

DF_aggtr_columns=pd.DataFrame(columns_aggtr)
DF_aggtr_columns["States"] = DF_aggtr_columns["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
DF_aggtr_columns["States"] = DF_aggtr_columns["States"].str.replace("-"," ")
DF_aggtr_columns["States"] = DF_aggtr_columns["States"].str.title()
DF_aggtr_columns['States'] = DF_aggtr_columns['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#Aggregated User

path2="C:/Users/kavin/OneDrive/Desktop/guvi projects/phonepe_data/pulse/data/aggregated/user/country/india/state/"
agg_user_list=os.listdir(path2)

columns_agguser={"States":[],"Year":[],"Quarter":[],"User_Brand":[],"Brand_Count":[],"Brand_Percentage":[]}

for state in agg_user_list:
    statedata=path2+state+"/"
    agguser_year_list=os.listdir(statedata)
    
    for year in agguser_year_list:
        yeardata=statedata+year+"/"
        agguseryear_file_list=os.listdir(yeardata)
        
        for files in agguseryear_file_list:
            filedata=yeardata+files
            needed_data=open(filedata,"r")
    
            File_au=json.load(needed_data)
            
            try:        
                for i in File_au['data']['usersByDevice']:       
                        phbrand=i['brand']
                        phcount=i['count']
                        phpercentage=i['percentage']
                        columns_agguser["User_Brand"].append(phbrand)
                        columns_agguser["Brand_Count"].append(phcount)
                        columns_agguser["Brand_Percentage"].append(phpercentage)
                        columns_agguser["States"].append(state)
                        columns_agguser["Year"].append(year)
                        columns_agguser["Quarter"].append(int(files.strip(".json")))
            except:
                pass

DF_agg_user=pd.DataFrame(columns_agguser)
DF_agg_user["States"] = DF_agg_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
DF_agg_user["States"] = DF_agg_user["States"].str.replace("-"," ")
DF_agg_user["States"] = DF_agg_user["States"].str.title()
DF_agg_user['States'] = DF_agg_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Map Transaction

path3="C:/Users/kavin/OneDrive/Desktop/guvi projects/phonepe_data/pulse/data/map/transaction/hover/country/india/state/"
map_tr=os.listdir(path3)

columns_maptr={"States":[],"Year":[],"Quarter":[],"District_Name":[],"Transaction_Count":[],"Transaction_Amount":[]}


for state in map_tr:
    statedata=path3+state+"/"
    maptr_year=os.listdir(statedata)
    
    for year in maptr_year:
        yeardata=statedata+year+"/"
        maptryear_file=os.listdir(yeardata)
        
        for files in maptryear_file:
            filedata=yeardata+files
            needed_data=open(filedata,"r")
            
            File_mt=json.load(needed_data)
            
            for i in File_mt['data']['hoverDataList']:
                        name=i['name']
                        count=i['metric'][0]['count']
                        amount=i['metric'][0]['amount']
                        columns_maptr["District_Name"].append(name)
                        columns_maptr["Transaction_Count"].append(count)
                        columns_maptr["Transaction_Amount"].append(amount)
                        columns_maptr["States"].append(state)
                        columns_maptr["Year"].append(year)
                        columns_maptr["Quarter"].append(int(files.strip(".json")))

DF_columns_maptr=pd.DataFrame(columns_maptr)
DF_columns_maptr["States"] = DF_columns_maptr["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
DF_columns_maptr["States"] = DF_columns_maptr["States"].str.replace("-"," ")
DF_columns_maptr["States"] = DF_columns_maptr["States"].str.title()
DF_columns_maptr['States'] = DF_columns_maptr['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Map User

path4="C:/Users/kavin/OneDrive/Desktop/guvi projects/phonepe_data/pulse/data/map/user/hover/country/india/state/"
map_user=os.listdir(path4)

columns_mapuser={"States":[],"Year":[],"Quarter":[],"District_Name":[],"Registered_Users":[],"App_Opens":[]}


for state in map_user:
    statedata=path4+state+"/"
    mapuser_state=os.listdir(statedata)
    
    for year in mapuser_state:
        yeardata=statedata+year+"/"
        mapuserstate_year=os.listdir(yeardata)
        
        for files in mapuserstate_year:
            filedata=yeardata+files
            needed_data=open(filedata,"r")
            
            File_mu=json.load(needed_data)            
                             
                    
            for i in File_mu['data']['hoverData'].items():
                            district=i[0]
                            users=i[1]['registeredUsers']
                            appopens=i[1]['appOpens']
                            columns_mapuser["District_Name"].append(district)
                            columns_mapuser["Registered_Users"].append(users)
                            columns_mapuser["App_Opens"].append(appopens)
                            columns_mapuser["States"].append(state)
                            columns_mapuser["Year"].append(year)
                            columns_mapuser["Quarter"].append(int(files.strip(".json")))                             
DF_columns_mapuser=pd.DataFrame(columns_mapuser)
DF_columns_mapuser["States"] = DF_columns_mapuser["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
DF_columns_mapuser["States"] = DF_columns_mapuser["States"].str.replace("-"," ")
DF_columns_mapuser["States"] = DF_columns_mapuser["States"].str.title()
DF_columns_mapuser['States'] = DF_columns_mapuser['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

# Top Transaction

path5="C:/Users/kavin/OneDrive/Desktop/guvi projects/phonepe_data/pulse/data/top/transaction/country/india/state/"

top_trans=os.listdir(path5)

columns_top_trans={"States":[],"Year":[],"Quarter":[],"District_Name":[],"Total_Transactions":[],"Transaction_Amount":[]}
columns_top_trans1={"States":[],"Year":[],"Quarter":[],"Pincode":[],"Total_Transactions":[],"Transaction_Amount":[]}


for state in top_trans:
    statedata=path5+state+"/"
    toptrans_state=os.listdir(statedata)
    
    for year in toptrans_state:
        yeardata=statedata+year+"/"
        toptransstate_year=os.listdir(yeardata)
        
        for files in toptransstate_year:
            filedata=yeardata+files
            needed_data=open(filedata,"r")
            
            File_tt=json.load(needed_data)
            
            
            for i in File_tt['data']['districts']:
                        district=i['entityName']
                        count=i['metric']['count']
                        amount=i['metric']['amount']
                        columns_top_trans["District_Name"].append(district)
                        columns_top_trans["Total_Transactions"].append(count)
                        columns_top_trans["Transaction_Amount"].append(amount)
                        columns_top_trans["States"].append(state)
                        columns_top_trans["Year"].append(year)
                        columns_top_trans["Quarter"].append(int(files.strip(".json")))
                        
            for i in File_tt['data']['pincodes']:
                        pincode=i['entityName']
                        count=i['metric']['count']
                        amount=i['metric']['amount']
                        columns_top_trans1["Pincode"].append(pincode)
                        columns_top_trans1["Total_Transactions"].append(count)
                        columns_top_trans1["Transaction_Amount"].append(amount)
                        columns_top_trans1["States"].append(state)
                        columns_top_trans1["Year"].append(year)
                        columns_top_trans1["Quarter"].append(int(files.strip(".json")))             
DF_columns_top_trans=pd.DataFrame(columns_top_trans)
DF_columns_top_trans["States"] = DF_columns_top_trans["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
DF_columns_top_trans["States"] = DF_columns_top_trans["States"].str.replace("-"," ")
DF_columns_top_trans["States"] = DF_columns_top_trans["States"].str.title()
DF_columns_top_trans['States'] = DF_columns_top_trans['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

DF_columns_top_trans1=pd.DataFrame(columns_top_trans1)
DF_columns_top_trans1["States"] = DF_columns_top_trans1["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
DF_columns_top_trans1["States"] = DF_columns_top_trans1["States"].str.replace("-"," ")
DF_columns_top_trans1["States"] = DF_columns_top_trans1["States"].str.title()
DF_columns_top_trans1['States'] = DF_columns_top_trans1['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

#Top User

path6="C:/Users/kavin/OneDrive/Desktop/guvi projects/phonepe_data/pulse/data/top/user/country/india/state/"

top_user=os.listdir(path6)

columns_top_user={"States":[],"Year":[],"Quarter":[],"District_Name":[],"Registered_Users":[]}
columns_top_user1={"States":[],"Year":[],"Quarter":[],"Pincode":[],"Registered_Users":[]}


for state in top_user:
    statedata=path6+state+"/"
    topuser_state=os.listdir(statedata)
    
    for year in topuser_state:
        yeardata=statedata+year+"/"
        topuserstate_year=os.listdir(yeardata)
        
        for files in topuserstate_year:
            filedata=yeardata+files
            needed_data=open(filedata,"r")
            
            File=json.load(needed_data)
            
            for i in File['data']['districts']:
                        district=i['name']
                        users=i['registeredUsers']
                        columns_top_user["District_Name"].append(district)
                        columns_top_user["Registered_Users"].append(users)
                        columns_top_user["States"].append(state)
                        columns_top_user["Year"].append(year)
                        columns_top_user["Quarter"].append(int(files.strip(".json")))
    
    
            for i in File['data']['pincodes']:
                        pincode=i['name']
                        users=i['registeredUsers']
                        columns_top_user1["Pincode"].append(pincode)
                        columns_top_user1["Registered_Users"].append(users)
                        columns_top_user1["States"].append(state)
                        columns_top_user1["Year"].append(year)
                        columns_top_user1["Quarter"].append(int(files.strip(".json")))    
DF_columns_top_user=pd.DataFrame(columns_top_user)
DF_columns_top_user["States"] = DF_columns_top_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
DF_columns_top_user["States"] = DF_columns_top_user["States"].str.replace("-"," ")
DF_columns_top_user["States"] = DF_columns_top_user["States"].str.title()
DF_columns_top_user['States'] = DF_columns_top_user['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")

DF_columns_top_user1=pd.DataFrame(columns_top_user1)
DF_columns_top_user1["States"] = DF_columns_top_user1["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
DF_columns_top_user1["States"] = DF_columns_top_user1["States"].str.replace("-"," ")
DF_columns_top_user1["States"] = DF_columns_top_user1["States"].str.title()
DF_columns_top_user1['States'] = DF_columns_top_user1['States'].str.replace("Dadra & Nagar Haveli & Daman & Diu", "Dadra and Nagar Haveli and Daman and Diu")


#SQL Table Functions
#Agg_Trans table

sqldb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="kavin",
                        database="Phonepe_Data",
                        port="5432")
cursor=sqldb.cursor()

create_query1='''create table if not exists Agg_Transaction(States varchar(100),
                                                           Year int,
                                                           Quarter int,
                                                           Transaction_Name varchar(100),
                                                           Transaction_Count bigint,
                                                           Transaction_Amount float)'''
cursor.execute(create_query1)
sqldb.commit()


for index,row in DF_aggtr_columns.iterrows():
    insert_query1 = '''insert into Agg_Transaction(States,
                                                  Year,
                                                  Quarter,
                                                  Transaction_Name,
                                                  Transaction_Count,
                                                  Transaction_Amount)
                                        
                                            values(%s,%s,%s,%s,%s,%s)'''

    values=(row['States'],
            row['Year'],
            row['Quarter'],
            row['Transaction_Name'],
            row['Transaction_Count'],
            row['Transaction_Amount'])


    cursor.execute(insert_query1,values)
    sqldb.commit()

    
#Agg_User Table
 
create_query2='''create table if not exists Agg_User(States varchar(100),
                                                    Year int,
                                                    Quarter int,
                                                    User_Brand varchar(100),
                                                    Brand_Count bigint,
                                                    Brand_Percentage float)'''

cursor.execute(create_query2)
sqldb.commit()


for index,row in DF_agg_user.iterrows():
    insert_query2 = '''insert into Agg_User(States,
                                          Year,
                                          Quarter,
                                          User_Brand,
                                          Brand_Count,
                                          Brand_Percentage)
                                          values(%s,%s,%s,%s,%s,%s)'''

    values=(row['States'],
            row['Year'],
            row['Quarter'],
            row['User_Brand'],
            row['Brand_Count'],
            row['Brand_Percentage'])


    cursor.execute(insert_query2,values)
    sqldb.commit()

#Map Trans Table

create_query3='''create table if not exists Map_Transaction(States varchar(100),
                                                           Year int,
                                                           Quarter int,
                                                           District_Name varchar(100),
                                                           Transaction_Count bigint,
                                                           Transaction_Amount float)'''

cursor.execute(create_query3)
sqldb.commit()


for index,row in DF_columns_maptr.iterrows():
    insert_query3 = '''insert into Map_Transaction(States,
                                                  Year,
                                                  Quarter,
                                                  District_Name,
                                                  Transaction_Count,
                                                  Transaction_Amount)
                                                 values(%s,%s,%s,%s,%s,%s)'''

    values=(row['States'],
            row['Year'],
            row['Quarter'],
            row['District_Name'],
            row['Transaction_Count'],
            row['Transaction_Amount'])


    cursor.execute(insert_query3,values)
    sqldb.commit() 

#Map User Table

create_query4 ='''create table if not exists Map_User(States varchar(100),
                                                   Year int,
                                                   Quarter int,
                                                   District_Name varchar(100),
                                                   Registered_Users bigint,
                                                   App_Opens bigint)'''

cursor.execute(create_query4)
sqldb.commit()


for index,row in DF_columns_mapuser.iterrows():
    insert_query4 = '''insert into Map_User(States,
                                          Year,
                                          Quarter,
                                          District_Name,
                                          Registered_Users,
                                          App_Opens)
                                        values(%s,%s,%s,%s,%s,%s)'''

    values=(row['States'],
            row['Year'],
            row['Quarter'],
            row['District_Name'],
            row['Registered_Users'],
            row['App_Opens'])


    cursor.execute(insert_query4,values)
    sqldb.commit()

#Top Trans District Table

create_query5 ='''create table if not exists District_Top_Trans(States varchar(100),
                                                           Year int,
                                                           Quarter int,
                                                           District_Name varchar(100),
                                                           Total_Transactions bigint,
                                                           Transaction_Amount float)'''

cursor.execute(create_query5)
sqldb.commit()


for index,row in DF_columns_top_trans.iterrows():
    insert_query5 = '''insert into District_Top_Trans(States,
                                                  Year,
                                                  Quarter,
                                                  District_Name,
                                                  Total_Transactions,
                                                  Transaction_Amount)
                                                values(%s,%s,%s,%s,%s,%s)'''

    values=(row['States'],
            row['Year'],
            row['Quarter'],
            row['District_Name'],
            row['Total_Transactions'],
            row['Transaction_Amount'])


    cursor.execute(insert_query5,values)
    sqldb.commit()

#Top Trans Pincode Table

create_query6 ='''create table if not exists Pincode_Top_Trans(States varchar(100),
                                                           Year int,
                                                           Quarter int,
                                                           Pincode bigint,
                                                           Total_Transactions bigint,
                                                           Transaction_Amount float)'''

cursor.execute(create_query6)
sqldb.commit()

for index,row in DF_columns_top_trans1.iterrows():
    insert_query6 = '''insert into Pincode_Top_Trans(States,
                                                  Year,
                                                  Quarter,
                                                  Pincode,
                                                  Total_Transactions,
                                                  Transaction_Amount)
                                                  values(%s,%s,%s,%s,%s,%s)'''

    values=(row['States'],
            row['Year'],
            row['Quarter'],
            row['Pincode'],
            row['Total_Transactions'],
            row['Transaction_Amount'])


    cursor.execute(insert_query6,values)
    sqldb.commit()

#Top User District Table

create_query7 ='''create table if not exists District_Top_User(States varchar(100),
                                                            Year int,
                                                            Quarter int,
                                                            District_Name varchar(100),
                                                            Registered_Users bigint)'''

cursor.execute(create_query7)
sqldb.commit()


for index,row in DF_columns_top_user.iterrows():
    insert_query7 = '''insert into District_Top_User(States,
                                                  Year,
                                                  Quarter,
                                                  District_Name,
                                                  Registered_Users)
                                                values(%s,%s,%s,%s,%s)'''

    values=(row['States'],
            row['Year'],
            row['Quarter'],
            row['District_Name'],
            row['Registered_Users'])


    cursor.execute(insert_query7,values)
    sqldb.commit()

#Top User Pincode Table

create_query8 ='''create table if not exists Pincode_Top_User(States varchar(100),
                                                           Year int,
                                                           Quarter int,
                                                           Pincode bigint,
                                                           Registered_Users bigint)'''

cursor.execute(create_query8)
sqldb.commit()


for index,row in DF_columns_top_user1.iterrows():
    insert_query8 = '''insert into Pincode_Top_User(States,
                                                  Year,
                                                  Quarter,
                                                  Pincode,
                                                  Registered_Users)
                                                 values(%s,%s,%s,%s,%s)'''

    values=(row['States'],
            row['Year'],
            row['Quarter'],
            row['Pincode'],
            row['Registered_Users'])


    cursor.execute(insert_query8,values)
    sqldb.commit()

#SQL Connection
sqldb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="kavin",
                        database="Phonepe_Data",
                        port="5432")
cursor=sqldb.cursor()

#Forming DataFrames from Fetching data from SQL Tables

cursor.execute("SELECT * FROM Agg_Transaction")
sqldb.commit()
Aggtr = cursor.fetchall()
Agg_trans = pd.DataFrame(Aggtr,columns=("States","Years","Quarter","Transaction_Name","Transaction_Count","Transaction_Amount"))


cursor.execute("SELECT * FROM Agg_User")
sqldb.commit()
Agguser = cursor.fetchall()
Agg_user = pd.DataFrame(Agguser,columns=("States","Years","Quarter","User_Brand","Brand_Count","Brand_Percentage"))


cursor.execute("SELECT * FROM Map_Transaction")
sqldb.commit()
Maptrans = cursor.fetchall()
Map_trans = pd.DataFrame(Maptrans,columns=("States","Years","Quarter","District_Name","Transaction_Count","Transaction_Amount"))


cursor.execute("SELECT * FROM Map_User")
sqldb.commit()
Mapuser = cursor.fetchall()
Map_user = pd.DataFrame(Mapuser,columns=("States","Years","Quarter","District_Name","Registered_Users","App_Opens"))


cursor.execute("SELECT * FROM District_Top_Trans")
sqldb.commit()
Toptransdis = cursor.fetchall()
Top_trans_dis = pd.DataFrame(Toptransdis,columns=("States","Years","Quarter","District_Name","Total_Transactions","Transaction_Amount"))


cursor.execute("SELECT * FROM Pincode_Top_Trans")
sqldb.commit()
Toptranspin = cursor.fetchall()
Top_trans_pin = pd.DataFrame(Toptranspin,columns=("States","Years","Quarter","Pincode","Total_Transactions","Transaction_Amount"))


cursor.execute("SELECT * FROM District_Top_User")
sqldb.commit()
Topuserdis = cursor.fetchall()
Top_user_dis = pd.DataFrame(Topuserdis,columns=("States","Years","Quarter","District_Name","Registered_Users"))


cursor.execute("SELECT * FROM Pincode_Top_User")
sqldb.commit()
Topuserpin = cursor.fetchall()
Top_user_pin = pd.DataFrame(Topuserpin,columns=("States","Years","Quarter","Pincode","Registered_Users"))


#Function for India map visual for Transaction Amount for All years
def visual_total_amount():
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response =requests.get(url)
    data1 = json.loads(response.content)
    state_names_tra = [feature["properties"]["ST_NM"] for feature in data1["features"]]
    state_names_tra.sort()
    pd.DataFrame({"States":state_names_tra})

    df = []

    for year in Map_user["Years"].unique():
        for quarter in Agg_trans["Quarter"].unique():

            at = Agg_trans[(Agg_trans["Years"]==year)&(Agg_trans["Quarter"]==quarter)]
            at1 = at[["States","Transaction_Amount"]]
            at1 = at1.sort_values(by="States")
            at1["Years"]=year
            at1["Quarter"]=quarter
            df.append(at1)

    concat_df = pd.concat(df)         

    fig_at = px.choropleth(concat_df, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                            color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "TRANSACTION AMOUNT",
                            animation_frame="Years", animation_group="Quarter")

    fig_at.update_geos(fitbounds= "locations", visible =False)
    fig_at.update_layout(width =500, height= 500)
    fig_at.update_layout(title_font= {"size":25})
    return st.plotly_chart(fig_at)


#Function for Transaction name and Count for All years
def count_plot():
    at= Agg_trans[["Transaction_Name", "Transaction_Count"]]
    at1= at.groupby("Transaction_Name")["Transaction_Count"].sum()
    df_at1= pd.DataFrame(at1).reset_index()
    fig_at= px.bar(df_at1,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION NAME VS TRANSACTION COUNT",
                color_discrete_sequence=px.colors.sequential.Redor_r)
    fig_at.update_layout(width=600, height= 500)
    return st.plotly_chart(fig_at)

#Function for India map visual for Transaction Count for All years
def visual_total_count():
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response =requests.get(url)
    data1 = json.loads(response.content)
    state_names_tra = [feature["properties"]["ST_NM"] for feature in data1["features"]]
    state_names_tra.sort()

    pd.DataFrame({"States":state_names_tra})

    df = []

    for year in Agg_trans["Years"].unique():
        for quarter in Agg_trans["Quarter"].unique():          
                    at = Agg_trans[(Agg_trans["Years"]==year)&(Agg_trans["Quarter"]==quarter)]
                    at1 = at[["States","Transaction_Count"]]
                    at1 = at1.sort_values(by="States")
                    at1["Years"]=year
                    at1["Quarter"]=quarter
                    df.append(at1)

    concat_df = pd.concat(df)

    fig_at = px.choropleth(concat_df, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                            color_continuous_scale= "rainbow", range_color= (0,300000000000), hover_name= "States", title = "TRANSACTION COUNT",
                            animation_frame="Years", animation_group="Quarter")

    fig_at.update_geos(fitbounds= "locations", visible =False)
    fig_at.update_layout(width =500, height= 500)
    fig_at.update_layout(title_font= {"size":25})
    return st.plotly_chart(fig_at)
    
            
#Function for plotting Transaction name and amount for All years
def amount_plot():
    at= Agg_trans[["Transaction_Name", "Transaction_Amount"]]
    at1= at.groupby("Transaction_Name")["Transaction_Amount"].sum()
    df_at1= pd.DataFrame(at1).reset_index()
    fig_at= px.bar(df_at1,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION NAME VS TRANSACTION AMOUNT",
                color_discrete_sequence=px.colors.sequential.Peach_r)
    fig_at.update_layout(width=600, height= 500)
    return st.plotly_chart(fig_at)


#Function for plotting All District wise registered users
def plot_all_state(state):
    mu= Map_user[["States","District_Name","Registered_Users"]]
    mu1= mu.loc[(mu["States"]==state)]
    mu2= mu1[["District_Name","Registered_Users"]]
    mu3= mu2.groupby("District_Name")["Registered_Users"].sum()
    mu4= pd.DataFrame(mu3).reset_index()
    fig_mu= px.bar(mu4, x= "District_Name", y= "Registered_Users", title= "DISTRICT and REGISTERED USERS",
                   color_discrete_sequence=px.colors.sequential.YlGn_r)
    fig_mu.update_layout(width= 1000, height= 500)
    return st.plotly_chart(fig_mu)


#Function for India map visual for yearwise Transaction Amount
def visual_totalyear_amount(select_year):
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response =requests.get(url)
    data1 = json.loads(response.content)
    state_names_tra = [feature["properties"]["ST_NM"] for feature in data1["features"]]
    state_names_tra.sort()

    year = int(select_year)
    aty = Agg_trans[["States","Years","Transaction_Amount"]]
    aty1 = aty.loc[(Agg_trans["Years"]==year)]
    aty2 = aty1.groupby("States")["Transaction_Amount"].sum()
    aty3 = pd.DataFrame(aty2).reset_index()

    fig_aty = px.choropleth(aty3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Amount",
                            color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "States", title = "STATE AND TRANSACTION AMOUNT")
    fig_aty.update_geos(fitbounds= "locations", visible =False)
    fig_aty.update_layout(width =700, height= 600)
    fig_aty.update_layout(title_font= {"size":25})
    return st.plotly_chart(fig_aty)

#Function for yearwise Transaction name and Count
def year_count_plot(select_year):
    year = int(select_year)
    aty = Agg_trans[["Transaction_Name","Years","Transaction_Count"]]
    aty1 = aty.loc[(Agg_trans["Years"]==year)]
    aty2= aty1.groupby("Transaction_Name")["Transaction_Count"].sum()
    df_aty2= pd.DataFrame(aty2).reset_index()
    
    fig_aty= px.bar(df_aty2,x= "Transaction_Name",y= "Transaction_Count",title= "TRANSACTION NAME VS TRANSACTION COUNT",
                color_discrete_sequence=px.colors.sequential.Burg_r)
    fig_aty.update_layout(width=600, height= 500)
    return st.plotly_chart(fig_aty)

#Function for India map visual for yearwise Transaction Count
def visual_totalyear_count(select_year):
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response =requests.get(url)
    data1 = json.loads(response.content)
    state_names_tra = [feature["properties"]["ST_NM"] for feature in data1["features"]]
    state_names_tra.sort()

    year = int(select_year)
    aty = Agg_trans[["States","Years","Transaction_Count"]]
    aty1 = aty.loc[(Agg_trans["Years"]==year)]
    aty2 = aty1.groupby("States")["Transaction_Count"].sum()
    aty3 = pd.DataFrame(aty2).reset_index()

    fig_aty = px.choropleth(aty3, geojson= data1, locations= "States", featureidkey= "properties.ST_NM", color= "Transaction_Count",
                            color_continuous_scale= "rainbow", range_color= (0,3000000000), hover_name= "States", title = "STATE AND TRANSACTION COUNT")
    fig_aty.update_geos(fitbounds= "locations", visible = False)
    fig_aty.update_layout(width =600, height= 500)
    fig_aty.update_layout(title_font= {"size":25})
    return st.plotly_chart(fig_aty)

#Function for plotting yearwise Transaction Name and Amount
def year_amount_plot(select_year):
    year = int(select_year)
    aty = Agg_trans[["Years","Transaction_Name","Transaction_Amount"]]
    aty1 = aty.loc[(Agg_trans["Years"]==year)]
    aty2= aty1.groupby("Transaction_Name")["Transaction_Amount"].sum()
    df_aty2= pd.DataFrame(aty2).reset_index()
    
    fig_aty= px.bar(df_aty2,x= "Transaction_Name",y= "Transaction_Amount",title= "TRANSACTION NAME VS TRANSACTION AMOUNT",
                color_discrete_sequence=px.colors.sequential.BuGn_r)
    fig_aty.update_layout(width=600, height= 500)
    return st.plotly_chart(fig_aty)
      
#Function for year,state wise Registered Users
def map_state_user(select_year,state):
    year = int(select_year)
    mu = Map_user[["States","Years","District_Name","Registered_Users"]]
    mu1 = mu.loc[(Map_user["States"]==state)&(Map_user["Years"]==year)]
    mu2 = mu1.groupby("District_Name")["Registered_Users"].sum()
    df_mu2 = pd.DataFrame(mu2).reset_index()

    fig_mu= px.bar(df_mu2,x= "District_Name",y= "Registered_Users",title= "DISTRICT NAME VS REGISTERED USERS",
                color_discrete_sequence=px.colors.sequential.Blugrn_r)
    fig_mu.update_layout(width=600, height= 500)
    return st.plotly_chart(fig_mu)

#Function for year,state wise Transaction Amount
def map_state_trans(select_year,state):
    year = int(select_year)
    mt = Map_trans[["States","Years","District_Name","Transaction_Amount"]]
    mt1 = mt.loc[(Map_trans["States"]==state)&(Map_trans["Years"]==year)]
    mt2 = mt1.groupby("District_Name")["Transaction_Amount"].sum()
    df_mt2 = pd.DataFrame(mt2).reset_index()

    fig_mt= px.bar(df_mt2,x= "District_Name",y= "Transaction_Amount",title= "DISTRICT NAME VS TRANSACTION AMOUNT",
                     color_discrete_sequence=px.colors.sequential.RdBu_r)
    fig_mt.update_layout(width=600, height= 500)
    return st.plotly_chart(fig_mt)

#Functions for Fact Questions

def ques1():
    amount= Agg_trans[["States","Transaction_Amount"]]
    amount1= amount.groupby("States")["Transaction_Amount"].sum().sort_values(ascending=True)
    amount2= pd.DataFrame(amount1).reset_index().head(10)

    fig_amount = px.bar(amount2, x= "States", y= "Transaction_Amount",title= "TEN STATES WITH LOWEST TRANSACTION AMOUNT",
                        color_discrete_sequence= px.colors.sequential.Oranges_r)
    return st.plotly_chart(fig_amount)

def ques2():
    brand = Agg_user[["User_Brand","Brand_Count"]]
    brand1 = brand.groupby("User_Brand")["Brand_Count"].sum().sort_values(ascending=False)
    brand2 = pd.DataFrame(brand1).reset_index().head(10)

    fig_brand = px.pie(brand2, values= "Brand_Count", names= "User_Brand", color_discrete_sequence=px.colors.sequential.Plasma_r,
                        title= "TOP TEN MOBILE BRANDS AND THEIR COUNTS")
    return st.plotly_chart(fig_brand)

def ques3():
    brand = Agg_user[["User_Brand","Brand_Percentage"]]
    brand1 = brand.groupby("User_Brand")["Brand_Percentage"].sum().sort_values(ascending=False)
    brand2 = pd.DataFrame(brand1).reset_index().head(10)

    fig_brand = px.pie(brand2, values= "Brand_Percentage", names= "User_Brand", color_discrete_sequence=px.colors.sequential.Blugrn_r,
                        title= "TOP TEN MOBILE BRANDS AND THEIR PERCENTAGE")
    return st.plotly_chart(fig_brand)
    
def ques4():
    amount= Map_trans[["States","Transaction_Amount"]]
    amount1= amount.groupby("States")["Transaction_Amount"].sum().sort_values(ascending=False)
    amount2= pd.DataFrame(amount1).reset_index().head(10)

    fig_amount = px.bar(amount2, x= "state", y= "Transaction_Amount",title= "STATE WITH TEN HIGHEST TRANSACTION AMOUNT",
                        color_discrete_sequence= px.colors.sequential.Peach_r)
    return st.plotly_chart(fig_amount)

def ques5():
    amount= Map_trans[["States","Transaction_Amount"]]
    amount1= amount.groupby("States")["Transaction_Amount"].sum().sort_values(ascending=True)
    amount2= pd.DataFrame(amount1).reset_index()

    fig_amount = px.bar(amount2, x= "States", y= "Transaction_Amount",title= "STATE WITH ITS TRANSACTION AMOUNT",
                        color_discrete_sequence= px.colors.sequential.Peach_r)
    return st.plotly_chart(fig_amount)

def ques6():
    user = Map_user[["States","Registered_Users"]]
    user1 = user.groupby("States")["Registered_Users"].sum().sort_values(ascending=True)
    user2 = pd.DataFrame(user1).reset_index()

    fig_user = px.bar(user2, x= "States", y= "Registered_Users",title= "STATE AND ITS REGISTERED USERS",
                        color_discrete_sequence= px.colors.sequential.Greens_r)
    return st.plotly_chart(fig_user)

def ques7():
    app = Map_user[["States","App_Opens"]]
    app1 = app.groupby("States")["App_Opens"].sum().sort_values(ascending=False)
    app2 = pd.DataFrame(app1).reset_index()

    fig_app = px.bar(app2, x= "States", y= "App_Opens",title= "STATE AND ITS NO OF APP_OPENS",
                        color_discrete_sequence= px.colors.sequential.Greens_r)
    return st.plotly_chart(fig_app)

def ques8():
    amount= Top_trans_dis[["District_Name","Transaction_Amount"]]
    amount1= amount.groupby("District_Name")["Transaction_Amount"].sum().sort_values(ascending=True)
    amount2= pd.DataFrame(amount1).reset_index().head(20)

    fig_amount = px.bar(amount2, x= "District_Name", y= "Transaction_Amount",title= "20 DISTRICTS WITH LOWEST TRANSACTION AMOUNT ",
                        color_discrete_sequence= px.colors.sequential.Oranges_r)
    return st.plotly_chart(fig_amount)

def ques9():
    user = Top_user_dis[["District_Name","Registered_Users"]]
    user1 = user.groupby("District_Name")["Registered_Users"].sum().sort_values(ascending=False)
    user2 = pd.DataFrame(user1).reset_index().head(20)

    fig_user = px.bar(user2, x= "District_Name", y= "Registered_Users",title= "TOP 20 DISTRICTS AND ITS REGISTERED USERS",
                      color_discrete_sequence= px.colors.sequential.OrRd_r)
    return st.plotly_chart(fig_user)

def ques10():
    user = Top_user_pin[["Pincode","Registered_Users"]]
    user1 = user.groupby("Pincode")["Registered_Users"].sum().sort_values(ascending=False)
    user2 = pd.DataFrame(user1).reset_index().head(10) 


    fig_user = px.bar(user2, x="Pincode" , y= "Registered_Users",title= "TOP TEN REGISTERED USERS AND ITS PINCODES",
                            color_discrete_sequence= px.colors.sequential.PuBu_r)
    fig_user.update_xaxes(type='category')
    return st.plotly_chart(fig_user)

#streamlit part for page layout

st.set_page_config(layout="wide")

st.title("Phonepe Pulse Data Visualization and Exploration")
tab1, tab2, tab3 = st.tabs(["**HOME**","**DATA EXPLORATION**","**ANALYSIS CHARTS**"])


#STREAMLIT CODE
#streamlit part for phonepe intro

with tab1:
    col1,col2 = st.columns(2)

    with col1:
        st.header("PhonePe")
        st.subheader("INDIA'S NO1 TRANSACTION APP")
        st.markdown("PhonePe is an Indian digital payments and leading online payment app")
        st.write("**KEY FEATURES**")
        st.write("**Mobile Banking using Card Linking**")
        st.write("**Secure PIN Autorization**")
        st.write("**Instant Money Transfers**")
        st.write("**Checking Bank Balance**")
        st.download_button("Download the APP","https://www.phonepe.com/app-download/")


    with col2:
        st.write("PHONE-PE a leading Indian company's transaction and usage has been analysed and reports were published")
        st.write("**Reliable Payment Gateway**")
        st.write("**Instant Money Transfers**")
        st.write("**One app for all things money**")
        st.write("**Pay whenever you like, wherever you like**")
        st.write("**Find all your favourite apps on PhonePe Switch.**")
        st.write("**Scan QR Code**")
        st.write("**Earn Valuable Rewards**")
        st.write("**Simple,Fast & Secure**")

#streamlit part visual data

with tab2:
    select_year = st.selectbox("Select the Year",("All","2018","2019","2020","2021","2022","2023"))
    if select_year == "All" :
        col1,col2 = st.columns(2)
        with col1:
             visual_total_amount()
             count_plot()
        with col2:
            visual_total_count()
            amount_plot()
            
        state = st.selectbox("Select the State",('Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh',
                                                'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
                                                'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Goa',
                                                'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir',
                                                'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep',
                                                'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram',
                                                'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan',
                                                'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh',
                                                'Uttarakhand', 'West Bengal'))
        
        plot_all_state(state)

    else:
        col1,col2 = st.columns(2)

        with col1:
            visual_totalyear_amount(select_year)
            year_count_plot(select_year)     

        with col2:
            visual_totalyear_count(select_year)
            year_amount_plot(select_year)

            state = st.selectbox("Select the State",('Andaman & Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh',
                                                'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
                                                'Dadra and Nagar Haveli and Daman and Diu', 'Delhi', 'Goa',
                                                'Gujarat', 'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir',
                                                'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep',
                                                'Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 'Mizoram',
                                                'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan',
                                                'Sikkim', 'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh',
                                                'Uttarakhand', 'West Bengal'))

            
            map_state_user(select_year,state)
            map_state_trans(select_year,state)


#streamlit part for Fact Questions           

with tab3:
    Facts = st.selectbox("Select a Question",("TEN STATE WITH LOWEST TRANSACTION AMOUNT","TOP TEN MOBILE BRANDS AND THEIR COUNTS",
                                              "TOP TEN MOBILE BRANDS AND THEIR PERCENTAGE","STATE WITH TEN HIGHEST TRANSACTION AMOUNT",
                                              "STATE WITH ITS TRANSACTION AMOUNT","STATE AND ITS REGISTERED USERS",
                                              "STATE AND ITS NO OF APP_OPENS","20 DISTRICTS WITH LOWEST TRANSACTION AMOUNT ",
                                              "TOP 20 DISTRICTS AND ITS REGISTERED USERS","TOP TEN REGISTERED USERS AND ITS PINCODES"))

    if Facts == "TEN STATE WITH LOWEST TRANSACTION AMOUNT":
        ques1()

    elif Facts == "TOP TEN MOBILE BRANDS AND THEIR COUNTS":
        ques2()

    elif Facts == "TOP TEN MOBILE BRANDS AND THEIR PERCENTAGE":
        ques3()   

    elif Facts == "STATE WITH TEN HIGHEST TRANSACTION AMOUNT":
        ques4()

    elif Facts == "STATE WITH ITS TRANSACTION AMOUNT":
        ques5()      

    elif Facts == "STATE AND ITS REGISTERED USERS":
        ques6()    

    elif Facts == "STATE AND ITS NO OF APP_OPENS":
        ques7()   

    elif Facts == "20 DISTRICTS WITH LOWEST TRANSACTION AMOUNT ":
        ques8()

    elif Facts == "TOP 20 DISTRICTS AND ITS REGISTERED USERS":
        ques9()    

    elif Facts == "TOP TEN REGISTERED USERS AND ITS PINCODES":
        ques10()