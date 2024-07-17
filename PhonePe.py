import streamlit as st
from streamlit_option_menu import option_menu
import sqlite3
import pandas as pd
import plotly.express as px
import requests
import json
from PIL import Image
import os

# DATA COLLECTION FROM THE GITHUB

# Data Collection 1 : aggre_transaction

path1="/content/pulse/data/aggregated/transaction/country/india/state/"
agg_tran_list= os.listdir(path1)

# Data Collection 2 : aggre_user

path2="/content/pulse/data/aggregated/user/country/india/state/"
agg_user_list= os.listdir(path2)

# Data Collection 3 : map_transaction

path3="/content/pulse/data/map/transaction/hover/country/india/state/"
map_tran_list= os.listdir(path3)

# Data Collection 4 : map_user

path4="/content/pulse/data/map/user/hover/country/india/state/"
map_user_list= os.listdir(path4)

# Data Collection 5 : top_transaction

path5="/content/pulse/data/top/transaction/country/india/state/"
top_tran_list= os.listdir(path5)

#  Data Collection 6 : top_user

path6="/content/pulse/data/top/user/country/india/state/"
top_tran_list= os.listdir(path6)

# DATA FRAME CREATION FROM THE EXTACTED DETAILS

# Data Frame Creation 1 : aggre_transaction

columns1={"States":[], "Years":[], "Quarter":[], "Transaction_type":[], "Transaction_count":[], "Transaction_amount":[]}

for state in agg_tran_list:
    cur_states=path1+state+"/"
    agg_years_list= os.listdir(cur_states)


    for year in agg_years_list:
        cur_year=cur_states+year+"/"
        agg_file_list= os.listdir(cur_year)

        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            B=json.load(data)

            for i in B["data"]["transactionData"]:
                name=i["name"]
                count=i["paymentInstruments"][0]["count"]
                amount=i["paymentInstruments"][0]["amount"]
                columns1["Transaction_type"].append(name)
                columns1["Transaction_count"].append(count)
                columns1["Transaction_amount"].append(amount)
                columns1["States"].append(state)
                columns1["Years"].append(year)
                columns1["Quarter"].append(int(file.strip(".json")))

aggre_transaction=pd.DataFrame(columns1)

aggre_transaction["States"]= aggre_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggre_transaction["States"]= aggre_transaction["States"].str.replace("-"," ")
aggre_transaction["States"]= aggre_transaction["States"].str.title()
aggre_transaction["States"]= aggre_transaction["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu","Dadra and Nagar Haveli and Daman and Diu")

# Data Frame Creation 2 : aggre_user

columns2={"States":[], "Years":[], "Quarter":[], "Brands":[], "Transaction_count":[], "Percentage":[]}

for state in agg_user_list:
    cur_states=path2+state+"/"
    agg_years_list= os.listdir(cur_states)


    for year in agg_years_list:
        cur_year=cur_states+year+"/"
        agg_file_list= os.listdir(cur_year)

        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            C=json.load(data)

            try:
                for i in C["data"]["usersByDevice"]:
                    brand=i["brand"]
                    count=i["count"]
                    percentage=i["percentage"]
                    columns2["Brands"].append(brand)
                    columns2["Transaction_count"].append(count)
                    columns2["Percentage"].append(percentage)
                    columns2["States"].append(state)
                    columns2["Years"].append(year)
                    columns2["Quarter"].append(int(file.strip(".json")))

            except:
                pass

aggre_user=pd.DataFrame(columns2)

aggre_user["States"]= aggre_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
aggre_user["States"]= aggre_user["States"].str.replace("-"," ")
aggre_user["States"]= aggre_user["States"].str.title()
aggre_user["States"]= aggre_user["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu","Dadra and Nagar Haveli and Daman and Diu")

# Data Frame Creation 3 : map_transaction

columns3={"States":[], "Years":[], "Quarter":[], "Districts":[], "Transaction_count":[], "Transaction_amount":[]}

for state in map_tran_list:
    cur_states=path3+state+"/"
    agg_years_list= os.listdir(cur_states)


    for year in agg_years_list:
        cur_year=cur_states+year+"/"
        agg_file_list= os.listdir(cur_year)

        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            E=json.load(data)

            for i in E["data"]["hoverDataList"]:
                    name=i["name"]
                    count=i["metric"][0]["count"]
                    amount=i["metric"][0]["amount"]
                    columns3["Districts"].append(name)
                    columns3["Transaction_count"].append(count)
                    columns3["Transaction_amount"].append(amount)
                    columns3["States"].append(state)
                    columns3["Years"].append(year)
                    columns3["Quarter"].append(int(file.strip(".json")))

map_transaction=pd.DataFrame(columns3)

map_transaction["States"]= map_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_transaction["States"]= map_transaction["States"].str.replace("-"," ")
map_transaction["States"]= map_transaction["States"].str.title()
map_transaction["States"]= map_transaction["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu","Dadra and Nagar Haveli and Daman and Diu")

# Data Frame Creation 4 : map_user

columns4={"States":[], "Years":[], "Quarter":[], "Districts":[], "RegisteredUsers":[], "AppOpens":[]}

for state in map_user_list:
    cur_states=path4+state+"/"
    agg_years_list= os.listdir(cur_states)


    for year in agg_years_list:
        cur_year=cur_states+year+"/"
        agg_file_list= os.listdir(cur_year)

        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            F=json.load(data)

            for i in F["data"]["hoverData"].items():
                    district=i[0]
                    registeredUsers=i[1]["registeredUsers"]
                    appOpens=i[1]["appOpens"]
                    columns4["Districts"].append(district)
                    columns4["RegisteredUsers"].append(registeredUsers)
                    columns4["AppOpens"].append(appOpens)
                    columns4["States"].append(state)
                    columns4["Years"].append(year)
                    columns4["Quarter"].append(int(file.strip(".json")))

map_user=pd.DataFrame(columns4)

map_user["States"]= map_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
map_user["States"]= map_user["States"].str.replace("-"," ")
map_user["States"]= map_user["States"].str.title()
map_user["States"]= map_user["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu","Dadra and Nagar Haveli and Daman and Diu")

# Data Frame Creation 5 : top_transaction

columns5={"States":[], "Years":[], "Quarter":[], "Pincodes":[], "Transaction_count":[], "Transaction_amount":[]}

for state in top_tran_list:
    cur_states=path5+state+"/"
    agg_years_list= os.listdir(cur_states)


    for year in agg_years_list:
        cur_year=cur_states+year+"/"
        agg_file_list= os.listdir(cur_year)

        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            H=json.load(data)

            for i in H["data"]["pincodes"]:
                    entityname=i["entityName"]
                    count=i["metric"]["count"]
                    amount=i["metric"]["amount"]
                    columns5["Pincodes"].append(entityname)
                    columns5["Transaction_count"].append(count)
                    columns5["Transaction_amount"].append(amount)
                    columns5["States"].append(state)
                    columns5["Years"].append(year)
                    columns5["Quarter"].append(int(file.strip(".json")))

top_transaction=pd.DataFrame(columns5)

top_transaction["States"]= top_transaction["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_transaction["States"]= top_transaction["States"].str.replace("-"," ")
top_transaction["States"]= top_transaction["States"].str.title()
top_transaction["States"]= top_transaction["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu","Dadra and Nagar Haveli and Daman and Diu")

# Data Frame Creation 5 : top_user

columns6={"States":[], "Years":[], "Quarter":[], "Pincodes":[], "RegisteredUsers":[],}

for state in top_tran_list:
    cur_states=path6+state+"/"
    agg_years_list= os.listdir(cur_states)


    for year in agg_years_list:
        cur_year=cur_states+year+"/"
        agg_file_list= os.listdir(cur_year)

        for file in agg_file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")

            I=json.load(data)

            for i in I["data"]["pincodes"]:
                    entityname=i["name"]
                    registeredusers=i["registeredUsers"]
                    columns6["Pincodes"].append(entityname)
                    columns6["RegisteredUsers"].append(registeredusers)
                    columns6["States"].append(state)
                    columns6["Years"].append(year)
                    columns6["Quarter"].append(int(file.strip(".json")))
top_user=pd.DataFrame(columns6)

top_user["States"]= top_user["States"].str.replace("andaman-&-nicobar-islands","Andaman & Nicobar")
top_user["States"]= top_user["States"].str.replace("-"," ")
top_user["States"]= top_user["States"].str.title()
top_user["States"]= top_user["States"].str.replace("Dadra & Nagar Haveli & Daman & Diu","Dadra and Nagar Haveli and Daman and Diu")

# STORING DATA IN SQLITE3
# Connect to the database
mydb = sqlite3.connect("PhonePe_DB")
cursor = mydb.cursor()

# aggregated_transaction_table
create_query_1 = '''CREATE TABLE if not exists aggregated_transaction(
                        States varchar(255),
                        Years int,
                        Quarter int,
                        Transaction_type varchar(255),
                        Transaction_count bigint,
                        Transaction_amount bigint)'''
cursor.execute(create_query_1)
mydb.commit()

insert_query_1 = '''INSERT INTO aggregated_transaction(
                        States, Years, Quarter, Transaction_type,
                        Transaction_count, Transaction_amount)
                    VALUES (?, ?, ?, ?, ?, ?)'''
data = aggre_transaction.values.tolist()
cursor.executemany(insert_query_1, data)
mydb.commit()

# aggregated_user_table
create_query_2 = '''CREATE TABLE if not exists aggregated_user(
                        States varchar(255),
                        Years int,
                        Quarter int,
                        Brands varchar(255),
                        Transaction_count bigint,
                        Percentage float)'''
cursor.execute(create_query_2)
mydb.commit()

insert_query_2 = '''INSERT INTO aggregated_user(
                        States, Years, Quarter, Brands,
                        Transaction_count, Percentage)
                    VALUES (?, ?, ?, ?, ?, ?)'''
data = aggre_user.values.tolist()
cursor.executemany(insert_query_2, data)
mydb.commit()

# map_transaction_table
create_query_3 = '''CREATE TABLE if not exists map_transaction(
                        States varchar(255),
                        Years int,
                        Quarter int,
                        Districts varchar(255),
                        Transaction_count bigint,
                        Transaction_amount bigint)'''
cursor.execute(create_query_3)
mydb.commit()

insert_query_3 = '''INSERT INTO map_transaction(
                        States, Years, Quarter, Districts,
                        Transaction_count, Transaction_amount)
                    VALUES (?, ?, ?, ?, ?, ?)'''
data = map_transaction.values.tolist()
cursor.executemany(insert_query_3, data)
mydb.commit()

# map_user_table
create_query_4 = '''CREATE TABLE if not exists map_user(
                        States varchar(255),
                        Years int,
                        Quarter int,
                        Districts varchar(255),
                        RegisteredUsers bigint,
                        AppOpens bigint)'''
cursor.execute(create_query_4)
mydb.commit()

insert_query_4 = '''INSERT INTO map_user(
                        States, Years, Quarter, Districts,
                        RegisteredUsers, AppOpens)
                    VALUES (?, ?, ?, ?, ?, ?)'''
data = map_user.values.tolist()
cursor.executemany(insert_query_4, data)
mydb.commit()

# top_transaction_table
create_query_5 = '''CREATE TABLE if not exists top_transaction(
                        States varchar(255),
                        Years int,
                        Quarter int,
                        Pincodes int,
                        Transaction_count bigint,
                        Transaction_amount bigint)'''
cursor.execute(create_query_5)
mydb.commit()

insert_query_5 = '''INSERT INTO top_transaction(
                        States, Years, Quarter, Pincodes,
                        Transaction_count, Transaction_amount)
                    VALUES (?, ?, ?, ?, ?, ?)'''
data = top_transaction.values.tolist()
cursor.executemany(insert_query_5, data)
mydb.commit()

# top_user_table
create_query_6 = '''CREATE TABLE if not exists top_user(
                        States varchar(255),
                        Years int,
                        Quarter int,
                        Pincodes int,
                        RegisteredUsers bigint)'''
cursor.execute(create_query_6)
mydb.commit()

insert_query_6 = '''INSERT INTO top_user(
                        States, Years, Quarter, Pincodes,
                        RegisteredUsers)
                    VALUES (?, ?, ?, ?, ?)'''
data = top_user.values.tolist()
cursor.executemany(insert_query_6, data)
mydb.commit()

# # Close the connection
# mydb.close()


# #sql connection
# # Connect to the database
# mydb = sqlite3.connect("PhonePe_DB")
# cursor = mydb.cursor()

#aggre_transaction_df
cursor.execute("SELECT * FROM aggregated_transaction")
mydb.commit()
table1=cursor.fetchall()

Aggre_transaction=pd.DataFrame(table1,columns=("States", "Years", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"))


#aggre_user_df
cursor.execute("SELECT * FROM aggregated_user")
mydb.commit()
table2=cursor.fetchall()

Aggre_user=pd.DataFrame(table2,columns=("States", "Years", "Quarter", "Brands", "Transaction_count", "Percentage"))

#map_transaction_df
cursor.execute("SELECT * FROM map_transaction")
mydb.commit()
table3=cursor.fetchall()

map_transaction=pd.DataFrame(table3,columns=("States", "Years", "Quarter", "Districts", "Transaction_count", "Transaction_amount"))

#map_user_df
cursor.execute("SELECT * FROM map_user")
mydb.commit()
table4=cursor.fetchall()

map_user=pd.DataFrame(table4,columns=("States", "Years", "Quarter", "Districts", "RegisteredUsers", "AppOpens"))


#top_transaction_df
cursor.execute("SELECT * FROM top_transaction")
mydb.commit()
table5=cursor.fetchall()

top_transaction=pd.DataFrame(table5,columns=("States", "Years", "Quarter", "Pincodes", "Transaction_count", "Transaction_amount"))


#top_user_df
cursor.execute("SELECT * FROM top_user")
mydb.commit()
table6=cursor.fetchall()

top_user=pd.DataFrame(table6,columns=("States", "Years", "Quarter", "Pincodes", "RegisteredUsers"))

# Close the connection
mydb.close()


# Transaction_year Based
def Transaction_amount_count_Y(df, year):
    # Filter data for the specified year
    tacy = df[df["Years"] == year]
    tacy.reset_index(drop=True, inplace=True)

    # Group by states and calculate sums of transaction count and amount
    tacyg = tacy.groupby("States")[["Transaction_count", "Transaction_amount"]].sum()
    tacyg.reset_index(inplace=True)

    # Plot 1: Transaction amount by state
    fig_amount = px.bar(tacyg, x="States", y="Transaction_amount", title=f"{year} TRANSACTION AMOUNT",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl, height=650, width=600)
    fig_amount.update_layout(
        title=dict(text=f"{year} TRANSACTION AMOUNT", font=dict(family="Times New Roman", color='black', size=20)),
        xaxis_title=dict(text="←<b><i>States</i></b>→", font=dict(family="Times New Roman", color='blue', size=15), standoff=10),
        yaxis_title=dict(text="←<b><i>Transaction Amount</i></b>→", font=dict(family="Times New Roman", color='blue', size=15), standoff=10),
        xaxis=dict(automargin=True, tickangle=45),
        yaxis=dict(automargin=True)
    )
    st.plotly_chart(fig_amount)

    # Plot 2: Transaction count by state
    fig_count = px.bar(tacyg, x="States", y="Transaction_count", title=f"{year} TRANSACTION COUNT",
                       color_discrete_sequence=px.colors.sequential.Bluered_r, height=650, width=600)
    fig_count.update_layout(
        title=dict(text=f"{year} TRANSACTION COUNT", font=dict(family="Times New Roman", color='black', size=20)),
        xaxis_title=dict(text="←<b><i>States</i></b>→", font=dict(family="Times New Roman", color='blue', size=15), standoff=10),
        yaxis_title=dict(text="←<b><i>Transaction Count</i></b>→", font=dict(family="Times New Roman", color='blue', size=15), standoff=10),
        xaxis=dict(automargin=True, tickangle=45),
        yaxis=dict(automargin=True)
    )
    st.plotly_chart(fig_count)

    # Load Indian states geojson data
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)

    # Choropleth plot for transaction amount
    fig_india_1 = px.choropleth(tacyg, geojson=data1, locations="States", featureidkey="properties.ST_NM",
                                color="Transaction_amount", color_continuous_scale="Rainbow",
                                range_color=(tacyg["Transaction_amount"].min(), tacyg["Transaction_amount"].max()),
                                hover_name="States", title=f"{year} TRANSACTION AMOUNT", fitbounds="locations",
                                height=600, width=600)
    fig_india_1.update_geos(visible=False)
    fig_india_1.update_layout(
        title=dict(text=f"{year} TRANSACTION AMOUNT", font=dict(family="Times New Roman", color='black', size=20)),
        coloraxis_colorbar=dict(title=dict(text="<b><i>Transaction Amount</i></b>", font=dict(family="Times New Roman", size=15))),
    )
    st.plotly_chart(fig_india_1)

    # Choropleth plot for transaction count
    fig_india_2 = px.choropleth(tacyg, geojson=data1, locations="States", featureidkey="properties.ST_NM",
                                color="Transaction_count", color_continuous_scale="Rainbow",
                                range_color=(tacyg["Transaction_count"].min(), tacyg["Transaction_count"].max()),
                                hover_name="States", title=f"{year} TRANSACTION COUNT", fitbounds="locations",
                                height=600, width=600)
    fig_india_2.update_geos(visible=False)
    fig_india_2.update_layout(
        title=dict(text=f"{year} TRANSACTION COUNT", font=dict(family="Times New Roman", color='black', size=20)),
        coloraxis_colorbar=dict(title=dict(text="<b><i>Transaction Count</i></b>", font=dict(family="Times New Roman", size=15))),
    )
    st.plotly_chart(fig_india_2)

    return tacy


def Transaction_amount_count_Y_Q(df, quarter):
    tacy = df[df["Quarter"] == quarter]
    tacy.reset_index(drop=True, inplace=True)

    tacyg = tacy.groupby("States")[["Transaction_count", "Transaction_amount"]].sum()
    tacyg.reset_index(inplace=True)

    year = tacy['Years'].min()

    # Plot 1: Transaction amount by state
    fig_amount = px.bar(tacyg, x="States", y="Transaction_amount",
                        title=f"{year} YEAR {quarter} QUARTER TRANSACTION AMOUNT",
                        color_discrete_sequence=px.colors.sequential.Aggrnyl, height=650, width=600)
    fig_amount.update_layout(
        title=dict(text=f"{year} YEAR {quarter} QUARTER TRANSACTION AMOUNT", font=dict(family="Times New Roman", size=20)),
        xaxis_title=dict(text="←<b><i>States</i></b>→", font=dict(family="Times New Roman", color='blue', size=15), standoff=10),
        yaxis_title=dict(text="←<b><i>Transaction Amount</i></b>→", font=dict(family="Times New Roman", color='blue', size=15), standoff=10),
        xaxis=dict(automargin=True, tickangle=-45),
        yaxis=dict(automargin=True, tickangle=-45)
    )
    st.plotly_chart(fig_amount)

    # Plot 2: Transaction count by state
    fig_count = px.bar(tacyg, x="States", y="Transaction_count",
                       title=f"{year} YEAR {quarter} QUARTER TRANSACTION COUNT",
                       color_discrete_sequence=px.colors.sequential.Bluered_r, height=650, width=600)
    fig_count.update_layout(
        title=dict(text=f"{year} YEAR {quarter} QUARTER TRANSACTION COUNT", font=dict(family="Times New Roman", size=20)),
        xaxis_title=dict(text="←<b><i>States</i></b>→", font=dict(family="Times New Roman", color='blue', size=15), standoff=10),
        yaxis_title=dict(text="←<b><i>Transaction Count</i></b>→", font=dict(family="Times New Roman", color='blue', size=15), standoff=10),
        xaxis=dict(automargin=True, tickangle=-45),
        yaxis=dict(automargin=True, tickangle=-45)
    )
    st.plotly_chart(fig_count)

    # Load Indian states geojson data
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data1 = json.loads(response.content)

    # Choropleth plot for transaction amount
    fig_india_1 = px.choropleth(tacyg, geojson=data1, locations="States", featureidkey="properties.ST_NM",
                                color="Transaction_amount", color_continuous_scale="Rainbow",
                                range_color=(tacyg["Transaction_amount"].min(), tacyg["Transaction_amount"].max()),
                                hover_name="States", title=f"{year} YEAR {quarter} QUARTER TRANSACTION AMOUNT", fitbounds="locations",
                                height=600, width=600)
    fig_india_1.update_layout(font=dict(family="Times New Roman"))
    fig_india_1.update_geos(visible=False)
    st.plotly_chart(fig_india_1)

    # Choropleth plot for transaction count
    fig_india_2 = px.choropleth(tacyg, geojson=data1, locations="States", featureidkey="properties.ST_NM",
                                color="Transaction_count", color_continuous_scale="Rainbow",
                                range_color=(tacyg["Transaction_count"].min(), tacyg["Transaction_count"].max()),
                                hover_name="States", title=f"{year} YEAR {quarter} QUARTER TRANSACTION COUNT", fitbounds="locations",
                                height=600, width=600)
    fig_india_2.update_layout(font=dict(family="Times New Roman"))
    fig_india_2.update_geos(visible=False)
    st.plotly_chart(fig_india_2)

    return tacy

# Transaction_Type Based
def Aggre_Tran_Transaction_type(df, state):
    # Filter data for the specified state
    tacy = df[df["States"] == state]
    tacy.reset_index(drop=True, inplace=True)

    # Group by transaction type and calculate sums of transaction count and amount
    tacyg = tacy.groupby("Transaction_type")[["Transaction_count", "Transaction_amount"]].sum()
    tacyg.reset_index(inplace=True)

    # Plot 1: Pie chart for transaction amount by transaction type
    fig_pie_1 = px.pie(data_frame=tacyg, names="Transaction_type", values="Transaction_amount",
                       title=f"{state.upper()} TRANSACTION AMOUNT", hole=0, width=600, height=800)
    fig_pie_1.update_traces(marker=dict(colors=px.colors.qualitative.Pastel), textinfo='percent+label')
    fig_pie_1.update_layout(
        title=dict(text=f"{state.upper()} TRANSACTION AMOUNT", font=dict(family="Times New Roman", color='black', size=20))
    )
    # fig_pie_1.show()
    st.plotly_chart(fig_pie_1)

    # Plot 2: Pie chart for transaction count by transaction type
    fig_pie_2 = px.pie(data_frame=tacyg, names="Transaction_type", values="Transaction_count",
                       title=f"{state.upper()} TRANSACTION COUNT", hole=0, width=600, height=800)
    fig_pie_2.update_traces(marker=dict(colors=px.colors.qualitative.Pastel), textinfo='percent+label')
    fig_pie_2.update_layout(
        title=dict(text=f"{state.upper()} TRANSACTION COUNT", font=dict(family="Times New Roman", color='black', size=20))
    )
    # fig_pie_2.show()
    st.plotly_chart(fig_pie_2)

#Aggre_user_analysis_1
def Aggre_user_plot_1(df, year):
    # Filter data for the specified year
    aguy = df[df["Years"] == year]
    aguy.reset_index(drop=True, inplace=True)

    # Group by brands and calculate sums of transaction count
    aguyg = pd.DataFrame(aguy.groupby("Brands")["Transaction_count"].sum())
    aguyg.reset_index(inplace=True)

    # Plot: Bar chart for transaction count by brands
    fig_bar_1 = px.bar(aguyg, x="Brands", y="Transaction_count", title=f"<i>{year} TRANSACTION COUNT BY BRANDS</i>",
                       width=600, color_discrete_sequence=px.colors.sequential.haline_r, hover_name="Brands")

    fig_bar_1.update_layout(
        title=dict(text=f"<b><i>{year} TRANSACTION COUNT BY BRANDS</i></b>", font=dict(family="Times New Roman", color='black', size=20)),
        xaxis_title=dict(text="←<b><i>Brands</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        yaxis_title=dict(text="←<b><i>Transaction_count</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        xaxis=dict(automargin=True, tickangle=45),
        yaxis=dict(automargin=True),
        margin=dict(l=50, r=50, t=80, b=50))

    # fig_bar_1.show()
    st.plotly_chart(fig_bar_1)

    return aguy

#Aggre_user_analysis_2
def aggre_user_plot2(df, quarter):
    # Filter data for the specified quarter
    aguyq = df[df["Quarter"] == quarter]
    aguyq.reset_index(drop=True, inplace=True)

    # Group by brands and calculate sums of transaction count
    aguyqg = pd.DataFrame(aguyq.groupby("Brands")["Transaction_count"].sum())
    aguyqg.reset_index(inplace=True)

    # Plot: Bar chart for transaction count by brands
    fig_bar_1 = px.bar(aguyqg, x="Brands", y="Transaction_count", title=f"<b><i>{quarter} QUARTER TRANSACTION COUNT BY BRANDS</i></b>",
                       width=600, color_discrete_sequence=px.colors.sequential.haline_r, hover_name="Brands")

    fig_bar_1.update_layout(
        xaxis_title=dict(text="<i>←<b>Brands</b>→</i>", font=dict(family="Times New Roman", color='blue', size=15)),
        yaxis_title=dict(text="<i>←<b>Transaction_count</b>→</i>", font=dict(family="Times New Roman", color='blue', size=15)),
        xaxis=dict(automargin=True, tickangle=45),
        yaxis=dict(automargin=True),
        margin=dict(l=50, r=50, t=80, b=50)
    )

    # fig_bar_1.show()
    st.plotly_chart(fig_bar_1)

    return aguyq

#Aggre_user_analysis_3
def Aggre_user_plot_3(df, state):
    # Filter data for the specified state
    auyqs = df[df["States"] == state]
    auyqs.reset_index(drop=True, inplace=True)

    # Plot: Line chart for transaction count by brands
    fig_line_1 = px.line(auyqs, x="Brands", y="Transaction_count", hover_data="Percentage",
                         title=f"<b>{state.upper()} BRANDS, TRANSACTION COUNT, PERCENTAGE</b>", width=1000, markers=True)

    fig_line_1.update_layout(
        xaxis_title=dict(text="<i>←<b>Brands</b>→</i>", font=dict(family="Times New Roman", color='blue', size=15)),
        yaxis_title=dict(text="<i>←<b>Transaction_count</b>→</i>", font=dict(family="Times New Roman", color='blue', size=15)),
        xaxis=dict(automargin=True, tickangle=45),
        yaxis=dict(automargin=True),
        margin=dict(l=50, r=50, t=80, b=50)
    )

    # fig_line_1.show()
    st.plotly_chart(fig_line_1)


#Map_Transation_Districts
def map_tran_Districts(df, state):
    # Filter data for the specified state
    tacy = df[df["States"] == state]
    tacy.reset_index(drop=True, inplace=True)

    # Group by districts and calculate sums of transaction count and amount
    tacyg = tacy.groupby("Districts")[["Transaction_count", "Transaction_amount"]].sum()
    tacyg.reset_index(inplace=True)

    # Plot 1: Horizontal bar chart for transaction amount by district
    fig_bar_1 = px.bar(tacyg, x="Transaction_amount", y="Districts", orientation="h",
                       title=f"<b>{state.upper()} DISTRICT AND TRANSACTION AMOUNT</b>",
                       color_discrete_sequence=px.colors.sequential.Mint_r)

    fig_bar_1.update_layout(
        xaxis_title=dict(text="<i>←<b>Transaction Amount</b>→</i>", font=dict(family="Times New Roman", color='blue', size=15)),
        yaxis_title=dict(text="<i>←<b>Districts</b>→</i>", font=dict(family="Times New Roman", color='blue', size=15)),
        xaxis=dict(automargin=True),
        yaxis=dict(automargin=True),
        margin=dict(l=150, r=50, t=80, b=50)
    )

    # fig_bar_1.show()
    st.plotly_chart(fig_bar_1)

    # Plot 2: Horizontal bar chart for transaction count by district
    fig_bar_2 = px.bar(tacyg, x="Transaction_count", y="Districts", orientation="h",
                       title=f"<b>{state.upper()} DISTRICT AND TRANSACTION COUNT</b>",
                       color_discrete_sequence=px.colors.sequential.Bluered_r)

    fig_bar_2.update_layout(
        xaxis_title=dict(text="<i>←<b>Transaction Count</b>→</i>", font=dict(family="Times New Roman", color='blue', size=15)),
        yaxis_title=dict(text="<i>←<b>Districts</b>→</i>", font=dict(family="Times New Roman", color='blue', size=15)),
        xaxis=dict(automargin=True),
        yaxis=dict(automargin=True),
        margin=dict(l=150, r=50, t=80, b=50)
    )

    # fig_bar_2.show()
    st.plotly_chart(fig_bar_2)

# map_user_plot1
def map_user_plot1(df, year):
    # Filter data for the specified year
    muy = df[df["Years"] == year]
    muy.reset_index(drop=True, inplace=True)

    # Group by states and calculate sums of registered users and app opens
    muyg = muy.groupby("States")[["RegisteredUsers", "AppOpens"]].sum()
    muyg.reset_index(inplace=True)

    # Plot: Line chart for registered users and app opens by state
    fig_line_1 = px.line(muyg, x="States", y=["RegisteredUsers", "AppOpens"],
                         title=f"{year} REGISTERED USERS AND APP OPENS",
                         width=1000, markers=True)

    fig_line_1.update_layout(
        title=dict(text=f"<b><i>{year} REGISTERED USERS AND APP OPENS</i></b>",
                   font=dict(family="Times New Roman", color='black', size=20)),
        xaxis_title=dict(text="←<b><i>States</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        yaxis_title=dict(text="←<b><i>Count</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        xaxis=dict(automargin=True, tickangle=45),
        yaxis=dict(automargin=True)
    )

    # fig_line_1.show()
    st.plotly_chart(fig_line_1)

    return muy

# map_user_plot2
def map_user_plot2(df, quarter):
    # Filter data for the specified quarter
    muyq = df[df["Quarter"] == quarter]
    muyq.reset_index(drop=True, inplace=True)

    # Group by states and calculate sums of registered users and app opens
    muyqg = muyq.groupby("States")[["RegisteredUsers", "AppOpens"]].sum()
    muyqg.reset_index(inplace=True)

    # Plot: Line chart for registered users and app opens by state for the specified year and quarter
    fig_line_1 = px.line(muyqg, x="States", y=["RegisteredUsers", "AppOpens"],
                         title=f"{muyq['Years'].min()} YEAR {quarter} QUARTER REGISTERED USERS AND APP OPENS",
                         width=1000, height=500, markers=True, color_discrete_sequence=px.colors.sequential.Rainbow_r)

    fig_line_1.update_layout(
        title=dict(text=f"<b><i>{muyq['Years'].min()} YEAR {quarter} QUARTER REGISTERED USERS AND APP OPENS</i></b>",
                   font=dict(family="Times New Roman", color='black', size=20)),
        xaxis_title=dict(text="←<b><i>States</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        yaxis_title=dict(text="←<b><i>Count</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        xaxis=dict(automargin=True, tickangle=45),
        yaxis=dict(automargin=True)
    )

    # fig_line_1.show()
    st.plotly_chart(fig_line_1)

    return muyq

# map_user_plot3
def map_user_plot3(df, state):
    # Filter data for the specified state
    muyqs = df[df["States"] == state]
    muyqs.reset_index(drop=True, inplace=True)

    # Plot 1: Horizontal bar chart for registered users by district
    fig_map_user_bar_1 = px.bar(muyqs, x="RegisteredUsers", y="Districts", orientation="h",
                                title="REGISTERED USERS", height=800, color_discrete_sequence=px.colors.sequential.Rainbow_r)

    fig_map_user_bar_1.update_layout(
        title=dict(text="<b><i>REGISTERED USERS</i></b>", font=dict(family="Times New Roman", color='black', size=20)),
        xaxis_title=dict(text="←<b><i>Registered Users</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        yaxis_title=dict(text="←<b><i>Districts</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        xaxis=dict(automargin=True),
        yaxis=dict(automargin=True)
    )

    # fig_map_user_bar_1.show()
    st.plotly_chart(fig_map_user_bar_1)

    # Plot 2: Horizontal bar chart for app opens by district
    fig_map_user_bar_2 = px.bar(muyqs, x="AppOpens", y="Districts", orientation="h",
                                title="APP OPENS", height=800, color_discrete_sequence=px.colors.sequential.Rainbow)

    fig_map_user_bar_2.update_layout(
        title=dict(text="<b><i>APP OPENS</i></b>", font=dict(family="Times New Roman", color='black', size=20)),
        xaxis_title=dict(text="←<b><i>App Opens</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        yaxis_title=dict(text="←<b><i>Districts</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        xaxis=dict(automargin=True),
        yaxis=dict(automargin=True)
    )

    # fig_map_user_bar_2.show()
    st.plotly_chart(fig_map_user_bar_2)


# Top_Transaction_plot_1
def Top_Transaction_plot_1(df, state):
    # Filter data for the specified state
    tty = df[df["States"] == state]
    tty.reset_index(drop=True, inplace=True)

    # Group by pincodes and calculate sums of transaction count and amount
    ttyg = tty.groupby("Pincodes")[["Transaction_count", "Transaction_amount"]].sum()
    ttyg.reset_index(inplace=True)

    # Plot 1: Bar chart for transaction amount by quarter and pincodes
    fig_top_tran_bar_1 = px.bar(tty, x="Quarter", y="Transaction_amount", hover_data="Pincodes",
                                title="TRANSACTION AMOUNT", height=800, color_discrete_sequence=px.colors.sequential.Rainbow_r)

    fig_top_tran_bar_1.update_layout(
        title=dict(text="<b><i>TRANSACTION AMOUNT</i></b>", font=dict(family="Times New Roman", color='black', size=20)),
        xaxis_title=dict(text="←<b><i>Quarter</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        yaxis_title=dict(text="←<b><i>Transaction Amount</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        xaxis=dict(automargin=True),
        yaxis=dict(automargin=True)
    )

    # fig_top_tran_bar_1.show()
    st.plotly_chart(fig_top_tran_bar_1)

    # Plot 2: Bar chart for transaction count by quarter and pincodes
    fig_top_tran_bar_2 = px.bar(tty, x="Quarter", y="Transaction_count", hover_data="Pincodes",
                                title="TRANSACTION COUNT", height=800, color_discrete_sequence=px.colors.sequential.Rainbow)

    fig_top_tran_bar_2.update_layout(
        title=dict(text="<b><i>TRANSACTION COUNT</i></b>", font=dict(family="Times New Roman", color='black', size=20)),
        xaxis_title=dict(text="←<b><i>Quarter</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        yaxis_title=dict(text="←<b><i>Transaction Count</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        xaxis=dict(automargin=True),
        yaxis=dict(automargin=True)
    )

    # fig_top_tran_bar_2.show()
    st.plotly_chart(fig_top_tran_bar_2)

# top_user_plot_1
def top_user_plot_1(df, year):
    # Filter data for the specified year
    tuy = df[df["Years"] == year]
    tuy.reset_index(drop=True, inplace=True)

    # Group by states and quarter, and calculate sum of registered users
    tuyg = pd.DataFrame(tuy.groupby(["States", "Quarter"])["RegisteredUsers"].sum())
    tuyg.reset_index(inplace=True)

    # Combine multiple color sequences to get enough unique colors
    color_sequences = px.colors.qualitative.Plotly + px.colors.qualitative.Bold + px.colors.qualitative.Safe
    unique_states = tuyg["States"].unique()

    # Ensure the color map covers all unique states
    color_map = {state: color_sequences[i % len(color_sequences)] for i, state in enumerate(unique_states)}

    # Plot: Bar chart for registered users by states, colored by states
    fig_top_plot_1 = px.bar(tuyg, x="States", y="RegisteredUsers", color="States", width=1000, height=800,
                            color_discrete_map=color_map, hover_name="States",
                            title=f"{year} REGISTERED USERS")

    fig_top_plot_1.update_layout(
        title=dict(text=f"<b><i>{year} REGISTERED USERS</i></b>", font=dict(family="Times New Roman", color='black', size=20)),
        xaxis_title=dict(text="←<b><i>States</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        yaxis_title=dict(text="←<b><i>Registered Users</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        xaxis=dict(automargin=True),
        yaxis=dict(automargin=True)
    )

    # fig_top_plot_1.show()
    st.plotly_chart(fig_top_plot_1)

    return tuy

# top_user_plot_2
def top_user_plot_2(df, state):
    # Filter data for the specified state
    tuys = df[df["States"] == state]
    tuys.reset_index(drop=True, inplace=True)

    # Plot: Bar chart for registered users by quarter, colored by registered users count
    fig_top_plot_2 = px.bar(tuys, x="Quarter", y="RegisteredUsers", title=f"{state.upper()} REGISTERED USERS, PINCODES, QUARTER",
                            width=1000, height=800, color="RegisteredUsers", hover_data="Pincodes",
                            color_continuous_scale=px.colors.sequential.Burgyl)

    fig_top_plot_2.update_layout(
        title=dict(text=f"<b><i>{state.upper()} REGISTERED USERS, PINCODES, QUARTER</i></b>",
                   font=dict(family="Times New Roman", color='black', size=20)),
        xaxis_title=dict(text="←<b><i>Quarter</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        yaxis_title=dict(text="←<b><i>Registered Users</i></b>→", font=dict(family="Times New Roman", color='blue', size=15)),
        xaxis=dict(automargin=True),
        yaxis=dict(automargin=True),
        legend=dict(font=dict(family="Times New Roman", color='black', size=12)),
        coloraxis_colorbar=dict(title=dict(font=dict(family="Times New Roman", color='blue', size=15)))
    )

    # fig_top_plot_2.show()
    st.plotly_chart(fig_top_plot_2)


#sql connection
def top_chart_transaction_amount(table_name):
    mydb = sqlite3.connect("PhonePe_DB")
    cursor = mydb.cursor()
    # plot_1
    query1= f'''SELECT states, SUM(transaction_amount) AS transaction_amount
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_amount DESC
                LIMIT 10;'''

    cursor.execute(query1)
    table= cursor.fetchall()
    mydb.commit()

    df_1= pd.DataFrame(table, columns=("states", "transaction_amount"))

    col1,col2= st.columns(2)
    with col1:

        fig_amount= px.bar(df_1,x="states", y="transaction_amount", title="TOP 10 OF TRANSACTION AMOUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_amount)

    # plot_2
    query2= f'''SELECT states, SUM(transaction_amount) AS transaction_amount
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_amount
                LIMIT 10;'''

    cursor.execute(query2)
    table2= cursor.fetchall()
    mydb.commit()

    df_2= pd.DataFrame(table2, columns=("states", "transaction_amount"))

    with col2:

        fig_amount_2= px.bar(df_2,x="states", y="transaction_amount", title="LAST 10 OF TRANSACTION AMOUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height=650, width=600)
        st.plotly_chart(fig_amount_2)


    # plot_3
    query3= f'''SELECT states, AVG(transaction_amount) AS transaction_amount
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_amount ;'''

    cursor.execute(query3)
    table3= cursor.fetchall()
    mydb.commit()

    df_3= pd.DataFrame(table3, columns=("states", "transaction_amount"))

    fig_amount_3= px.bar(df_3,y="states", x="transaction_amount", title="AVG 10 0F TRANSACTION AMOUNT", hover_name= "states",orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height=800, width=1000)
    st.plotly_chart(fig_amount_3)


#sql connection
def top_chart_transaction_count(table_name):
    mydb = sqlite3.connect("PhonePe_DB")
    cursor = mydb.cursor()
    # plot_1
    query1= f'''SELECT states, SUM(transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_count DESC
                LIMIT 10;'''

    cursor.execute(query1)
    table= cursor.fetchall()
    mydb.commit()

    df_1= pd.DataFrame(table, columns=("states", "transaction_count"))

    col1,col2= st.columns(2)
    with col1:

        fig_amount= px.bar(df_1,x="states", y="transaction_count", title="TOP 10 OF TRANSACTION COUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_amount)

    # plot_2
    query2= f'''SELECT states, SUM(transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_count
                LIMIT 10;'''

    cursor.execute(query2)
    table2= cursor.fetchall()
    mydb.commit()

    df_2= pd.DataFrame(table2, columns=("states", "transaction_count"))

    with col2:
        fig_amount_2= px.bar(df_2,x="states", y="transaction_count", title="LAST 10 OF TRANSACTION COUNT", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height=650, width=600)
        st.plotly_chart(fig_amount_2)


    # plot_3
    query3= f'''SELECT states, AVG(transaction_count) AS transaction_count
                FROM {table_name}
                GROUP BY states
                ORDER BY transaction_count ;'''

    cursor.execute(query3)
    table3= cursor.fetchall()
    mydb.commit()

    df_3= pd.DataFrame(table3, columns=("states", "transaction_count"))

    fig_amount_3= px.bar(df_3,y="states", x="transaction_count", title="AVG 10 OF TRANSACTION COUNT", hover_name= "states",orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height=800, width=1000)
    st.plotly_chart(fig_amount_3)


#sql connection
def top_chart_registered_user(table_name, state):
    mydb = sqlite3.connect("PhonePe_DB")
    cursor = mydb.cursor()
    # plot_1
    query1= f'''SELECT districts, SUM(registeredusers) AS registeredusers
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY registeredusers DESC
                LIMIT 10;'''

    cursor.execute(query1)
    table= cursor.fetchall()
    mydb.commit()

    df_1= pd.DataFrame(table, columns=("districts", "registeredusers"))
    col1,col2= st.columns(2)
    with col1:
        fig_amount= px.bar(df_1,x="districts", y="registeredusers", title=" TOP 10 OF REGISTERED USERS", hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_amount)

    # plot_2
    query2= f'''SELECT districts, SUM(registeredusers) AS registeredusers
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY registeredusers
                LIMIT 10;'''

    cursor.execute(query2)
    table2= cursor.fetchall()
    mydb.commit()

    df_2= pd.DataFrame(table2, columns=("districts", "registeredusers"))
    with col2:
        fig_amount_2= px.bar(df_2,x="districts", y="registeredusers", title="LAST 10 OF REGISTERED USER", hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height=650, width=600)
        st.plotly_chart(fig_amount_2)


    # plot_3
    query3= f'''SELECT districts, AVG(registeredusers) AS registeredusers
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY registeredusers;'''

    cursor.execute(query3)
    table3= cursor.fetchall()
    mydb.commit()

    df_3= pd.DataFrame(table3, columns=("districts", "registeredusers"))

    fig_amount_3= px.bar(df_3,y="districts", x="registeredusers", title="AVG OF REGISTERED USER", hover_name= "districts",orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height=650, width=600)
    st.plotly_chart(fig_amount_3)


#sql connection
def top_chart_appopens(table_name, state):
    mydb = sqlite3.connect("PhonePe_DB")
    cursor = mydb.cursor()
    # plot_1
    query1= f'''SELECT districts, SUM(appopens) AS appopens
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY appopens DESC
                LIMIT 10;'''

    cursor.execute(query1)
    table= cursor.fetchall()
    mydb.commit()

    df_1= pd.DataFrame(table, columns=("districts", "appopens"))

    col1,col2= st.columns(2)
    with col1:

        fig_amount= px.bar(df_1,x="districts", y="appopens", title=" TOP 10 OF APPOPENS ", hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_amount)

    # plot_2
    query2= f'''SELECT districts, SUM(appopens) AS appopens
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY appopens
                LIMIT 10;'''

    cursor.execute(query2)
    table2= cursor.fetchall()
    mydb.commit()

    df_2= pd.DataFrame(table2, columns=("districts", "appopens"))

    with col2:
        fig_amount_2= px.bar(df_2,x="districts", y="appopens", title="LAST 10 OF APPOPENS ", hover_name= "districts",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height=650, width=600)
        st.plotly_chart(fig_amount_2)


    # plot_3
    query3= f'''SELECT districts, AVG(appopens) AS appopens
                FROM {table_name}
                WHERE states= '{state}'
                GROUP BY districts
                ORDER BY appopens;'''

    cursor.execute(query3)
    table3= cursor.fetchall()
    mydb.commit()

    df_3= pd.DataFrame(table3, columns=("districts", "appopens"))

    fig_amount_3= px.bar(df_3,y="districts", x="appopens", title="AVG OF APPOPENS", hover_name= "districts",orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height=650, width=600)
    st.plotly_chart(fig_amount_3)


#sql connection
def top_chart_registered_users(table_name):
    mydb = sqlite3.connect("PhonePe_DB")
    cursor = mydb.cursor()
    # plot_1
    query1= f'''SELECT states,SUM(registeredusers) AS registeredusers
                FROM {table_name}
                GROUP BY states
                ORDER BY registeredusers DESC
                LIMIT 10;'''

    cursor.execute(query1)
    table= cursor.fetchall()
    mydb.commit()

    df_1= pd.DataFrame(table, columns=("states", "registeredusers"))

    col1,col2= st.columns(2)
    with col1:

        fig_amount= px.bar(df_1,x="states", y="registeredusers", title=" TOP 10 OF REGISTERED USERS", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl, height=650, width=600)
        st.plotly_chart(fig_amount)

    # plot_2
    query2= f'''SELECT states,SUM(registeredusers) AS registeredusers
                FROM {table_name}
                GROUP BY states
                ORDER BY registeredusers
                LIMIT 10;'''

    cursor.execute(query2)
    table2= cursor.fetchall()
    mydb.commit()

    df_2= pd.DataFrame(table2, columns=("states", "registeredusers"))

    with col2:
        fig_amount_2= px.bar(df_2,x="states", y="registeredusers", title="LAST 10 OF REGISTERED USER", hover_name= "states",
                            color_discrete_sequence=px.colors.sequential.Aggrnyl_r, height=650, width=600)
        st.plotly_chart(fig_amount_2)


    # plot_3
    query3= f'''SELECT states,AVG(registeredusers) AS registeredusers
                FROM {table_name}
                GROUP BY states
                ORDER BY registeredusers;'''

    cursor.execute(query3)
    table3= cursor.fetchall()
    mydb.commit()

    df_3= pd.DataFrame(table3, columns=("states", "registeredusers"))

    fig_amount_3= px.bar(df_3,y="states", x="registeredusers", title="AVG OF REGISTERED USER", hover_name= "states",orientation= "h",
                        color_discrete_sequence=px.colors.sequential.Bluered_r, height=650, width=600)
    st.plotly_chart(fig_amount_3)







#streamlit part

st.set_page_config(layout= "wide")
st.title("PHONEPE DATA VISUALIZATION AND EXPLORATION")

with st.sidebar:

    select=option_menu("Main Menu",["HOME", "DATA ANALYSIS", "TOP CHARTS"])

if select == "HOME":


    col1,col2= st.columns(2)

    with col1:
        st.header("PHONEPE")
        st.subheader("INDIA'S BEST TRNSACTION APP")
        st.markdown("Phonepe is an Indian payments and financial technology company")
        st.write("****FEATUERS****")
        st.write("****Credit & Debit card linking****")
        st.write("****Bank Balance check****")
        st.write("****Money Storage****")
        st.write("****PIN Authorization****")
        st.download_button("DOWNLOAD THE APP NOW", "https://www.phonepe.com/app-download/")

    with col2:
        st.image(Image.open(r"/content/Phonepe-images.jpg"), width= 400)

    col3,col4= st.columns(2)

    with col3:
        st.image(Image.open(r"/content/Phonepe image 2.jpg"), width= 400)

    with col4:
        st.write("****Easy Transaction****")
        st.write("****One App For All Your Payments****")
        st.write("****Your Bank Acount Is All You Need****")
        st.write("****Multiple Payments Moodes****")
        st.write("****Phonepe Merchants****")
        st.write("****Multiple Ways To Pay****")
        st.write("****1.Dierect Ways To Pay****")
        st.write("****2.QR Code****")
        st.write("****Earn Great Rewards****")

    col5,col6= st.columns(2)

    with col5:
        st.markdown("")
        st.markdown("")
        st.markdown("")
        st.markdown("")
        st.markdown("")
        st.markdown("")
        st.markdown("")
        st.markdown("")
        st.markdown("")
        st.write("****No Wallet Top-up Required****")
        st.write("****Pay Directly From Any Bank To Any A/C****")
        st.write("****Instantly & Free****")

    with col6:
        st.image(Image.open(r"/content/Phonepe-image 3.webp"), width= 400)

elif select == "DATA ANALYSIS":

    tab1, tab2, tab3, = st.tabs(["Aggregated Analysis", "Map Analysis", "Top Analysis"])

    with tab1:
        method = st.radio("Select The Method", ["Transaction Analysis", "User Analysis"])

        if method == "Transaction Analysis":

            col1,col2= st.columns(2)
            with col1:

                years= st.slider("Select The Year",Aggre_transaction["Years"].min(), Aggre_transaction["Years"].max(), Aggre_transaction["Years"].min())
            tac_Y= Transaction_amount_count_Y(Aggre_transaction, years)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State", tac_Y["States"].unique())

            Aggre_Tran_Transaction_type(tac_Y, states)

            col1,col2= st.columns(2)
            with col1:

                quarters= st.slider("Select The Quarter",tac_Y["Quarter"].min(), tac_Y["Quarter"].max(), tac_Y["Quarter"].min())
            tac_y_Q= Transaction_amount_count_Y_Q(tac_Y, quarters)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State_Type", tac_y_Q["States"].unique())

            Aggre_Tran_Transaction_type(tac_y_Q, states)



        elif method == "User Analysis":

            col1,col2= st.columns(2)
            with col1:

                years= st.slider("Select The Year",Aggre_user["Years"].min(), Aggre_user["Years"].max(), Aggre_user["Years"].min())
            Aggre_user_Y= Aggre_user_plot_1(Aggre_user, years)

            col1,col2= st.columns(2)
            with col1:

                quarters= st.slider("Select The Quarter",Aggre_user_Y["Quarter"].min(), Aggre_user_Y["Quarter"].max(), Aggre_user_Y["Quarter"].min())
            Aggre_user_Y_Q= aggre_user_plot2(Aggre_user_Y, quarters)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State", Aggre_user_Y_Q["States"].unique())

            Aggre_user_plot_3(Aggre_user_Y_Q, states)

    with tab2:
        method2 = st.radio("Select The Method", ["Map Transaction", "Map User"])
        if method2 == "Map Transaction":
            col1,col2= st.columns(2)
            with col1:

                years= st.slider("Select The Year Map Transaction",map_transaction["Years"].min(), map_transaction["Years"].max(), map_transaction["Years"].min())
            map_tac_y= Transaction_amount_count_Y(map_transaction, years)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State Map Transaction", map_tac_y["States"].unique())

            map_tran_Districts(map_tac_y, states)

            col1,col2= st.columns(2)
            with col1:

                quarters= st.slider("Select The Quarter Map Transaction",map_tac_y["Quarter"].min(), map_tac_y["Quarter"].max(), map_tac_y["Quarter"].min())
            map_tac_y_Q= Transaction_amount_count_Y_Q(map_tac_y, quarters)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State Type Map Transaction", map_tac_y_Q["States"].unique())

            map_tran_Districts(map_tac_y_Q, states)

        elif method2 == "Map User":
            col1,col2= st.columns(2)
            with col1:

                years= st.slider("Select The Year Map User",map_user["Years"].min(), map_user["Years"].max(), map_user["Years"].min())
            map_user_y= map_user_plot1(map_user, years)

            col1,col2= st.columns(2)
            with col1:

                quarters= st.slider("Select The Quarter Map User",map_user_y["Quarter"].min(), map_user_y["Quarter"].max(), map_user_y["Quarter"].min())
            map_user_y_Q= map_user_plot2(map_user_y, quarters)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State Type Map User", map_user_y_Q["States"].unique())

            map_user_plot3(map_user_y_Q, states)

    with tab3:
        method3 = st.radio("Select The Method", ["Top Transaction", "Top User"])
        if method3 == "Top Transaction":

            col1,col2= st.columns(2)
            with col1:

                years= st.slider("Select The Year Top Transaction",top_transaction["Years"].min(), top_transaction["Years"].max(), top_transaction["Years"].min())
            top_tac_y= Transaction_amount_count_Y(top_transaction, years)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State Type Top Transction", top_tac_y["States"].unique())

            Top_Transaction_plot_1(top_tac_y, states)

            col1,col2= st.columns(2)
            with col1:

                quarters= st.slider("Select The Quarter Top Transaction",top_tac_y["Quarter"].min(), top_tac_y["Quarter"].max(), top_tac_y["Quarter"].min())
            top_tac_y_Q= Transaction_amount_count_Y_Q(top_tac_y, quarters)

        elif method3 == "Top User":
            col1,col2= st.columns(2)
            with col1:

                years= st.slider("Select The Year Top User",top_user["Years"].min(), top_user["Years"].max(), top_user["Years"].min())
            top_user_tac_y= top_user_plot_1(top_user, years)

            col1,col2= st.columns(2)
            with col1:
                states= st.selectbox("Select The State Type Top User", top_user_tac_y["States"].unique())

            top_user_plot_2(top_user_tac_y, states)

elif select == "TOP CHARTS":
    question= st.selectbox("select the questions", ["1. Transaction Amount and count of Aggregated Transation",
                                                    "2. Transaction Amount and count of Map Transaction",
                                                    "3. Transaction Amount and count of Top Transaction",
                                                    "4. Transaction count of Aggregated User",
                                                    "5. Registered users of Map User",
                                                    "6. App opens of Map User",
                                                    "7. Registered users of Top User",
                                                    ])

    if question == "1. Transaction Amount and count of Aggregated Transation":

        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("aggregated_transaction")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("aggregated_transaction")


    elif question == "2. Transaction Amount and count of Map Transaction":

        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("map_transaction")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("map_transaction")

    elif question == "3. Transaction Amount and count of Top Transaction":

        st.subheader("TRANSACTION AMOUNT")
        top_chart_transaction_amount("top_transaction")

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("top_transaction")

    elif question == "4. Transaction count of Aggregated User":

        st.subheader("TRANSACTION COUNT")
        top_chart_transaction_count("aggregated_user")


    elif question == "5. Registered users of Map User":

        states= st.selectbox("Select The State", map_user["States"].unique())
        st.subheader("REGISTERED USER")
        top_chart_registered_user("map_user", states)


    elif question == "6. App opens of Map User":

        states= st.selectbox("Select The State", map_user["States"].unique())
        st.subheader("APPOPENS")
        top_chart_appopens("map_user", states)

    elif question == "7. Registered users of Top User":

        st.subheader("REGISTERED USERS")
        top_chart_registered_users("top_user")
