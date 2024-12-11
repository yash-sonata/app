# ******************************************************
# Data Loader (File -> MySQL Table)
# ******************************************************

# ***********************IMPORTANT**********************
#
# Before running this code, create a table in MySQL:
# Create a CSV or Excel file with the same format and add dummy data to it.
#
# Helpful Commands:
# -- Create the table with columns: id, name, email, phone number
# CREATE TABLE contacts (
#     id INT PRIMARY KEY,
#     name VARCHAR(100),
#     email VARCHAR(100),
#     phone_number VARCHAR(20));
# INSERT INTO contacts (id, name, email, phone_number) VALUES
# (1, 'John Doe', 'johndoe@example.com', '123-456-7890'),
# (2, 'Jane Smith', 'janesmith@example.com', '987-654-3210'),
# (3, 'Alice Johnson', 'alicej@example.com', '555-123-4567'),
# (4, 'Bob Brown', 'bobbrown@example.com', '444-987-6543');
# 
# SELECT * FROM contacts;
# TRUNCATE TABLE contacts;
# DROP TABLE contacts;
# ******************************************************
# TO RUN THIS CODE:  streamlit run native-streamlit.py
# ******************************************************

# Importing Libraries
import streamlit as st
import pandas as pd
import mysql.connector
from streamlit import session_state as ss
#import time
from datetime import datetime, timedelta
import re
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader


# Data Preview
def data_preview(file):
    # Read the file into a pandas DataFrame
    if file.name.endswith('.csv'):
        df = pd.read_csv(file)
    elif file.name.endswith('.xlsx'):
        df = pd.read_excel(file)
    else:
        st.error("Unsupported file format. Please upload a CSV or Excel file.")
        return
    st.caption("Data Preview")
    #ss.edited = st.data_editor(df, use_container_width=True, num_rows="dynamic", key='ed')
    ss.edited = st.dataframe(df, use_container_width=True)
    return df

# Function to load data into MySQL table
def load_data_to_mysql(df, table_name, db_config):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    
    # Read the file into a pandas DataFrame
    if file.name.endswith('.csv'):
        df = pd.read_csv(file)
    elif file.name.endswith('.xlsx'):
        df = pd.read_excel(file)
    else:
        st.error("Unsupported file format. Please upload a CSV or Excel file.")
        return

    # Insert data into MySQL table
    for i, row in df.iterrows():
        sql = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()
    st.success("Data loaded successfully into MySQL table.")

# Show Tables
def show_loaded_table(table_name, db_config):
    conn = mysql.connector.connect(**db_config)
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    # Just for demo **********************************************
    #st.write(df)
    if table_name in ['customers', 'app3_table']:
        return df
    # Just for demo **********************************************
    df = df[df['ACTIVE'] == 'Y']
    df.reset_index(inplace=True, drop=True)
    #return df.loc[:, ~df.columns.isin(['ACTIVE','lst_upd_ts', 'UPDATE_USERID','eff_from_dt'])]
    columns_to_remove = ['ACTIVE', 'lst_upd_ts', 'UPDATE_USERID', 'eff_from_dt']
    if all(column in df.columns for column in columns_to_remove):
        return df.loc[:, ~df.columns.isin(columns_to_remove)]
    else:
        return df

# Get Tables from MySQL Database
def get_tables(db_config):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    cursor.close()
    conn.close()
    return [table[0] for table in tables]

# Get Users
def get_users(db_config):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()
    cursor.execute("SELECT user FROM mysql.user")
    users = cursor.fetchall()
    cursor.close()
    conn.close()
    return [user[0] for user in users]

# Get apps list from apps table for this user
def get_app_group_list(curr_user, cursor):
    # Run this query and get list of tables
    # select distinct app_name from apps where app_user = 'User1';
    cursor.execute(f"SELECT DISTINCT app_group FROM udd_apps WHERE app_user = '{curr_user}'")
    app_list = cursor.fetchall()
    return [app[0] for app in app_list]   

# Get tables  from apps table for this user
def get_tables_list(curr_user, cursor, curr_app_grp):
    # Run this query and get list of tables
    cursor.execute(f"SELECT tables_associated_with_app FROM udd_apps WHERE app_user = '{curr_user}' and app_group = '{curr_app_grp}'")
    table_list = cursor.fetchall()
    return [table[0] for table in table_list]  

# Get apps list from apps table for this user
def get_app_list(curr_user, cursor, curr_app_grp):
    # Run this query and get list of apps
    cursor.execute(f"SELECT app_name FROM udd_apps WHERE app_user = '{curr_user}' and app_group = '{curr_app_grp}'")
    app_list = cursor.fetchall()
    return [app[0] for app in app_list]  

# Get apps list from apps table for this user
def get_table_name_from_app_name(cursor, app_name):
    cursor.execute(f"SELECT tables_associated_with_app FROM udd_apps WHERE app_name = '{app_name}'")
    app_list = cursor.fetchall()
    return [app[0] for app in app_list][0]  

# Get apps description
def get_app_description(curr_user, cursor, curr_app_grp, curr_table):
    # Run this query and get list of tables
    cursor.execute(f"SELECT app_description FROM udd_apps WHERE app_user = '{curr_user}' and app_group = '{curr_app_grp}' and tables_associated_with_app = '{curr_table}'")
    table_list = cursor.fetchall()
    return [table[0] for table in table_list]

# Set up MySQL database configuration
db_config = {
    'host': 'localhost',  # Change to your local MySQL host
    'user': 'root',       # Your MySQL username
    'password': 'Iam99%cool',  # Your MySQL password
    'database': 'db'  # Your MySQL database name
}

#create function for config
def sql_connnection(host = 'localhost', user = 'root', password = '', database = 'db'):
    return {
    'host' : host,  # Change to your local MySQL host
    'user': user,       # Your MySQL username
    'password': password,  # Your MySQL password
    'database': database,  # Your MySQL database name
    'auth_plugin': 'mysql_native_password',
    }

def process_cols(columns, tabname, cursor, curr_user, curr_time):
    i = 0
    stmt = ""
    for c in columns:
        if i == 0:
            stmt = "UPDATE " + tabname + " SET " + c + " = '" + str(columns[c]) + "'"
            i = 5
        else:
            stmt = stmt + ", " + c + " = '" + str(columns[c]) + "'"
    stmt += f",UPDATE_USERID = '{curr_user}', lst_upd_ts = '{curr_time}'"
    return stmt

def select_cols (df, idx):
    first = True
    stmt = ""
    cols = list(df.columns.values)
    for col in cols:
        if col in ["UPDATE_USERID", "lst_upd_ts"]:
            continue
        if first:
            stmt = " WHERE " + col + " = " + str(df.iloc[idx][col]) + ""
            first = False
        else:
            if str(df.iloc[idx][col]) == 'None' or  str(df.iloc[idx][col]) == "nan":
                stmt = stmt + " AND " + col + " IS NULL "
            else:
                stmt = stmt + " AND " + col + " = '" + str(df.iloc[idx][col]) + "'"
    return stmt

def insert_cols(cols, tabname):
    first = True
    stmt = ""
    vals = ""
    for col in cols:
        if first:
            stmt = "INSERT INTO " + tabname + " ( " + col 
            vals = " VALUES ('" + str(cols[col]) + "'"
            first = False
        else:
            stmt = stmt + ", " + col 
            vals = vals + ", '" + str(cols[col]) + "'"
    return stmt + ") " + vals + ")"

def delete_cols(idx, df, tabname):
    first = True
    stmt = ""
    cols = list(df.columns.values)
    for col in cols:
        if first:
            stmt = f"UPDATE {tabname} SET Active = 'N' WHERE {col} = '{str(df.iloc[idx][col])}'"
            first = False
        else:
            if pd.isnull(df.iloc[idx][col]):
                stmt += f" AND {col} IS NULL"
            else:
                stmt += f" AND {col} = '{str(df.iloc[idx][col])}'"
    return stmt

# Email validation function
def is_valid_email(email):
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None
# ******************** MAIN STREAMLIT APP ********************************

## ************* Testing Auth **********************
#st.image('https://logos-world.net/wp-content/uploads/2022/02/Truist-Emblem.png', width=80)
#st.title("UDD Data Loader")
with open(r'C:\UDD\config.yaml') as file:
    config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config['credentials']
)

try:
    authenticator.login('main')
    if st.session_state['authentication_status']:
        authenticator.logout()
        st.image('https://logos-world.net/wp-content/uploads/2022/02/Truist-Emblem.png', width=80)
        st.title("UDD Data Loader")
        st.subheader(f'Welcome *{st.session_state["name"]}*')
        page_bg_img = f"""
        <style>
        h1, h2, h3 {{
            color: #2E1A47;
        }}
        label {{
            color: #2E1A47;
        }}
        </style>
        """
        st.markdown(page_bg_img, unsafe_allow_html=True)

        curr_user = st.session_state["name"]
        #session_db_cred = sql_connnection('localhost', curr_user, '', 'db')
        # Changing for VDI Deployment run
        session_db_cred = sql_connnection('localhost', 'root', 'Yash@sonata123', 'db')
        sess_db_conn = mysql.connector.connect(**session_db_cred)
        cursor = sess_db_conn.cursor()
        curr_table_name = ""
        # ********************* Trying Tabs for About & Contacts Section ***********************
        home_tab, about_tab, contacts_tab = st.tabs(['Home', 'About', 'Contacts'])

        with home_tab:
            # ********************* Trying Tabs for About & Contacts Section End ***********************
            # ************************** App and Table option Code *********************************
            curr_app_grp = st.selectbox("App Name", get_app_group_list(curr_user, cursor), index = None, placeholder = "Select App")
            if curr_app_grp:
                st.write(f"App Description: Description of the app to be displayed here.")
                curr_app_name = st.selectbox("Table Name", get_app_list(curr_user, cursor, curr_app_grp), index = None, placeholder = "Select Table")
                if curr_app_name:
                    curr_table_name = get_table_name_from_app_name(cursor, curr_app_name)
                    st.write(f"Table Description: {get_app_description(curr_user, cursor, curr_app_grp, curr_table_name)[0]}")
                    curr_table = show_loaded_table(curr_table_name, session_db_cred)
                    st.write("Current Table:")
                    st.dataframe(curr_table)
                    # ************************** App and Table option Code End ****************************
                    genre = st.radio("",["Bulk Upload", "Manual Upload"], captions=[" "," ",])
                    if genre == "Bulk Upload":
                        file = st.file_uploader("Upload CSV or Excel file", type=["csv", "xlsx"])
                        if file:
                            df = data_preview(file)
                            # Upload button to load data
                            if st.button("Load Data"):
                                if curr_table_name:
                                    with st.spinner("Loading data..."):
                                        load_data_to_mysql(df, curr_table_name, session_db_cred)
                                    df = show_loaded_table(curr_table_name, session_db_cred)
                                    st.write("Loaded Table:")
                                    st.dataframe(df, use_container_width=True)
                                else:
                                    st.error("Please select a table.")
                    elif genre == "Manual Upload":
                        st.write("Edit Table:")
                        ss.edited = st.data_editor(curr_table,use_container_width=True, num_rows="dynamic", key = 'pq')
                        submit_button = st.button("Submit", key = 'ed')

                        if submit_button:
                            idx = 0
                            with st.spinner('Updating Table. Please Wait'):
                                #------- Validation Code ----------
                                invalid_emails = []
                                def validate_row(row):
                                    for col_name, value in row.items():
                                        col_name = col_name.lower()
                                        if col_name == "email" and not is_valid_email(value):
                                            invalid_emails.append(value)
                                for row_group in [ss["pq"]["edited_rows"].values(), ss["pq"]["added_rows"]]:
                                    for row in row_group:
                                        validate_row(row)
                                # Display errors if any invalid emails or SSNs are found
                                if invalid_emails:
                                    st.error(f"Invalid email(s) found: {', '.join(invalid_emails)}. Please enter a valid email")
                                    st.stop()
                                # ------ Validation Code End ---------
                                # ------ Deal with Datetime -------------------------------------------
                                cursor.execute('SELECT current_timestamp()')
                                curr_time = cursor.fetchall()
                                curr_time = curr_time[0][0]
                                # ------ Deal with Datetime END-------------------------------------------
                                
                                # Edited Rows
                                for rec in ss["pq"]["edited_rows"]:
                                    idx = int(rec)
                                    updt = process_cols(ss["pq"]["edited_rows"][rec], curr_table_name, cursor, curr_user, curr_time)
                                    where = select_cols(curr_table, idx)
                                    update_stmt = updt + " " + where
                                    cursor.execute(update_stmt)
                                    result = cursor.fetchall()
                                
                                # Process newly inserted row(s)
                                for irec in ss["pq"]["added_rows"]:
                                    irec["ACTIVE"] = f"Y"
                                    irec["UPDATE_USERID"] = f"{curr_user}"
                                    irec["lst_upd_ts"] = f"{curr_time}"
                                    irec["eff_from_dt"] = f"{curr_time}"
                                    insert_stmt = insert_cols(irec, curr_table_name)
                                    cursor.execute(insert_stmt)
                                    result = cursor.fetchall()
                                    
                                #Process the deleted row(s)
                                for rec in ss["pq"]["deleted_rows"]:
                                    idx = int(rec)
                                    delete_stmt = delete_cols(idx, curr_table, curr_table_name)
                                    cursor.execute(delete_stmt)
                                    result = cursor.fetchall()

                            st.success("Table updated successfully")


        with about_tab:
            st.subheader("App name here")
            st.write("App description here.")
            if curr_table_name != "":
                st.write(f"Table Description: {get_app_description(curr_user, cursor, curr_app_grp, curr_table_name)[0]}")

        with contacts_tab:
            st.subheader("Contact your Administrator for Access Issues")
            st.write("For all other issues contact Truist HelpDesk: +1 (866)567-4357 or log an incident ticket in [Servicenow](https://www.google.com/)")

        sess_db_conn.commit()
        cursor.close()
        sess_db_conn.close()

    elif st.session_state['authentication_status'] is False:
        st.error('Username/password is incorrect')
    elif st.session_state['authentication_status'] is None:
        st.warning('Please enter your username and password')
except Exception as e:
    st.error(e)
# ****************** End of Streamlit App *************