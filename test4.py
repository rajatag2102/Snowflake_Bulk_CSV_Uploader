from snowflake.snowpark import Session
import os
import pandas as pd
import streamlit as st
import shutil

st.set_page_config(layout="wide")

# st.header("Bulk CSV file Loader into Snowflake")

st.markdown("""
<style>
.css-z5fcl4 {
    width: 100%;
    padding: 2rem 5rem 5rem 5rem;
    min-width: auto;
    max-width: initial;
}
.css-fg4pbf {
    position: absolute;
    background: #C1D5F0  ;
    color: rgb(49, 51, 63);
    inset: 0px;
    overflow: hidden;
}
.css-18ni7ap {
    position: fixed;
    top: 0px;
    left: 0px;
    right: 0px;
    height: 0rem;
}
</style>
""", unsafe_allow_html=True)

st.markdown("<h2 style='text-align: center; color: black;text-size: 2px;'>Bulk CSV file Loader into Snowflake</h4>", unsafe_allow_html=True)
def snowpark_session_create():
    st.markdown("""
<h4 style='text-align: justify; color: black;'>Creating Connection</h4>
""", unsafe_allow_html=True)
    connection_params = {
    "user": "xxxx",
    "password": "xxxx",
    "account": "xxxx",
    "warehouse": "xxxx",
    "role": "xxxx",
    }
    session = Session.builder.configs(connection_params).create()
    return session
session = snowpark_session_create()
st.markdown("""
<h4 style='text-align: left; color: black;'>Connection Established</h4>
""", unsafe_allow_html=True)

def file_uploading(base_path, archive_path):
    st.write(session.sql("use database xxxx").collect())
    st.write(session.sql("use schema xxxx").collect())

    csv_files = [file for file in os.listdir(base_path) if file.endswith('.csv')]
    st.write(csv_files)
    if csv_files:
        for file in csv_files:
            df = pd.read_csv(os.path.join(base_path, file))
            st.write("CSV File: ", file)
            session.createDataFrame(df).write.save_as_table(table_name = file.replace('.csv', ''),table_type="transient")
            shutil.move(os.path.join(base_path, file), os.path.join(archive_path, file))
            st.write(file,"Uploaded Successfully")
    else:
        st.write("No CSV File Found")

if __name__ ==  "__main__":

    base_path = st.text_input("Enter Your Base Path")
    archive_path = st.text_input("Enter Your Archive Path")
    if st.button('Apply'):
        file_uploading(base_path, archive_path)
    
