

import pandas as pd 
import sqlite3


def create_db(db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    c.execute("""CREATE TABLE customers
                (gender text, firstname text, lastname text, email text, age int, city text, country text, created_at text, referral text)""")

    # c.execute("DROP TABLE customers")
    conn.commit()
    conn.close()

# create_db("plastic_free_boutique.db")




def my_m_and_a(csv1, csv2, csv3, db_name):
    
    conn = sqlite3.connect(db_name)     # createing an connection engine
    c = conn.cursor()

# CSV1:

    df1 = pd.read_csv(csv1)
    df1.drop("UserName", axis=1, inplace=True)      # column "UserName" does not exist in our database
  


# CSV2:

    df2 = pd.read_csv(csv2, delimiter=';', header=None, names=["age", "city", "gender", "firstname lastname", "email"]) 
    df2[["firstname", "lastname"]] = df2['firstname lastname'].str.split(expand=True)
    df2.drop(["firstname lastname"], axis=1, inplace=True)
    
# CSV3:

    df3 = pd.read_csv(csv3)
    df3[["Gender", "Name", "Email", "Age", "City", "Country"]] = df3["Gender"].str.split('\t', expand=True)
    df3[["firstname", "lastname"]] = df3['Name'].str.split(expand=True)
    df3.drop(["Name"], axis=1, inplace=True)

    to_delete = ['string_', '"integer_', 'boolean_', 'character_']      # Removing these prefixes

# All CSV:

    for each in df3:
        for prefix in to_delete:
            df3[each] = df3[each].map(lambda x: x.lstrip(prefix).rstrip('"'))   # cleaning data from left and right side

   
    frames = [df1, df2, df3]
    for i in frames:
        i.columns = i.columns.str.lower()

    df = pd.concat(frames, ignore_index=True)       # To work with all data

# Cleaning:

# Firstname Lastname:

    df_name = ["firstname", "lastname", "city", "email"]

    for each in df_name:

        if each == "city":
            df[each] = df[each].str.capitalize()

        elif each == "email": 
            df[each] = df[each].str.lower()

        else:
            df[each] = df[each].str.extract("(\w+)")        # Only words for the names
            df[each] = df[each].str.capitalize()
        
# Gender: 

    for i in ['0', 'F', '1', 'M']:      # Female/Male indicated by characters
        if i == '0' or i == 'F':
            df.gender = df.gender.replace(i, "Female")
        else:
            df.gender = df.gender.replace(i, "Male")

# Country:

    for i in set(df.country):   # All variations are USA, others are NaN
        if type(i) is str:      # NaN is not str
            df.country = df.country.replace(i, "USA")   # Only USA customers
 
# Age:

    df = df.astype({"age":"str"})           # changing data types to str
    df.age = df.age.str.extract("(\d+)")        # Extracting digits as str
    df = df.astype({"age":"int64"})         # numbers as int
    

    df.to_sql('customers', con = conn, index=False, if_exists='append')


    conn.commit()       
    conn.close()        # closing our engine



# my_m_and_a("csv_1_draft.csv", "csv_2_draft.csv", "csv_3_draft.csv", "plastic_free_boutique.db")
my_m_and_a("only_wood_customer_us_1.csv", "only_wood_customer_us_2.csv", "only_wood_customer_us_3.csv", "plastic_free_boutique.db")


# df_group = pd.read_csv("only_wood_customer_us_3.csv")















