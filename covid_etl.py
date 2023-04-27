import psycopg2
import numpy as np
import pandas as pd 
import psycopg2.extras as extras


def data_extract(x):  #This is the function to ingest data into our pipeline
    df = pd.read_csv(x)
    df = df.dropna()
    return df


def transform_education_data():
    edu_data = data_extract("Education.csv")
    clean_edu_data = edu_data[["Federal Information Processing Standard (FIPS) Code", "State", "Area name", "2013 Rural-urban Continuum Code", "2013 Urban Influence Code" , "Less than a high school diploma, 2017-21", "High school diploma only, 2017-21", "Some college or associate's degree, 2017-21", "Bachelor's degree or higher, 2017-21", "Percent of adults with less than a high school diploma, 2017-21", "Percent of adults with a high school diploma only, 2017-21", "Percent of adults completing some college or associate's degree, 2017-21", "Percent of adults with a bachelor's degree or higher, 2017-21"]]
    #clean_edu_data.head(10)

    return clean_edu_data

def transform_population_data():
    pop_data = data_extract("PopulationEstimates.csv")
    clean_pop_data = pop_data[["Federal Information Processing Standards (FIPS) Code", "State", "Area name", "Rural-Urban Continuum Code 2013", "Population 2020"]]
    #clean_pop_data.head(10)

    return clean_pop_data

def transform_employment_data():
    emp_data = data_extract("Unemployment.csv")
    clean_emp_data = emp_data[['FIPS_code', 'State', 'Area_name', 'Rural_urban_continuum_code_2013', 'Urban_influence_code_2013', 'Employed_2020', 'Unemployed_2020', 'Unemployment_rate_2020', 'Median_Household_Income_2020', 'Med_HH_Income_Percent_of_State_Total_2020']]
    #clean_emp_data.head(10)

    return clean_emp_data


def transform_poverty_data():
    pov_data = pd.read_csv("PovertyEstimates.csv")
    clean_pov_data = pov_data[['FIPS_code', 'Stabr', 'Area_name', 'Rural-urban_Continuum_Code_2013', 'Urban_Influence_Code_2013', 'POVALL_2020', 'PCTPOVALL_2020', 'POV017_2020', 'PCTPOV017_2020', 'POV517_2020', 'PCTPOV517_2020', 'MEDHHINC_2020']]
    clean_pov_data = clean_pov_data.dropna()
    #clean_pov_data.head(10)

    return clean_pov_data

#Now we are going to transform data into the desired formats for loading into the database

def dim_location(): 
    df1 = transform_population_data()
    df1 = df1[["Federal Information Processing Standards (FIPS) Code", "State", "Area name", "Rural-Urban Continuum Code 2013"]]

    df2 = transform_employment_data()
    df2 = df2[["Urban_influence_code_2013"]]
    
    Dim_location = pd.concat([df1, df2], axis=1, join='inner')

    Dim_location.rename(columns={'Federal Information Processing Standards (FIPS) Code':'fips_code'}, inplace = True)
    Dim_location.rename(columns={'Area name':'area_name'}, inplace = True)
    Dim_location.rename(columns={'State':'state'}, inplace = True)
    Dim_location.rename(columns={'Rural-Urban Continuum Code 2013':'rural_urban_continuum_code'}, inplace = True)
    Dim_location.rename(columns={'Urban_influence_code_2013':' urban_influence_code'}, inplace = True)
    
    #Dim_location.head(10)

    return Dim_location



def dim_population():

    df3 = transform_population_data()
    df3 = df3[["Federal Information Processing Standards (FIPS) Code", "Population 2020"]] 

    df4 = transform_poverty_data()
    df4 = df4[['POVALL_2020', 'POV017_2020', 'POV517_2020']]

    df5 = transform_employment_data()
    df5 = df5[['Employed_2020', 'Unemployed_2020']]

    df6 = transform_education_data()
    df6 = df6[["Less than a high school diploma, 2017-21", "High school diploma only, 2017-21", "Bachelor's degree or higher, 2017-21"]]

    Dim_Population = pd.concat([df3, df4, df5, df6], axis = 1, join = 'inner')
    
    Dim_Population.rename(columns={'Federal Information Processing Standards (FIPS) Code':'fips_code'}, inplace = True)
    Dim_Population.rename(columns={'Population 2020':'population'}, inplace = True)
    Dim_Population.rename(columns={'POVALL_2020':'people_all_ages_in_poverty'}, inplace = True)
    Dim_Population.rename(columns={'POV017_2020':'ages_0_to_17_in_poverty'}, inplace = True)
    Dim_Population.rename(columns={'POV517_2020':'related_children_ages_5_to_17_in_poverty'}, inplace = True)
    Dim_Population.rename(columns={'Employed_2020':'employed_people', }, inplace = True)
    Dim_Population.rename(columns={'Unemployed_2020':'unemployed_people'}, inplace = True)
    Dim_Population.rename(columns={'Less than a high school diploma, 2017-21':'adults_with_less_than_highschool_diploma'}, inplace = True)
    Dim_Population.rename(columns={'High school diploma only, 2017-21':'adults_with_highschool_diploma_only'}, inplace = True)
    Dim_Population.rename(columns={"Bachelor's degree or higher, 2017-21":'adults_with_bachelors_or_higher'}, inplace = True)

    Dim_Population = Dim_Population.replace(",", "", regex=True)

    return Dim_Population



def fact_table():
    
    df7 = transform_poverty_data()
    df7 = df7[['FIPS_code', 'PCTPOVALL_2020', 'PCTPOV017_2020', 'PCTPOV517_2020', 'MEDHHINC_2020']]

    df8 = transform_education_data()
    df8 = df8[["Percent of adults with less than a high school diploma, 2017-21", "Percent of adults with a high school diploma only, 2017-21", "Percent of adults with a bachelor's degree or higher, 2017-21"]]

    df9 = transform_employment_data()
    df9 = df9[['Unemployment_rate_2020']]

    Fact_table = pd.concat([df7, df8, df9], axis=1, join='inner')

    Fact_table.rename(columns={'FIPS_code':'fips_code'}, inplace = True)
    Fact_table.rename(columns={'PCTPOVALL_2020':'people_all_ages_in_poverty'}, inplace = True)
    Fact_table.rename(columns={'POVALL_2020':'people_all_ages_in_poverty'}, inplace = True)
    Fact_table.rename(columns={'PCTPOV017_2020':'ages_0_to_17_in_poverty'}, inplace = True)
    Fact_table.rename(columns={'PCTPOV517_2020':'related_children_ages_5_to_17_in_poverty'}, inplace = True)
    Fact_table.rename(columns={'MEDHHINC_2020':'median_income', }, inplace = True)
    Fact_table.rename(columns={'Unemployment_rate_2020':'unemployment_rate'}, inplace = True)
    Fact_table.rename(columns={'Percent of adults with less than a high school diploma, 2017-21':'adults_with_less_than_highschool_diploma'}, inplace = True)
    Fact_table.rename(columns={'Percent of adults with a high school diploma only, 2017-21':'adults_with_highschool_diploma_only'}, inplace = True)
    Fact_table.rename(columns={"Percent of adults with a bachelor's degree or higher, 2017-21":'adults_with_bachelors_or_higher'}, inplace = True)

    Fact_table = Fact_table.replace(",", "", regex=True)

    #Fact_table.head(10)

    return Fact_table



def load_data(conn, df, table):   #Function to load data

	tuples = [tuple(x) for x in df.to_numpy()]

	cols = ','.join(list(df.columns))
	# SQL query to execute
	query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
	cursor = conn.cursor()
	try:
		extras.execute_values(cursor, query, tuples)
		conn.commit()
	except (Exception, psycopg2.DatabaseError) as error:
		print("Error: %s" % error)
		conn.rollback()
		cursor.close()
		return 1
	print("the dataframe is inserted")
	cursor.close()





def final_stage():  # Calling load function to commit loading to the db
    conn = psycopg2.connect("host=<host> dbname=<database> user=<user> password=<password>")
    df_location = dim_location()
    df_population = dim_population()
    df_factable = fact_table()

    load_data(conn, df_location, 'locationdata')
    load_data(conn, df_population, 'populationdata')
    load_data(conn, df_factable, 'pandemicdata')

final_stage()