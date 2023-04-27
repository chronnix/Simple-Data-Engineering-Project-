fact_table_create = ("""
Create table If Not Exists PandemicData \
    (FIPS_code int Primary Key,
    people_all_ages_in_poverty decimal,
    ages_0_to_17_in_poverty decimal,
    related_children_ages_5_to_17_in_poverty decimal,
    ages_0_to_4_in_poverty decimal,
    adults_with_bachelors_or_higher decimal,
    adults_with_highschool_diploma_only decimal,
    adults_with_less_than_highschool_diploma decimal,
    unemployment_rate decimal,
    median_income int)
""")


dim_population_table_create =("""
Create table If Not Exists PopulationData \
    (FIPS_code int Primary Key,
    population int,
    people_all_ages_in_poverty int,
    ages_0_to_17_in_poverty int,
    related_children_ages_5_to_17_in_poverty int,
    ages_0_to_4_in_poverty int,
    employed_people int,
    unemployed_people int,
    adults_with_bachelors_or_higher int,
    adults_with_highschool_diploma_only int,
    adults_with_less_than_highschool_diploma int)
""")



dim_location_table_create = ("""
Create Table If Not Exists LocationData \
    (FIPS_code int Primary Key,
    state varchar,
    area_name text,
    rural_urban_continuum_code int,
    urban_influence_code int)
""")



create_table_queries = [fact_table_create, dim_population_table_create, dim_location_table_create]



fact_table_insert = ("""
Insert Into pandemicdata (FIPS_code, people_all_ages_in_poverty, ages_0_to_17_in_poverty, related_children_ages_5_to_17_in_poverty, ages_0_to_4_in_poverty, adults_with_bachelors_or_higher, adults_with_highschool_diploma_only, adults_with_less_than_highschool_diploma, unemployment_rate, median_income) \
    Values(%s,%s, %s, %s, %s, %s, %s, %s,%s,%s)
""")


dim_population_table_insert = ("""
Insert Into populationdata(FIPS_code, population, people_all_ages_in_poverty, ages_0_to_17_in_poverty, related_children_ages_5_to_17_in_poverty, ages_0_to_4_in_poverty, employed_people, unemployed_people, adults_with_bachelors_or_higher, adults_with_highschool_diploma_only, adults_with_less_than_highschool_diploma ) \
    Values (%s,%s, %s, %s, %s, %s, %s, %s,%s,%s,%s)
""")


dim_location_table_insert = ("""
Insert Into locationdata (FIPS_code, state, area_name, rural_urban_continuum_code, urban_influence_code) \
    Values (%s)
""")
