from pyspark import SparkConf, SparkContext
import sys
import re, string
import operator
import json
import requests
import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from zipfile import ZipFile
from pyspark.sql import SparkSession, functions, types
from io import *
import csv
import pandas as pd
from urllib.request import *
import getCodeSets as codesets
import loadHousingPrice as housepriceindex
import loadLabourFource as laborfoce
import loadIncomeData as incomedata
import loadDiverseExpenditures as expenditures
import loadTouristInfo as tourist_data
import loadCrimeData as crime_incidents
import loadCPI as cpi_data
import loadOilData as oil_data
import loadImmigrationData as immigration_020
import loadImmigrationData_2 as immigration_040
import loadMortageInfo as mortage_info
import loadWeather as weather_data

spark = SparkSession.builder.appName('Load Data Sets').getOrCreate()


def main():
    housing_df = housepriceindex.loadPriceIndex().createOrReplaceTempView("housing")
    income_df = incomedata.loadIncomeData().createOrReplaceTempView("income")
    expenditures_df = expenditures.loadExpenditures().createOrReplaceTempView("expenditures")
    tourism_df = tourist_data.loadTouristInfo().createOrReplaceTempView("tourist_info")
    labourforce_df = laborfoce.loadLabourForceData().createOrReplaceTempView("labour_force")
    crimes_df = crime_incidents.loadCrimeData().createOrReplaceTempView("crimes")
    cpi_df = cpi_data.loadCPI().createOrReplaceTempView("cpi")
    oil_df = oil_data.loadOilData().createOrReplaceTempView("oil")
    imm_df = immigration_020.loadImmigrationData().createOrReplaceTempView("immigration_020")
    imm2_df = immigration_040.loadImmigrationData().createOrReplaceTempView("immigration_040")
    mortage = mortage_info.loadMortageInfo().createOrReplaceTempView("mortage_info")
    weather = weather_data.loadWeatherData().createOrReplaceTempView("weather_data")
    house_date_transformed = spark.sql("SELECT h.province, h.REF_DATE, h.uom_houseindex, h.scalar_houseindex,h.avg_house_only,h.avg_land_only,SUBSTR(h.REF_DATE , 0, INSTR(h.REF_DATE , '-')-1) as ref_date_temp \
      FROM housing h").createOrReplaceTempView("housing_temp")
    join_houseindex_income = spark.sql("SELECT h.province, h.REF_DATE, h.uom_houseindex, h.scalar_houseindex,h.avg_house_only,h.avg_land_only,l.uom_income, l.scalar_income,l.statistic_income, round(l.avg_income/12,2) as avg_income \
    FROM housing_temp h LEFT JOIN income l ON h.province = l.province AND h.ref_date_temp = l.REF_DATE").createOrReplaceTempView(
        "house_income")

    join_prev_expenditures = spark.sql("SELECT h.province, h.REF_DATE, h.uom_houseindex, h.scalar_houseindex,h.avg_house_only,h.avg_land_only, h.uom_income, h.scalar_income,h.statistic_income,h.avg_income, \
    e.uom_expenditure , e.scalar_expenditure,e.statistic_expenditure,round(e.avg_food_expenditures/12,2) as avg_food_expenditures, round(e.avg_income_taxes/12,2) as avg_income_taxes, round(e.avg_mortageinsurance/12,2) as avg_mortageinsurance, round(e.avg_mortagePaid/12,2) as avg_mortagePaid ,\
    round(e.avg_accomodation/12,2) as avg_accomodation, round(e.avg_rent/12,2) as avg_rent, round(e.avg_shelter/12,2) as avg_shelter, round(e.avg_total_expenditure/12,2) as avg_total_expenditure, round(e.avg_taxes_landregfees/12,2) as avg_taxes_landregfees \
    FROM house_income h LEFT JOIN expenditures e ON h.province = e.province AND SUBSTR(h.REF_DATE , 0, INSTR(h.REF_DATE , '-')-1) = e.REF_DATE ").createOrReplaceTempView(
        "prev_plus_expenditures")

    join_prev_touristdata = spark.sql("SELECT h.province, h.REF_DATE, h.uom_houseindex, h.scalar_houseindex,h.avg_house_only,h.avg_land_only, h.uom_income, h.scalar_income,h.statistic_income,h.avg_income, \
    h.uom_expenditure , h.scalar_expenditure,h.statistic_expenditure,h.avg_food_expenditures, h.avg_income_taxes, h.avg_mortageinsurance, h.avg_mortagePaid ,\
    h.avg_accomodation, h.avg_rent, h.avg_shelter, h.avg_total_expenditure, h.avg_taxes_landregfees, \
    t.uom_tourist, t.scalar_tourist,t.avg_international_tourism as avg_international_tourism, t.avg_domestic_tourism as avg_domestic_tourism \
     FROM prev_plus_expenditures h LEFT JOIN tourist_info t ON h.province = t.province AND h.REF_DATE=t.REF_DATE").createOrReplaceTempView(
        "prev_plus_labourforce")

    join_prev_labourforce = spark.sql("SELECT h.province ,h.REF_DATE,h.uom_houseindex, h.scalar_houseindex, h.avg_house_only,h.avg_land_only,h.uom_income, h.scalar_income,h.statistic_income, h.avg_income,\
    h.uom_expenditure,h.scalar_expenditure,h.statistic_expenditure,h.avg_food_expenditures, h.avg_income_taxes, h.avg_mortageinsurance, h.avg_mortagePaid, \
    h.avg_accomodation, h.avg_rent, h.avg_shelter, h.avg_total_expenditure, h.avg_taxes_landregfees,\
    h.uom_tourist, h.scalar_tourist,h.avg_international_tourism, h.avg_domestic_tourism, \
    l.uom_labourforce,l.scalar_labourforce,l.statistic_labourforce,l.avg_employment, l.avg_fulltime, l.avg_labourforce, l.avg_parttime, l.avg_population, l.avg_unemployment,\
    l.uom_lfperc,l.scalar_lfperc,l.statistic_labourforceperc,l.avg_employment_rate,l.avg_participationrate,l.avg_unemploymentrate \
    FROM prev_plus_labourforce h LEFT JOIN labour_force l ON h.province=l.province AND h.REF_DATE=l.REF_DATE").createOrReplaceTempView(
        "prev_plus_crimes")

    join_prev_crimes = spark.sql("SELECT h.province ,h.REF_DATE,h.uom_houseindex, h.scalar_houseindex, h.avg_house_only,h.avg_land_only,h.uom_income, h.scalar_income,h.statistic_income, h.avg_income,\
    h.uom_expenditure,h.scalar_expenditure,h.statistic_expenditure,h.avg_food_expenditures, h.avg_income_taxes, h.avg_mortageinsurance, h.avg_mortagePaid, \
    h.avg_accomodation, h.avg_rent, h.avg_shelter, h.avg_total_expenditure, h.avg_taxes_landregfees,\
    h.uom_tourist, h.scalar_tourist,h.avg_international_tourism, h.avg_domestic_tourism, \
    h.uom_labourforce,h.scalar_labourforce,h.statistic_labourforce,h.avg_employment, h.avg_fulltime, h.avg_labourforce, h.avg_parttime, h.avg_population, h.avg_unemployment,\
    h.uom_lfperc,h.scalar_lfperc,h.statistic_labourforceperc,h.avg_employment_rate,h.avg_participationrate,h.avg_unemploymentrate, \
    c.uom_crime, c.scalar_crime, c.statistic_crime,round(c.avg_crime_incidents/12,2) as avg_crime_incidents \
    FROM prev_plus_crimes h LEFT JOIN crimes c ON h.province = c.province AND SUBSTR(h.REF_DATE , 0, INSTR(h.REF_DATE , '-')-1) = c.REF_DATE").createOrReplaceTempView(
        "prev_plus_cpi")  # .coalesce(1)

    join_prev_cpi = spark.sql("SELECT h.province ,h.REF_DATE,h.uom_houseindex, h.scalar_houseindex, h.avg_house_only,h.avg_land_only,h.uom_income, h.scalar_income,h.statistic_income, h.avg_income,\
    h.uom_expenditure,h.scalar_expenditure,h.statistic_expenditure,h.avg_food_expenditures, h.avg_income_taxes, h.avg_mortageinsurance, h.avg_mortagePaid, \
    h.avg_accomodation, h.avg_rent, h.avg_shelter, h.avg_total_expenditure, h.avg_taxes_landregfees,\
    h.uom_tourist, h.scalar_tourist,h.avg_international_tourism, h.avg_domestic_tourism, \
    h.uom_labourforce,h.scalar_labourforce,h.statistic_labourforce,h.avg_employment, h.avg_fulltime, h.avg_labourforce, h.avg_parttime, h.avg_population, h.avg_unemployment,\
    h.uom_lfperc,h.scalar_lfperc,h.statistic_labourforceperc,h.avg_employment_rate,h.avg_participationrate,h.avg_unemploymentrate, \
    h.uom_crime, h.scalar_crime, h.statistic_crime,h.avg_crime_incidents, \
    c.uom_cpi, c.scalar_cpi, c.avg_cpi_index \
    FROM prev_plus_cpi h LEFT JOIN cpi c ON h.REF_DATE = c.REF_DATE").createOrReplaceTempView("prev_plus_oil")  #

    join_prev_oil = spark.sql("SELECT h.province ,h.REF_DATE,h.uom_houseindex, h.scalar_houseindex, h.avg_house_only,h.avg_land_only,h.uom_income, h.scalar_income,h.statistic_income, h.avg_income,\
    h.uom_expenditure,h.scalar_expenditure,h.statistic_expenditure,h.avg_food_expenditures, h.avg_income_taxes, h.avg_mortageinsurance, h.avg_mortagePaid, \
    h.avg_accomodation, h.avg_rent, h.avg_shelter, h.avg_total_expenditure, h.avg_taxes_landregfees,\
    h.uom_tourist, h.scalar_tourist,h.avg_international_tourism, h.avg_domestic_tourism, \
    h.uom_labourforce,h.scalar_labourforce,h.statistic_labourforce,h.avg_employment, h.avg_fulltime, h.avg_labourforce, h.avg_parttime, h.avg_population, h.avg_unemployment,\
    h.uom_lfperc,h.scalar_lfperc,h.statistic_labourforceperc,h.avg_employment_rate,h.avg_participationrate,h.avg_unemploymentrate, \
    h.uom_crime, h.scalar_crime, h.statistic_crime,h.avg_crime_incidents, \
    h.uom_cpi, h.scalar_cpi, h.avg_cpi_index, \
    o.uom_oil, o.scalar_oil, o.avg_diesel_fillingstations, o.avg_diesel_selfservstations,o.avg_premium_fillingstations,o.avg_premium_selfservstations, o.avg_regular_fillingstations, o.avg_regular_selfservstations  \
    FROM prev_plus_oil h LEFT JOIN oil o ON h.province = o.province AND h.REF_DATE = o.REF_DATE").createOrReplaceTempView(
        "prev_plus_immigration")

    prev_plus_immigration = spark.sql("SELECT h.province ,h.REF_DATE,h.uom_houseindex, h.scalar_houseindex, h.avg_house_only,h.avg_land_only,h.uom_income, h.scalar_income,h.statistic_income, h.avg_income,\
    h.uom_expenditure,h.scalar_expenditure,h.statistic_expenditure,h.avg_food_expenditures, h.avg_income_taxes, h.avg_mortageinsurance, h.avg_mortagePaid, \
    h.avg_accomodation, h.avg_rent, h.avg_shelter, h.avg_total_expenditure, h.avg_taxes_landregfees,\
    h.uom_tourist, h.scalar_tourist,h.avg_international_tourism, h.avg_domestic_tourism, \
    h.uom_labourforce,h.scalar_labourforce,h.statistic_labourforce,h.avg_employment, h.avg_fulltime, h.avg_labourforce, h.avg_parttime, h.avg_population, h.avg_unemployment,\
    h.uom_lfperc,h.scalar_lfperc,h.statistic_labourforceperc,h.avg_employment_rate,h.avg_participationrate,h.avg_unemploymentrate, \
    h.uom_crime, h.scalar_crime, h.statistic_crime,h.avg_crime_incidents, \
    h.uom_cpi, h.scalar_cpi, h.avg_cpi_index, \
    h.uom_oil, h.scalar_oil, h.avg_diesel_fillingstations, h.avg_diesel_selfservstations,h.avg_premium_fillingstations,h.avg_premium_selfservstations, h.avg_regular_fillingstations, h.avg_regular_selfservstations,  \
    imm1.uom_imm, imm1.scalar_imm ,(imm1.avg_in_migrants + imm2.avg_immigrants - imm1.avg_out_migrants) as avg_immigrants \
    FROM prev_plus_immigration h \
    LEFT JOIN immigration_020 imm1 ON h.province = imm1.province AND h.REF_DATE = imm1.REF_DATE \
    LEFT JOIN immigration_040 imm2 ON h.province = imm2.province AND h.REF_DATE = imm2.REF_DATE").createOrReplaceTempView(
        "prev_plus_mortage")

    prev_plus_mortage = spark.sql("SELECT h.province ,h.REF_DATE,h.uom_houseindex, h.scalar_houseindex, h.avg_house_only,h.avg_land_only,h.uom_income, h.scalar_income,h.statistic_income, h.avg_income,\
    h.uom_expenditure,h.scalar_expenditure,h.statistic_expenditure,h.avg_food_expenditures, h.avg_income_taxes, h.avg_mortageinsurance, h.avg_mortagePaid, \
    h.avg_accomodation, h.avg_rent, h.avg_shelter, h.avg_total_expenditure, h.avg_taxes_landregfees,\
    h.uom_tourist, h.scalar_tourist,h.avg_international_tourism, h.avg_domestic_tourism, \
    h.uom_labourforce,h.scalar_labourforce,h.statistic_labourforce,h.avg_employment, h.avg_fulltime, h.avg_labourforce, h.avg_parttime, h.avg_population, h.avg_unemployment,\
    h.uom_lfperc,h.scalar_lfperc,h.statistic_labourforceperc,h.avg_employment_rate,h.avg_participationrate,h.avg_unemploymentrate, \
    h.uom_crime, h.scalar_crime, h.statistic_crime,h.avg_crime_incidents, \
    h.uom_cpi, h.scalar_cpi, h.avg_cpi_index, \
    h.uom_oil, h.scalar_oil, h.avg_diesel_fillingstations, h.avg_diesel_selfservstations,h.avg_premium_fillingstations,h.avg_premium_selfservstations, h.avg_regular_fillingstations, h.avg_regular_selfservstations,  \
    h.uom_imm, h.scalar_imm ,h.avg_immigrants, \
    m.avg_1y_fixed_posted, m.avg_2y_bond, m.avg_3y_bond, m.avg_3y_fixed_posted, m.avg_5y_bond, m.avg_5y_fixed_posted, m.avg_7y_bond, m.avg_10y_bond, m.avg_bank, m.avg_overnight, m.avg_overnight_target, m.avg_prime \
    FROM prev_plus_mortage h \
    LEFT JOIN mortage_info m ON h.REF_DATE = m.REF_DATE").createOrReplaceTempView("prev_plus_weather")

    prev_plus_weather = spark.sql("SELECT h.province ,h.REF_DATE,h.uom_houseindex, h.scalar_houseindex, h.avg_house_only,h.avg_land_only,h.uom_income, h.scalar_income,h.statistic_income, h.avg_income,\
    h.uom_expenditure,h.scalar_expenditure,h.statistic_expenditure,h.avg_food_expenditures, h.avg_income_taxes, h.avg_mortageinsurance, h.avg_mortagePaid, \
    h.avg_accomodation, h.avg_rent, h.avg_shelter, h.avg_total_expenditure, h.avg_taxes_landregfees,\
    h.uom_tourist, h.scalar_tourist,h.avg_international_tourism, h.avg_domestic_tourism, \
    h.uom_labourforce,h.scalar_labourforce,h.statistic_labourforce,h.avg_employment, h.avg_fulltime, h.avg_labourforce, h.avg_parttime, h.avg_population, h.avg_unemployment,\
    h.uom_lfperc,h.scalar_lfperc,h.statistic_labourforceperc,h.avg_employment_rate,h.avg_participationrate,h.avg_unemploymentrate, \
    h.uom_crime, h.scalar_crime, h.statistic_crime,h.avg_crime_incidents, \
    h.uom_cpi, h.scalar_cpi, h.avg_cpi_index, \
    h.uom_oil, h.scalar_oil, h.avg_diesel_fillingstations, h.avg_diesel_selfservstations,h.avg_premium_fillingstations,h.avg_premium_selfservstations, h.avg_regular_fillingstations, h.avg_regular_selfservstations,  \
    h.uom_imm, h.scalar_imm ,h.avg_immigrants, \
    h.avg_1y_fixed_posted, h.avg_2y_bond, h.avg_3y_bond, h.avg_3y_fixed_posted, h.avg_5y_bond, h.avg_5y_fixed_posted, h.avg_7y_bond, h.avg_10y_bond, h.avg_bank, h.avg_overnight, h.avg_overnight_target, h.avg_prime, \
    w.Mean_Max_Temp,w.Mean_Min_Temp, w.Mean_Temp,w.Total_Rain,w.Total_Snow\
    FROM prev_plus_weather h \
    LEFT JOIN weather_data w ON h.REF_DATE = w.REF_DATE AND h.province = w.province").coalesce(1)
    # coalesce(1)

    prev_plus_weather.write.csv("data", mode='overwrite', header='true')


if __name__ == '__main__':
    # type_ = sys.argv[1]
    # output = sys.argv[2]
    main()