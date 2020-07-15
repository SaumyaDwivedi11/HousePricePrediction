import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
from urllib.request import *
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
    #Load all dataframes. Each of these methods will return a formatted dataframe ready to be joined with each other.

    housing_df = housepriceindex.loadPriceIndex().cache().createOrReplaceTempView("housing")
    income_df = incomedata.loadIncomeData().cache().createOrReplaceTempView("income")
    expenditures_df = expenditures.loadExpenditures().cache().createOrReplaceTempView("expenditures")
    tourism_df = tourist_data.loadTouristInfo().cache().createOrReplaceTempView("tourist_info")
    labourforce_df = laborfoce.loadLabourForceData().cache().createOrReplaceTempView("labour_force")
    crimes_df = crime_incidents.loadCrimeData().cache().createOrReplaceTempView("crimes")
    cpi_df = cpi_data.loadCPI().cache().createOrReplaceTempView("cpi")
    oil_df = oil_data.loadOilData().cache().createOrReplaceTempView("oil")
    imm_df = immigration_020.loadImmigrationData().cache().createOrReplaceTempView("immigration_020")
    imm2_df = immigration_040.loadImmigrationData().cache().createOrReplaceTempView("immigration_040")
    mortage = mortage_info.loadMortageInfo().cache().createOrReplaceTempView("mortage_info")
    weather = weather_data.loadWeatherData().cache().createOrReplaceTempView("weather_data")


    house_date_transformed = spark.sql("SELECT h.GEO, h.DGUID, h.REF_DATE, h.uom_houseindex, h.scalar_houseindex,h.house_only,h.land_only, h.total_house_land, SUBSTR(h.REF_DATE , 0, INSTR(h.REF_DATE , '-')-1) as ref_date_temp \
      FROM housing h").createOrReplaceTempView("housing_temp")

    # Join HPI with Income
    join_houseindex_income = spark.sql("SELECT h.GEO, h.DGUID, h.REF_DATE, h.uom_houseindex, h.scalar_houseindex,h.house_only,h.land_only, h.total_house_land,l.uom_income, l.scalar_income,l.statistic_income, round(l.income/12,2) as income \
    FROM housing_temp h LEFT JOIN income l ON h.DGUID = l.DGUID AND h.ref_date_temp = l.REF_DATE").createOrReplaceTempView(
        "house_income")
    # Join previous datafrmae with expenditure information
    join_prev_expenditures = spark.sql("SELECT h.GEO, h.REF_DATE, h.DGUID, h.uom_houseindex, h.scalar_houseindex, h.house_only, h.land_only, h.total_house_land, h.uom_income, h.scalar_income,h.statistic_income,h.income, \
    e.uom_expenditure , e.scalar_expenditure,e.statistic_expenditure,round(e.food_expenditures/12,2) as food_expenditures, round(e.income_taxes/12,2) as income_taxes, round(e.Mortgage_insurance/12,2) as mortageinsurance, round(e.Mortgage_paid/12,2) as mortagePaid ,\
    round(e.Principal_accommodation/12,2) as accomodation, round(e.rent/12,2) as rent, round(e.shelter/12,2) as shelter, round(e.total_expenditure/12,2) as total_expenditure, round(e.Taxes_transfer_landregistration_fees/12,2) as taxes_landregfees \
    FROM house_income h LEFT JOIN expenditures e ON h.DGUID = e.DGUID AND SUBSTR(h.REF_DATE , 0, INSTR(h.REF_DATE , '-')-1) = e.REF_DATE ").createOrReplaceTempView(
        "prev_plus_expenditures")

    # Join previous datafrmae with tourist information
    join_prev_touristdata = spark.sql("SELECT h.GEO, h.REF_DATE, h.DGUID, h.uom_houseindex, h.scalar_houseindex,h.house_only,h.land_only, h.total_house_land, h.uom_income, h.scalar_income,h.statistic_income,h.income, \
    h.uom_expenditure , h.scalar_expenditure,h.statistic_expenditure,h.food_expenditures, h.income_taxes, h.mortageinsurance, h.mortagePaid ,\
    h.accomodation, h.rent, h.shelter, h.total_expenditure, h.taxes_landregfees, \
    t.uom_tourist, t.scalar_tourist,t.international_tourism as international_tourism, t.domestic_tourism as domestic_tourism \
     FROM prev_plus_expenditures h LEFT JOIN tourist_info t ON h.DGUID = t.DGUID AND h.REF_DATE=t.REF_DATE").createOrReplaceTempView(
        "prev_plus_labourforce")

    # Join previous datafrmae with labour information
    join_prev_labourforce = spark.sql("SELECT h.GEO ,h.REF_DATE, h.DGUID, h.uom_houseindex, h.scalar_houseindex, h.house_only,h.land_only,h.total_house_land, h.uom_income, h.scalar_income,h.statistic_income, h.income,\
    h.uom_expenditure,h.scalar_expenditure,h.statistic_expenditure,h.food_expenditures, h.income_taxes, h.mortageinsurance, h.mortagePaid, \
    h.accomodation, h.rent, h.shelter, h.total_expenditure, h.taxes_landregfees,\
    h.uom_tourist, h.scalar_tourist,h.international_tourism, h.domestic_tourism, \
    l.uom_labourforce,l.scalar_labourforce,l.statistic_labourforce,l.employment, l.fulltime, l.labourforce, l.parttime, l.population, l.unemployment,\
    l.uom_lfperc,l.scalar_lfperc,l.statistic_labourforceperc,l.employment_rate,l.participationrate,l.unemployment_rate \
    FROM prev_plus_labourforce h LEFT JOIN labour_force l ON h.DGUID=l.DGUID AND h.REF_DATE=l.REF_DATE").createOrReplaceTempView(
        "prev_plus_crimes")

    # Join previous datafrmae with crime information
    join_prev_crimes = spark.sql("SELECT h.GEO ,h.REF_DATE, h.DGUID, h.uom_houseindex, h.scalar_houseindex, h.house_only,h.land_only, h.total_house_land, h.uom_income, h.scalar_income,h.statistic_income, h.income,\
    h.uom_expenditure,h.scalar_expenditure,h.statistic_expenditure,h.food_expenditures, h.income_taxes, h.mortageinsurance, h.mortagePaid, \
    h.accomodation, h.rent, h.shelter, h.total_expenditure, h.taxes_landregfees,\
    h.uom_tourist, h.scalar_tourist,h.international_tourism, h.domestic_tourism, \
    h.uom_labourforce,h.scalar_labourforce,h.statistic_labourforce,h.employment, h.fulltime, h.labourforce, h.parttime, h.population, h.unemployment,\
    h.uom_lfperc,h.scalar_lfperc,h.statistic_labourforceperc,h.employment_rate,h.participationrate,h.unemployment_rate, \
    c.uom_crime, c.scalar_crime, c.statistic_crime,round(c.crime_incidents/12,2) as crime_incidents \
    FROM prev_plus_crimes h LEFT JOIN crimes c ON h.DGUID = c.DGUID AND SUBSTR(h.REF_DATE , 0, INSTR(h.REF_DATE , '-')-1) = c.REF_DATE").createOrReplaceTempView(
        "prev_plus_cpi")

    # Join previous datafrmae with consumer price index information
    join_prev_cpi = spark.sql("SELECT h.GEO ,h.REF_DATE, h.DGUID, h.uom_houseindex, h.scalar_houseindex, h.house_only,h.land_only, h.total_house_land, h.uom_income, h.scalar_income,h.statistic_income, h.income,\
    h.uom_expenditure,h.scalar_expenditure,h.statistic_expenditure,h.food_expenditures, h.income_taxes, h.mortageinsurance, h.mortagePaid, \
    h.accomodation, h.rent, h.shelter, h.total_expenditure, h.taxes_landregfees,\
    h.uom_tourist, h.scalar_tourist,h.international_tourism, h.domestic_tourism, \
    h.uom_labourforce,h.scalar_labourforce,h.statistic_labourforce,h.employment, h.fulltime, h.labourforce, h.parttime, h.population, h.unemployment,\
    h.uom_lfperc,h.scalar_lfperc,h.statistic_labourforceperc,h.employment_rate,h.participationrate,h.unemployment_rate, \
    h.uom_crime, h.scalar_crime, h.statistic_crime,h.crime_incidents, \
    c.uom_cpi, c.scalar_cpi, c.cpi_index \
    FROM prev_plus_cpi h LEFT JOIN cpi c ON h.REF_DATE = c.REF_DATE").createOrReplaceTempView("prev_plus_oil")

    # Join previous datafrmae with oil information
    join_prev_oil = spark.sql("SELECT h.GEO ,h.REF_DATE, h.DGUID, h.uom_houseindex, h.scalar_houseindex, h.house_only,h.land_only, h.total_house_land, h.uom_income, h.scalar_income,h.statistic_income, h.income,\
    h.uom_expenditure,h.scalar_expenditure,h.statistic_expenditure,h.food_expenditures, h.income_taxes, h.mortageinsurance, h.mortagePaid, \
    h.accomodation, h.rent, h.shelter, h.total_expenditure, h.taxes_landregfees,\
    h.uom_tourist, h.scalar_tourist,h.international_tourism, h.domestic_tourism, \
    h.uom_labourforce,h.scalar_labourforce,h.statistic_labourforce,h.employment, h.fulltime, h.labourforce, h.parttime, h.population, h.unemployment,\
    h.uom_lfperc,h.scalar_lfperc,h.statistic_labourforceperc,h.employment_rate,h.participationrate,h.unemployment_rate, \
    h.uom_crime, h.scalar_crime, h.statistic_crime,h.crime_incidents, \
    h.uom_cpi, h.scalar_cpi, h.cpi_index, \
    o.uom_oil, o.scalar_oil, o.diesel_fillingstations, o.diesel_selfservstations,o.premium_fillingstations,o.premium_selfservstations, o.regular_fillingstations, o.regular_selfservstations  \
    FROM prev_plus_oil h LEFT JOIN oil o ON h.DGUID = o.DGUID AND h.REF_DATE = o.REF_DATE").createOrReplaceTempView(
        "prev_plus_immigration")

    # Join previous datafrmae with immigration information
    prev_plus_immigration = spark.sql("SELECT h.GEO ,h.REF_DATE, h.DGUID, h.uom_houseindex, h.scalar_houseindex, h.house_only,h.land_only, h.total_house_land, h.uom_income, h.scalar_income,h.statistic_income, h.income,\
    h.uom_expenditure,h.scalar_expenditure,h.statistic_expenditure,h.food_expenditures, h.income_taxes, h.mortageinsurance, h.mortagePaid, \
    h.accomodation, h.rent, h.shelter, h.total_expenditure, h.taxes_landregfees,\
    h.uom_tourist, h.scalar_tourist,h.international_tourism, h.domestic_tourism, \
    h.uom_labourforce,h.scalar_labourforce,h.statistic_labourforce,h.employment, h.fulltime, h.labourforce, h.parttime, h.population, h.unemployment,\
    h.uom_lfperc,h.scalar_lfperc,h.statistic_labourforceperc,h.employment_rate,h.participationrate,h.unemployment_rate, \
    h.uom_crime, h.scalar_crime, h.statistic_crime,h.crime_incidents, \
    h.uom_cpi, h.scalar_cpi, h.cpi_index, \
    h.uom_oil, h.scalar_oil, h.diesel_fillingstations, h.diesel_selfservstations,h.premium_fillingstations,h.premium_selfservstations, h.regular_fillingstations, h.regular_selfservstations,  \
    imm1.uom_imm, imm1.scalar_imm ,(imm1.in_migrants + imm2.immigrants - imm1.out_migrants) as immigrants \
    FROM prev_plus_immigration h \
    LEFT JOIN immigration_020 imm1 ON h.DGUID = imm1.DGUID AND h.REF_DATE = imm1.REF_DATE \
    LEFT JOIN immigration_040 imm2 ON h.DGUID = imm2.DGUID AND h.REF_DATE = imm2.REF_DATE").createOrReplaceTempView(
        "prev_plus_mortage")

    # Join previous datafrmae with mortage information
    prev_plus_mortage = spark.sql("SELECT h.GEO ,h.REF_DATE, h.DGUID, h.uom_houseindex, h.scalar_houseindex, h.house_only,h.land_only, h.total_house_land, h.uom_income, h.scalar_income,h.statistic_income, h.income,\
    h.uom_expenditure,h.scalar_expenditure,h.statistic_expenditure,h.food_expenditures, h.income_taxes, h.mortageinsurance, h.mortagePaid, \
    h.accomodation, h.rent, h.shelter, h.total_expenditure, h.taxes_landregfees,\
    h.uom_tourist, h.scalar_tourist,h.international_tourism, h.domestic_tourism, \
    h.uom_labourforce,h.scalar_labourforce,h.statistic_labourforce,h.employment, h.fulltime, h.labourforce, h.parttime, h.population, h.unemployment,\
    h.uom_lfperc,h.scalar_lfperc,h.statistic_labourforceperc,h.employment_rate,h.participationrate,h.unemployment_rate, \
    h.uom_crime, h.scalar_crime, h.statistic_crime,h.crime_incidents, \
    h.uom_cpi, h.scalar_cpi, h.cpi_index, \
    h.uom_oil, h.scalar_oil, h.diesel_fillingstations, h.diesel_selfservstations,h.premium_fillingstations,h.premium_selfservstations, h.regular_fillingstations, h.regular_selfservstations,  \
    h.uom_imm, h.scalar_imm ,h.immigrants, \
    m.1y_fixed_posted, m.2y_bond, m.3y_bond, m.3y_fixed_posted, m.5y_bond, m.5y_fixed_posted, m.7y_bond, m.10y_bond, m.bank, m.overnight, m.overnight_target, m.prime \
    FROM prev_plus_mortage h \
    LEFT JOIN mortage_info m ON h.REF_DATE = m.REF_DATE").createOrReplaceTempView("prev_plus_weather")

    # Join previous datafrmae with weather information
    prev_plus_weather = spark.sql("SELECT h.GEO ,h.REF_DATE, h.DGUID, h.uom_houseindex, h.scalar_houseindex, h.house_only,h.land_only, h.total_house_land, h.uom_income, h.scalar_income,h.statistic_income, h.income,\
    h.uom_expenditure,h.scalar_expenditure,h.statistic_expenditure,h.food_expenditures, h.income_taxes, h.mortageinsurance, h.mortagePaid, \
    h.accomodation, h.rent, h.shelter, h.total_expenditure, h.taxes_landregfees,\
    h.uom_tourist, h.scalar_tourist,h.international_tourism, h.domestic_tourism, \
    h.uom_labourforce,h.scalar_labourforce,h.statistic_labourforce,h.employment, h.fulltime, h.labourforce, h.parttime, h.population, h.unemployment,\
    h.uom_lfperc,h.scalar_lfperc,h.statistic_labourforceperc,h.employment_rate,h.participationrate,h.unemployment_rate, \
    h.uom_crime, h.scalar_crime, h.statistic_crime,h.crime_incidents, \
    h.uom_cpi, h.scalar_cpi, h.cpi_index, \
    h.uom_oil, h.scalar_oil, h.diesel_fillingstations, h.diesel_selfservstations,h.premium_fillingstations,h.premium_selfservstations, h.regular_fillingstations, h.regular_selfservstations,  \
    h.uom_imm, h.scalar_imm ,h.immigrants, \
    h.1y_fixed_posted, h.2y_bond, h.3y_bond, h.3y_fixed_posted, h.5y_bond, h.5y_fixed_posted, h.7y_bond, h.10y_bond, h.bank, h.overnight, h.overnight_target, h.prime, \
    w.Mean_Max_Temp,w.Mean_Min_Temp, w.Mean_Temp,w.Total_Rain,w.Total_Snow\
    FROM prev_plus_weather h \
    LEFT JOIN weather_data w ON h.REF_DATE = w.REF_DATE AND h.DGUID = w.DGUID").repartition(1)
    prev_plus_weather.write.csv("data", mode='overwrite', header='true')

    print(prev_plus_weather.count())


if __name__ == '__main__':
    # type_ = sys.argv[1]
    # output = sys.argv[2]
    main()