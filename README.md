# housepriceprediction


Overview of execution workflow.
Download code and change the directory to Code_no_agg folder.
1. Extract and generate consolidated file per province and year-month of all features. This can be done by running the following command:
    - ${SPARK_HOME}/bin/spark-submit --num-executors=8 loadData.py
	- After this is done, rename the output file generated under Code_no_agg/data/part-<number>.csv to data.csv


2. Take the file generated in step 1 and use it to make the predictions and apply the different prediction algorithms

Change the directory to be at the root level of the project and then run.
    - python3 prediction_algos/housing_price_prediction.py
    - python3 prediction_algos/housing_price_prediction_lstm.py

3. Run web crawler to retrieve listing prices.
Before running this section make sure to install scrapy package.
    -  scrapy runspider ListingPrices/p2h.py -o ListingPrices/part1.csv
       Do this by changing startURLs for all the URLs commented  after the startURL variable. One file for each URL will be
       obtained.
    - After getting the part files, run ListingPrices/combine.py which will combine the part files into one  csv. Change the    'ipdir' path to the path where all the part files have been saved. Command:
       python3 combine.py

    -  Command for running clean.py:
       spark-submit ListingPrices/clean.py ListingPrices/listingdata.csv  ListingPrices/exp_loc.py ListingPricesnewlistdata1.csv
       (Listing data.csv is a sample dataset obtained after combining the part files of data scraped)
    -  Command for running all other codes:
       spark-submit ListingPrices/<filename>.py  ListingPrices/newlistdata1.csv
       (newlistdata1 is a sample dataset obtained after combining and cleaning)

4. Visualize in tableau. Output tableau worksheet is called Main-WB.twb
https://10az.online.tableau.com/#/site/housepricing/workbooks/1474770/views

