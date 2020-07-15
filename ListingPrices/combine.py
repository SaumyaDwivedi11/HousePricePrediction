

#combining csv files into one csv file
import os
import glob
import csv

files =glob.glob('ListingPrices/csvs/*.csv')
out = csv.writer(open('ListingPrices/listingdata.csv', 'w'), delimiter = ',')
#out.writerow(['province', 'listprice', 'date added', 'locality', 'postal code',
         #'year built', 'taxes', 'Basement', 'Lot Size', 'Bed', 'Baths', 'Area'])

for file in files:
    print(file)
    read_csv = csv.reader(open(file, 'r'), delimiter = ',')
    next(read_csv)
    for row in read_csv:
        #print(row)
        out.writerow(row)




