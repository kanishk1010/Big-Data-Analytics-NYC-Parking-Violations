# Big-Data-Analytics-NYC-Parking-Violations


These scripts are written for Apache Spark and python to analyze different aspects of parking violations in NYC. 

The input data used is open and available from NYC Open Data at:
https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2016/kiv2-tbus/data https://data.cityofnewyork.us/City-Government/Open-Parking-and-Camera-Violations/nc67-uf89

Task 1:
Write a map-reduce job that finds all parking violations that have been paid, i.e., that do not occur in open-violations.csv

Task 2:
Write a map-reduce job that finds the distribution of the violation types, i.e., for each violation code, the number of violations that have this code.

Task 3:
Write a map-reduce job that finds the total and average amount due in open violations for each license type.

Task 4:
Write a Spark program that computes the total number of violations for vehicles registered in the state of NY and all other vehicles.

Task 5:
Write a Spark program that finds the vehicle that has had the greatest number of violations (assume that plate_id and registration_state uniquely identify a vehicle)

Task 6:
Write a Spark program that finds the top-20 vehicles in terms of total violations (assume that plate id and
registration state uniquely identify a vehicle).

Task 7:
Write a Spark program that, for each violation code, lists the average number of violations with that code issued per day on weekdays and weekend days.
