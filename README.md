# Προχωρημένα Θέματα Βάσεων Δεδομένων
Για να τρέξουμε τα scripts, πρέπει πρώτα να είναι στημένη η πλατφόρμα σύμφωνα με τον οδηγό, έπειτα πρέπει να περασούν στο hdfs τα εξής csv με τα δεδομένα:
LAPD_Police_Stations.csv,
LA_income_2015.csv,
LA_income_2017.csv,
LA_income_2019.csv,
LA_income_2021.csv,
crime-data-2010-to-2019.csv,
crime-data-2020-to-present.csv,
revgecoding.csv.

Η εντολή που χρησιμοποιήσαμε είναι η: hadoop fs -copyFromLocal file
Έπειτα για τρέξουμε το script.py στο directory PATH με 4 executors πρέπει να τρέξουμε την εντολή:
spark-submit --master yarn --num-executors 4 PATH/script.py 
