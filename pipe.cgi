#!/bin/bash

echo "Content-Type: text/html\n\n"
echo

#getting the name of the file to be loaded into database
filename=$(perl filename.pl)

#creating a directory to store the intermediate text files
mkdir data

perl dataDump.pl $filename data

#creating the database tables and setting constraints as per the schema design
perl dbSchema.pl

#finally loading the data from text files into database
perl load_orders.pl data
