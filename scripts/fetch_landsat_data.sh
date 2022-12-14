#!/bin/bash
DIRNAME=`dirname $0`
cd $DIRNAME/.

# Update these fields if necessary
catalog_passwd='catalog_passwd'
catalog_db_name='catalog_db_name' 
catalog_user='catalog_user'

curl 'https://storage.googleapis.com/gcp-public-data-landsat/index.csv.gz' --output index.csv.gz
gunzip index.csv.gz

input_csv='index.csv'
landsat_output='landsat_images_data.csv'
landsat_db_keys='landsat_db_keys.csv'

cut -d, -f2,17 $input_csv | awk -v db_output="$landsat_db_keys" -v lan_output="$landsat_output" -F"," '{
    if($1 != "PRODUCT_ID") {
        split($1,pid,"_");
        print (pid[3]pid[4] "," $1 "," $2) >> lan_output

        if (cnt[(pid[3]pid[4])] < 1) {
            print (pid[3]pid[4]) >> db_output
        }
        cnt[(pid[3]pid[4])] ++
    } else {
        print "LANDSAT_KEY" >> db_output
        print "LANDSAT_KEY,PRODUCT_ID,TOTAL_SIZE" >> lan_output
    }
}'

PGPASSWORD=$catalog_passwd psql -h localhost -p 5432 $catalog_db_name $catalog_user -c "\copy landsat_images(LANDSAT_KEY) FROM '${landsat_db_keys}' DELIMITER ',' CSV HEADER;"

rm $input_csv
rm $landsat_db_keys