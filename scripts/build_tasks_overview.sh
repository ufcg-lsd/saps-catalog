#!/bin/bash

DIRNAME=`dirname $0`
cd $DIRNAME/..

date=$(date  --date="yesterday" +"%Y-%m-%d")

out_tasks_general_path="./data/out-tasks/general"
out_tasks_days_path="./data/out-tasks/days"
dump_postgres_path="./data/dump-postgres"
dump_landsat_path="./data/dump-landsat-years"
dump_postgres_filepath="${dump_postgres_path}/tasks-${date}.csv"
general_task_raw_filepath="${out_tasks_general_path}/task_raw_data.csv"
general_task_overview_filepath="${out_tasks_general_path}/task_overview_data.csv"
days_task_raw_filepath="${out_tasks_days_path}/task-info-raw-${date}.csv"
days_task_overview_filepath="${out_tasks_days_path}/task-info-overview-${date}.csv"
generate_days_out=0 # (False - 0 | True - 1)

if [ $generate_days_out -eq 0 ]; then
        mkdir -p $out_tasks_days_path
        touch $days_task_raw_filepath
        touch $days_task_overview_filepath
fi

mkdir -p $dump_postgres_path
mkdir -p $out_tasks_general_path
touch $general_task_raw_filepath
touch $general_task_overview_filepath

PGPASSWORD=catalog_passwd psql -h localhost -p 5432 catalog_db_name catalog_user  -c "\copy (SELECT * FROM tasks WHERE CAST(updated_time AS VARCHAR) LIKE '${date}%') to '${dump_postgres_filepath}' with csv;"
python3 get_tasks_stats.py $dump_landsat_path $dump_postgres_filepath $general_task_raw_filepath $general_task_overview_filepath $days_task_raw_filepath $days_task_overview_filepath $generate_days_out

cp $general_task_raw_filepath  /home/ubuntu/saps-dispatcher/stats/tasks_raw_data.csv 
cp $general_task_overview_filepath /home/ubuntu/saps-dispatcher/stats/tasks_overview_data.csv

rm -rf dump_postgres_path
rm -rf out_tasks_general_path
