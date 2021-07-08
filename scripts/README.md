# Catalog Scripts

This is a script that shows the `n` most up-to-date tasks

## Setup

In an apt-based Linux distro, type the below commands to install the script dependency:

```.bash
wget http://initd.org/psycopg/tarballs/PSYCOPG-2-6/psycopg2-2.6.tar.gz
tar -vzxf psycopg2-2.6.tar.gz
cd psycopg2-2.6
python3 setup.py install
sudo apt-get install python3-psycopg2
```

## How to use

```.bash
./catalog.py <catalog_address> <catalog_port> <catalog_db_name> <catalog_user_name> <catalog_user_password>
```

By default it will show the 10 most up-to-date tasks, but you can also specify the number of tasks to show:

```.bash
./catalog.py <catalog_address> <catalog_port> <catalog_db_name> <catalog_user_name> <catalog_user_password> -n <Number of tasks to show>
```
