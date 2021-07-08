# Catalog Scripts

This is a script that shows the `n` most up-to-date tasks

## How to use

```.bash
./catalog.py <catalog_address> <catalog_port> <catalog_db_name> <catalog_user_name> <catalog_user_password>
```

By default it will show the 10 most up-to-date tasks, but you can also specify the number of tasks to show:

```.bash
./catalog.py <catalog_address> <catalog_port> <catalog_db_name> <catalog_user_name> <catalog_user_password> -n <Number of tasks to show>
```
