# MFIndia

The MFIndia comparision dashboard is used to compare the returns of the selected mutual funds under a particular scheme_category.

![alt text](https://github.com/SarveshShetty19/MFIndia/blob/f31d03928d501dc6b011f6011fb2831357d56d36/MFIndia_Dashboard.jpeg)

How to use ?

You will need to create a configuration file "server_config.ini" with the database drive details and the table details [Template available - server_config_template.ini]

There are 3 main tables.

**Mutual_Funds** - This is the main table which contains the nav of all the mutual funds

**Mutual_Funds_Scheme** - This is the table which contains the scheme details of the mutual funds like scheme_type (eg . Open-Ended),scheme_category (eg . ELSS) etc.

**Mutual_Funds_Quality** - For Lot of business_dates there is only partial data available in amfindia. This table is specifically created to flag quality issues for the dates where the count of the data is very low.

Once the configuration file is set up you first need to run the script **download_mf_data.py** to download the data from amfindia and populate the above tables.

You can then run **mf_dashboard.py** to populate the dashboard.

Scripts Details :-

**sql_parser** - basically used to parse the database information from server_config_template.ini.

**get_mf_details.py** - This script is used to get the performance metrics for mutual funds using the data stored in the database.

**api_mf.py** - This script contains the api for getting the performance of mutual funds.
