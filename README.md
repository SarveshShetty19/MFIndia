# MFIndia

The MFIndia comparision dashboard is used to compare the returns of the selected mutual funds under a particular scheme_category.

![alt text](https://github.com/SarveshShetty19/MFIndia/blob/050f84a519ef1d04ad38c37ce5ad8ff58ba97e8a/Web%20capture_13-9-2021_23111_127.0.0.1.jpeg)

The data is downloaded in  database server using **download_mf_data.py** .
You need to create a new file named server_config.ini ( There is a template already server_config_template.ini)

There are 3 main tables.

**Mutual_Funds** - This is the main table which contains the nav of all the mutual funds

**Mutual_Funds_Scheme** - This is the table which contains the scheme details of the mutual funds

**Mutual_Funds_Quality** - This table flags the business_dates which has quality issues.


Scripts :-

**sql_parser** - basically used to parse the database information from server_config_template.ini.

**get_mf_details.py** - This script is used to get the performance metrics for mutual funds using the data stored in the database.

**api_mf.py** - This script contains the api for getting the performance of mutual funds.

**mf_dashboard.py** - This script is used to populate the dashboard using plotly and dash.





