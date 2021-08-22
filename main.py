import datetime

business_date = datetime.date.today() - datetime.timedelta(days=15)

print(business_date.strftime("%d-%b-%Y"))