import pandas as pd
import datetime
import sqlalchemy as sql
import pyodbc
import configparser
from sql_parser import sql_parser


class MutualFundsDownload:

    def __init__(self):
        self.engine = sql_parser.engine()
        self.__mf_data_url = 'http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt='
        self.__count = 0

    def download_mutual_fund_data(self, days):
        ''' Downloads the data from amfindia for the number of days supplied in argument '''
        self.download_days = days
        self.start_date = datetime.date.today() - datetime.timedelta(days=self.download_days)
        self.end_date = datetime.date.today()
        self.mf_data_frame = pd.DataFrame()
        self.na = ['N.A.']

        #Retrieve the dates where we have the data for mutual funds.

        self.fetch_existing_mf_dates = ''' select distinct [business_date] from {} '''.format(sql_parser.mutual_funds)
        self.existing_mf_dates = pd.read_sql(self.fetch_existing_mf_dates, self.engine)
        self.existing_mf_dates_list = self.existing_mf_dates['business_date'].dt.date.tolist()

        #Append the data for the dates where the data for mutual funds is missing.
        while self.start_date != self.end_date:
            if self.start_date.weekday() != 5 and self.start_date.weekday() != 6 and self.start_date not in self.existing_mf_dates_list:
                self.download_mf_data_url = self.__mf_data_url + str(self.start_date)
                self.start_date = self.start_date + datetime.timedelta(days=1)
                self.mf_data_frame = pd.read_csv(self.download_mf_data_url, sep=';', na_values=self.na)
                print(self.download_mf_data_url)
                self.mf_data_frame.dropna(axis='index', how='any', subset=['Scheme Name', 'Net Asset Value'],
                                          inplace=True)
                self.mf_data_frame['Date'] = pd.to_datetime(self.mf_data_frame['Date'], format='%d-%b-%Y')
                self.mf_data_frame['Scheme Code'] = self.mf_data_frame['Scheme Code'].astype(int)
                self.mf_data_frame['Repurchase Price'] = self.mf_data_frame['Repurchase Price'].replace(',', '',
                                                                                                        regex=True).astype(
                    float)
                self.mf_data_frame['Net Asset Value'] = self.mf_data_frame['Net Asset Value'].replace(',', '',
                                                                                                      regex=True).astype(
                    float)
                self.mf_data_frame['Sale Price'] = self.mf_data_frame['Sale Price'].replace(',', '', regex=True).astype(
                    float)
                self.mf_data_frame.rename(columns={'Net Asset Value': 'nav', 'Date': 'business_date','scheme_name':'scheme_nav_name'}, inplace=True)
                self.mf_data_frame.columns = [x.lower() for x in self.mf_data_frame.columns.str.replace(' ', '_')]
                print(self.start_date)
                self.mf_data_frame.to_sql(sql_parser.mutual_funds, self.engine, index=False, if_exists='append')
                self.__count += 1
                print(self.__count)
            else:
                self.start_date = self.start_date + datetime.timedelta(days=1)
                self.__count += 1
                print(self.__count)

    def download_scheme_details(self):
        self.scheme_data = pd.read_csv('https://portal.amfiindia.com/DownloadSchemeData_Po.aspx?mf=0')
        self.scheme_data.columns = self.scheme_data.columns.str.replace(' ', '_').str.lower()
        self.get_existing_schemes = '''select code from {}'''.format(sql_parser.mutual_funds_scheme)
        self.existing_scheme_codes = pd.read_sql(self.get_existing_schemes, self.engine)
        self.existing_scheme_codes = self.existing_scheme_codes["code"].to_list()
        self.filt_schemes = self.scheme_data['code'].isin(self.existing_scheme_codes)
        self.add_scheme_data = self.scheme_data.loc[~self.filt_schemes]
        self.add_scheme_data.to_sql(sql_parser.mutual_funds_scheme, self.engine, index=False, if_exists='append')


if __name__ == "__main__":
     mf = MutualFundsDownload()
     mf.download_mutual_fund_data(15)
     mf.download_scheme_details()
