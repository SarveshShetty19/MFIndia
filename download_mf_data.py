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
        self.na = ['N.A.']
        self.__count = 0

    def download_mutual_fund_data(self, days):
        ''' Downloads the data from amfindia for the number of days supplied in argument '''
        self.download_days = days
        self.start_date = datetime.date.today() - datetime.timedelta(days=self.download_days)
        self.end_date = datetime.date.today()
        self.mf_data_frame = pd.DataFrame()


        #Retrieve the dates where we have the data for mutual funds.

        self.fetch_existing_mf_dates = ''' select distinct [business_date] from {} '''.format(sql_parser.mutual_funds)
        self.existing_mf_dates = pd.read_sql(self.fetch_existing_mf_dates, self.engine)
        self.existing_mf_dates_list = self.existing_mf_dates['business_date'].dt.date.tolist()

        #Append the data for the dates where the data for mutual funds is missing.
        while self.start_date != self.end_date:
            if self.start_date.weekday() != 5 and self.start_date.weekday() != 6 and self.start_date not in self.existing_mf_dates_list:
                self.delete_data(sql_parser.mutual_funds,self.start_date)
                self.download_data(self.start_date)
                self.start_date = self.start_date + datetime.timedelta(days=1)
                self.__count += 1
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
        self.add_scheme_data['launch_date'] = pd.to_datetime(self.add_scheme_data['launch_date'], format='%d-%b-%Y')
        self.add_scheme_data['_closure_date'] = pd.to_datetime(self.add_scheme_data['_closure_date'], format='%d-%b-%Y')
        self.add_scheme_data.to_sql(sql_parser.mutual_funds_scheme, self.engine, index=False, if_exists='append')

    def delete_data(self,table_name,day):
        print("Deleting data for day {}".format(day))
        self.delete_data = ''' delete from {} where business_date in ('{}') '''.format(table_name,day)
        self.engine.execute(self.delete_data)

    def delete_data_from(self,table_name,day):
        print("Deleting data from day {}".format(day))
        self.delete_data = ''' delete from {} where business_date > '{}' '''.format(table_name,day)
        self.engine.execute(self.delete_data)


    def download_data(self,day):
        self.day = day
        print("Downloading  data for day {}".format(self.day))
        self.download_mf_data_url = self.__mf_data_url + str(self.day)
        self.mf_data_frame = pd.read_csv(self.download_mf_data_url, sep=';', na_values=self.na)
        self.mf_data_frame = pd.read_csv(self.download_mf_data_url, sep=';', na_values=self.na)
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
        self.mf_data_frame.rename(
            columns={'Net Asset Value': 'nav', 'Date': 'business_date', 'Scheme Name': 'scheme_nav_name'}, inplace=True)
        self.mf_data_frame.columns = [x.lower() for x in self.mf_data_frame.columns.str.replace(' ', '_')]
        self.mf_data_frame.to_sql(sql_parser.mutual_funds, self.engine, index=False, if_exists='append')

    def check_data_quality(self,days):
        self.mean_days = datetime.date.today() - datetime.timedelta(days=days)
        self.delete_data_from(sql_parser.mf_quality_issues, self.mean_days)
        business_query = '''select business_date,count(*)  as "counts"
        from mf_india
        where business_date > '{}'
        group by business_date
        order by business_date desc
         '''.format(self.mean_days)
        self.df = pd.read_sql(business_query, self.engine)
        self.mean_22_days = self.df.groupby(self.df.index // 22).agg(['mean'])
        self.df.index = self.df.index // 22
        self.df['mean_22_days'] = self.mean_22_days
        self.filt = self.df['counts'] < (self.df['mean_22_days'] - 1000)
        self.quality_issues = self.df.loc[self.filt]['business_date'].to_list()
        self.df['quality_issues'] = self.df['business_date'].apply(lambda x: 'Y' if x in self.quality_issues else 'N')
        self.df.set_index('business_date', inplace=True)
        self.df.to_sql(sql_parser.mf_quality_issues, self.engine, if_exists='append')




if __name__ == "__main__":
     mf = MutualFundsDownload()
     mf.download_mutual_fund_data(15)
     mf.download_scheme_details()
     #mf.download_data('19-Aug-2021')
     mf.check_data_quality(30)
