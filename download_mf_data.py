""" Download Mutual Fund data from amfindia.

imports :
    sql_parser - The sql_parser contains the information of the driver,database
    and the tables

  Typical usage example:

  mf = MutualFundsDownload()
"""



import datetime
import pandas as pd
from sql_parser import sql_parser

class MutualFundsDownload:
    """ MutualFundsDownload is the only class in this module.
        It contains functions that is used to download the data from amfindia.
    """

    def __init__(self):
        """ intializes the sql engine,mf download url,
            N.A values in dataframe and count is check the progress of number of days the data is downloaded for.

            Args:
                N.A
            Returns:
                N.A
            Raises:
                N.A
        """
        self.engine = sql_parser.engine()
        self.__mf_data_url = 'http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt='
        self.na = ['N.A.']
        self.__count = 0

    def download_mutual_fund_data(self, days):
        """ downloads mutual fund data from amfindia.
            The function checks whether we already have the data in our database for the cob and
            calls download_data function which stores the data in the table if the data doesn't exists.
            This table "sql_parser.mutual_funds" would contain important details like
            nav value of all the funds.The download_mutual_fund_data needs to be run on T+1
            to download the data for previous business day.


            Args:
                days : Last number of days the data is to be downloaded for..

            Returns:
                N.A

            Raises:
                N.A
        """

        start_date = datetime.date.today() - datetime.timedelta(days=days)
        end_date = datetime.date.today()
        fetch_existing_mf_dates = ''' select distinct [business_date] from {} '''.format(sql_parser.mutual_funds)
        existing_mf_dates = pd.read_sql(fetch_existing_mf_dates, self.engine)
        existing_mf_dates_list = existing_mf_dates['business_date'].dt.date.tolist()

        # Append the data for the dates where the data for mutual funds is missing.
        while start_date != end_date:
            if start_date.weekday() != 5 and start_date.weekday() != 6 and start_date not in existing_mf_dates_list:
                self.delete_data(sql_parser.mutual_funds, start_date)
                self.download_data(start_date)
                start_date = start_date + datetime.timedelta(days=1)
                self.__count += 1
            else:
                start_date = start_date + datetime.timedelta(days=1)
                self.__count += 1
                print(self.__count)

    def download_scheme_details(self):
        """ The download_mutual_fund_data function doesn't provide the scheme details like the scheme_category,scheme_type etc.
            This function will download the scheme details and store it in table sql_parser.mutual_funds_scheme.
            Unlike download_mutual_fund_data , The download_scheme_details needs to be run only once.

            Args:
                N.A

            Returns:
                N.A

            Raises:
                N.A
        """
        scheme_data = pd.read_csv('https://portal.amfiindia.com/DownloadSchemeData_Po.aspx?mf=0')
        scheme_data.columns = scheme_data.columns.str.replace(' ', '_').str.lower()
        get_existing_schemes = '''select code from {}'''.format(sql_parser.mutual_funds_scheme)
        existing_scheme_codes = pd.read_sql(get_existing_schemes, self.engine)
        existing_scheme_codes = existing_scheme_codes["code"].to_list()
        filt_schemes = scheme_data['code'].isin(existing_scheme_codes)
        add_scheme_data = scheme_data.loc[~filt_schemes]
        add_scheme_data['launch_date'] = pd.to_datetime(add_scheme_data['launch_date'], format='%d-%b-%Y')
        add_scheme_data['_closure_date'] = pd.to_datetime(add_scheme_data['_closure_date'], format='%d-%b-%Y')
        add_scheme_data.to_sql(sql_parser.mutual_funds_scheme, self.engine, index=False, if_exists='append')

    def delete_data(self, table_name, day):
        """ deletes the data for a specific day from a table.

            Args:
                table_name - the table from which the data needs to be deleted.
                day - the date the data needs to be deleted for.

            Returns:
                N.A

            Raises:
                N.A
        """
        print("Deleting data for day {}".format(day))
        delete_business_data = ''' delete from {} where business_date in ('{}') '''.format(table_name, day)
        self.engine.execute(delete_business_data)

    def delete_data_from(self, table_name, day):
        """ deletes the data starting from a specific day.

            Args:
                table_name - the table from which the data needs to be deleted.
                day - the date from which the data needs to be deleted from

            Returns:
                N.A

            Raises:
                N.A
        """
        print("Running delete_data_from :Deleting data from day {}".format(day))
        delete_business_data = ''' delete from {} where business_date > '{}' '''.format(table_name, day)
        self.engine.execute(delete_business_data)

    def download_data(self, day):
        """ download the data starting from a specific day.

            Args:
                day - the date the data needs to be downloaed from

            Returns:
                N.A

            Raises:
                N.A
        """
        print("Downloading  data for day {}".format(day))
        download_mf_data_url = self.__mf_data_url + str(day)
        mf_data_frame = pd.read_csv(download_mf_data_url, sep=';', na_values=self.na)
        mf_data_frame.dropna(axis='index', how='any', subset=['Scheme Name', 'Net Asset Value'],
                                  inplace=True)
        mf_data_frame['Date'] = pd.to_datetime(mf_data_frame['Date'], format='%d-%b-%Y')
        mf_data_frame['Scheme Code'] = mf_data_frame['Scheme Code'].astype(int)
        mf_data_frame['Repurchase Price'] = mf_data_frame['Repurchase Price'].replace(',', '',
                                                                                                regex=True).astype(
            float)
        mf_data_frame['Net Asset Value'] = mf_data_frame['Net Asset Value'].replace(',', '',
                                                                                              regex=True).astype(
            float)
        mf_data_frame['Sale Price'] = mf_data_frame['Sale Price'].replace(',', '', regex=True).astype(
            float)
        mf_data_frame.rename(
            columns={'Net Asset Value': 'nav', 'Date': 'business_date', 'Scheme Name': 'scheme_nav_name'}, inplace=True)
        mf_data_frame.columns = [x.lower() for x in mf_data_frame.columns.str.replace(' ', '_')]
        mf_data_frame.to_sql(sql_parser.mutual_funds, self.engine, index=False, if_exists='append')

    def check_data_quality(self, days):
        """ -- Needs more improvement.
            Checks the quality of the data present in sql_parser.mutual_funds.
            This is required as many times,the quality of data retrieve from amfindia is not good.
            Basically function gets the mean of last 22 days
            and checks whether the count of the the data is above the (mean - 1000).
            It flags quality issue for all the cobs if it doesn't fulfill the criterial of (mean - 1000)

            Args:
                days - the days the quality issue needs to be checked for.

            Returns:
                N.A

            Raises:
                N.A
        """
        mean_days = datetime.date.today() - datetime.timedelta(days=days)
        print(mean_days)
        self.delete_data_from(sql_parser.mf_quality_issues, mean_days)
        business_query = '''select business_date,count(*)  as "counts"
        from {}
        where business_date > '{}'
        group by business_date
        order by business_date desc
         '''.format(sql_parser.mutual_funds,mean_days)
        df = pd.read_sql(business_query, self.engine)
        mean_22_days = df.groupby(df.index // 22).mean()
        df.index = df.index // 22
        df['mean_22_days'] = mean_22_days
        filt = df['counts'] < (df['mean_22_days'] - 1000)
        quality_issues = df.loc[filt]['business_date'].to_list()
        df['quality_issues'] = df['business_date'].apply(lambda x: 'Y' if x in quality_issues else 'N')
        df.set_index('business_date', inplace=True)
        df.to_sql(sql_parser.mf_quality_issues, self.engine, if_exists='append')



if __name__ == "__main__":
    mf = MutualFundsDownload()
    mf.download_mutual_fund_data(15)
    mf.download_scheme_details()
    mf.check_data_quality(22)

