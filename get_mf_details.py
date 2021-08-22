import pandas as pd
import datetime
import sqlalchemy as sql
import pyodbc
from sql_parser import sql_parser
import datetime
# BDay is business day, not birthday...
from pandas.tseries.offsets import BDay
from functools import reduce

today = datetime.date.today()
business_date = (today - BDay()).strftime('%Y-%m-%d')


class MutualFunds:

    def __init__(self):
        self.engine = sql_parser.engine()
        self.today = (datetime.date.today() - BDay())
        self.initalize_business_days(self.today)

    def initalize_business_days(self, today):
        self.fetch_existing_mf_dates = ''' select distinct [business_date] from {} '''.format(sql_parser.mutual_funds)
        self.existing_mf_dates = pd.read_sql(self.fetch_existing_mf_dates, self.engine)
        self.existing_mf_dates_list = self.existing_mf_dates['business_date'].dt.date.tolist()
        self.business_date = today

        self.business_date_1yrs = self.check_business_date(today - BDay(262))
        self.business_date_2yrs = self.check_business_date(today - BDay(524))
        self.business_date_3yrs = self.check_business_date(today - BDay(786))
        self.business_date_4yrs = self.check_business_date(today - BDay(1048))
        self.business_date_5yrs = self.check_business_date(today - BDay(1310))


        self.business_days = [self.business_date,self.business_date_1yrs,self.business_date_2yrs,self.business_date_3yrs,\
                              self.business_date_4yrs,self.business_date_5yrs]

        self.business_date, self.business_date_1yrs, self.business_date_2yrs,self.business_date_3yrs, \
        self.business_date_4yrs, self.business_date_5yrs = map(lambda x: x.strftime('%Y-%m-%d') ,self.business_days)




    def check_business_date(self,business_date):
        while business_date not in self.existing_mf_dates_list:
            business_date = (business_date - BDay(2))
        return business_date

    def convert_date_to_string(self,business_date):
        return business_date.strftime('%Y-%m-%d')

    def get_scheme_metrics(self, mutual_fund_list=None):
        ''' Gives performance metrics for the given mutual funds '''
        self.mutual_fund_list = mutual_fund_list
        self.scheme_details = self.get_mf_history(self.mutual_fund_list)
        self.scheme_performance = self.perform_calculation(self.scheme_details)
        self.scheme_performance = self.add_scheme_details(self.scheme_performance)
        self.scheme_performance = self.refine_columns(self.scheme_performance)
        return self.scheme_performance

    def get_all_metrics(self, sort_field, numcount=5):

        ''' Gives perfroamnce metrics for all mutual funds limited to the numcount value
        and 3 business days i.e today,3 years ago and 5 years ago'''

        self.get_all_mf_performance = '''select business_date,nav,scheme_nav_name,scheme_code from {} where business_date in ({})'''.format(
            sql_parser.mutual_funds,
            ",".join("?" for _ in self.business_days))
        self.all_scheme_performance = pd.read_sql(self.get_all_mf_performance, self.engine, params=self.business_days)
        self.all_scheme_performance_calc = self.perform_calculation(self.all_scheme_performance)
        self.all_scheme_performance_calc = self.add_scheme_details(self.all_scheme_performance_calc)
        self.all_scheme_performance_calc = self.refine_columns(self.all_scheme_performance_calc)

        return self.all_scheme_performance_calc.sort_values(by=sort_field,
                                                            ascending=False) if numcount == 'max' else self.all_scheme_performance_calc.sort_values(
            by=sort_field, ascending=False).head(numcount)

    def get_all_metrics_by_scheme_category(self,scheme_category,sort_field,numcount=5):
        self.all_metrics = self.get_all_metrics('cagr(5yrs)', 'max')
        self.filt = self.all_metrics['scheme_category'] == scheme_category
        self.all_metrics = self.all_metrics.loc[self.filt]
        return self.all_metrics.sort_values(by=sort_field, ascending=False).head(numcount)

    def get_all_metrics_by_scheme_type(self, scheme_type, sort_field, numcount=5):
        self.all_metrics = self.get_all_metrics('cagr(5yrs)', 'max')
        self.filt = self.all_metrics['scheme_type'] == scheme_type
        self.all_metrics = self.all_metrics.loc[self.filt]
        return self.all_metrics.sort_values(by=sort_field, ascending=False).head(numcount)

    def get_mf_history(self, mutual_fund_list=None):

        ''' Gives the historical data for the given mutual funds  '''

        self.mutual_fund_list = mutual_fund_list
        self.get_scheme_code = '''select code from {} where scheme_nav_name in ({}) '''.format(
            sql_parser.mutual_funds_scheme,
            ",".join(['?' for _ in self.mutual_fund_list]))
        self.scheme_code = pd.read_sql(self.get_scheme_code, self.engine, params=[self.mutual_fund_list])
        self.scheme_code = self.scheme_code.loc[:, 'code'].tolist()
        self.get_scheme_details = ''' select business_date,nav,scheme_nav_name,scheme_code from {} where scheme_code in ({})'''.format(
            sql_parser.mutual_funds,
            ",".join(['?' for _ in self.scheme_code]))
        self.scheme_details = pd.read_sql(self.get_scheme_details, self.engine, params=[self.scheme_code])
        return self.scheme_details

    def get_mf_history_with_scheme_details(self, mutual_fund_list=None):
        self.mutual_fund_list = mutual_fund_list
        mf_history = self.get_mf_history(mutual_fund_list)
        mf_with_scheme_details = self.add_scheme_details(mf_history)
        mf_columns = ['amc', 'scheme_code', 'scheme_nav_name', 'scheme_type', 'scheme_category', 'business_date', 'nav',
                      'scheme_minimum_amount', 'launch_date', '_closure_date']

        return mf_with_scheme_details.loc[:, mf_columns]

    def filter_on_business_date(self, df, business_date):
        ''' filters a dataframe for a particular business_date '''
        return df.loc[df['business_date'] == business_date]

    def get_cagr(self, current_nav, hist_nav, years):
        ''' retyrns cagr '''
        return (current_nav / hist_nav) ** years - 1

    def perform_calculation(self, scheme_dataframe):
        ''' This function is used to calculate cagr over 3 business days i.e today,3 years ago and 5 years ago '''

        # Filter out the dataframe on 3 business day i.e today,3 years ago and 5 years ago
        self.todays_scheme_details = pd.DataFrame()
        self.scheme_details_one_years_ago = pd.DataFrame()
        self.scheme_details_two_years_ago = pd.DataFrame()
        self.scheme_details_three_years_ago = pd.DataFrame()
        self.scheme_details_four_years_ago = pd.DataFrame()
        self.scheme_details_five_years_ago = pd.DataFrame()
        self.todays_scheme_details, self.scheme_details_one_years_ago, self.scheme_details_two_years_ago, self.scheme_details_three_years_ago,self.scheme_details_four_years_ago, self.scheme_details_five_years_ago = map(lambda business_time: self.filter_on_business_date(scheme_dataframe,business_time), self.business_days)

        self.dataframes = [self.todays_scheme_details,self.scheme_details_one_years_ago,self.scheme_details_two_years_ago,self.scheme_details_three_years_ago,self.scheme_details_four_years_ago,self.scheme_details_five_years_ago]
       # self.todays_scheme_details, self.scheme_details_one_years_ago, self.scheme_details_two_years_ago, self.scheme_details_three_years_ago,self.scheme_details_four_years_ago, self.scheme_details_five_years_ago = map(lambda suffix_dataframe,suffix: self.add_suffix(suffix_dataframe,suffix), self.dataframes,range(len(self.dataframes)))


        # Merge dataframe to get current_nav,three_years_ago_nav and five_years_ago_nav

        self.scheme_performance =  self.merge_dataframes(self.dataframes)

        print(self.scheme_performance)
        print(self.scheme_performance.columns)

        self.scheme_performance.rename(columns={'nav_0yrs': 'nav', 'business_date_0yrs': 'business_date'}, inplace=True)



        self.scheme_performance['cagr(1yrs)'] = self.get_cagr(self.scheme_performance['nav'],
                                                              self.scheme_performance['nav_1yrs'], 1)

        self.scheme_performance['cagr(2yrs)'] = self.get_cagr(self.scheme_performance['nav'],
                                                              self.scheme_performance['nav_2yrs'], 2)

        self.scheme_performance['cagr(3yrs)'] = self.get_cagr(self.scheme_performance['nav'],
                                                              self.scheme_performance['nav_3yrs'], 3)

        self.scheme_performance['cagr(4yrs)'] = self.get_cagr(self.scheme_performance['nav'],
                                                              self.scheme_performance['nav_4yrs'], 4)

        self.scheme_performance['cagr(5yrs)'] = self.get_cagr(self.scheme_performance['nav'],
                                                              self.scheme_performance['nav_5yrs'], 5)

        return self.scheme_performance


    def add_scheme_details(self, scheme_dataframe):

        self.get_scheme_details = ''' select * from {}'''.format(sql_parser.mutual_funds_scheme)
        self.scheme_details = pd.read_sql(self.get_scheme_details, self.engine)

        self.scheme_details.rename(columns={'code': 'scheme_code'}, inplace=True)
        self.added_scheme_details = pd.merge(scheme_dataframe, self.scheme_details,
                                             on=['scheme_code', 'scheme_nav_name'])
        return self.added_scheme_details

    def add_suffix(self,dataframe,suffix):
        dataframe.columns = [str(x)+str(suffix) for x in dataframe.columns]
        return dataframe

    def merge_dataframes(self,dataframes):
        merge_tables = dataframes[0]
        for i in range(len(dataframes) -1 ):
            merge_tables = pd.merge(merge_tables,dataframes[i+1],on=['scheme_nav_name','scheme_code'],suffixes=('_'+str(i)+'yrs', '_'+str(i+1)+'yrs'))
        return merge_tables

    def refine_columns(self, scheme_details):
        mf_columns = ['amc', 'scheme_code', 'scheme_nav_name', 'scheme_type', 'scheme_category', 'business_date', 'nav',
                      'business_date_3yrs', 'cagr(3yrs)', 'business_date_5yrs', 'cagr(5yrs)', 'scheme_minimum_amount',
                      'launch_date', '_closure_date']
        return scheme_details.loc[:, mf_columns]


if __name__ == "__main__":
    mf = MutualFunds()
    mutual_funds_list = ['Axis Liquid Fund - Direct Plan - Daily IDCW',
                         'UTI - Master Equity Plan Unit Scheme']
    mf_pd = mf.get_scheme_metrics(mutual_funds_list)
    all_mf = mf.get_all_metrics('cagr(5yrs)', 300)
    scheme_history = mf.get_mf_history_with_scheme_details(mutual_funds_list)
    elss = mf.get_all_metrics_by_scheme_category('ELSS','cagr(5yrs)')
