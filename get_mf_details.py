''' The module is used to get the performance of single mutual fund or all the mutual fund.
We can also get the history of any mutual fund.
'''

import datetime
import re
import pandas as pd
from pandas.tseries.offsets import BDay
from sql_parser import sql_parser


class MutualFunds:
    ''' Can be used to get the performance of all mutual funds or a single mutual fund. '''
    def __init__(self):
        self.engine = sql_parser.engine()
        self.today = (datetime.date.today() - BDay())
        self.initalize_business_days(self.today)

    def initalize_business_days(self, today):
        '''Used to initalized business days i.e today,1 year ago ...5year ago '''
        self.fetch_existing_mf_dates = ''' select distinct [business_date] 
                                           from {} '''.format(sql_parser.mutual_funds)
        self.existing_mf_dates = pd.read_sql(self.fetch_existing_mf_dates, self.engine)
        self.existing_mf_dates_list = self.existing_mf_dates['business_date'].dt.date.tolist()
        self.fetch_quality_issues_business_dates = \
            '''select distinct [business_date] 
               from {} 
               where quality_issues='Y' '''.format(sql_parser.mf_quality_issues)
        self.quality_issues_business_dates = pd.read_sql(self.fetch_quality_issues_business_dates, self.engine)
        self.quality_issues_business_dates = self.quality_issues_business_dates['business_date'].dt.date.tolist()
        self.business_date = today

        self.business_date_1yrs = self.validate_business_date(today - BDay(262))
        self.business_date_2yrs = self.validate_business_date(today - BDay(524))
        self.business_date_3yrs = self.validate_business_date(today - BDay(786))
        self.business_date_4yrs = self.validate_business_date(today - BDay(1048))
        self.business_date_5yrs = self.validate_business_date(today - BDay(1310))

        self.business_days = [self.business_date, self.business_date_1yrs, self.business_date_2yrs,
                              self.business_date_3yrs,
                              self.business_date_4yrs, self.business_date_5yrs]

        self.business_date, self.business_date_1yrs, self.business_date_2yrs, self.business_date_3yrs, \
        self.business_date_4yrs, self.business_date_5yrs = map(lambda x: x.strftime('%Y-%m-%d'), self.business_days)

    def validate_business_date(self, business_date):
        ''' Checks if the business_date is present in our database and
        also validates if the business date has enough data'''
        while business_date not in self.existing_mf_dates_list or business_date in self.quality_issues_business_dates:
            business_date = (business_date - BDay(1))
        return business_date

    @staticmethod
    def convert_date_to_string(business_date):
        ''' Converts a date to string format %Y-%m-%d '''
        return business_date.strftime('%Y-%m-%d')

    def get_scheme_metrics(self, mutual_fund_list=None):
        ''' Gives performance metrics for the given mutual funds '''
        scheme_details = self.get_mf_history(mutual_fund_list)
        scheme_performance = self.perform_calculation(scheme_details)
        scheme_performance = self.add_scheme_details(scheme_performance)
        scheme_performance = self.refine_columns(scheme_performance)
        return scheme_performance

    def get_all_metrics(self, sort_field, numcount=5):
        ''' Gives perfroamnce metrics for all mutual funds limited to the numcount value
        and 3 business days i.e today,3 years ago and 5 years ago'''
        get_all_mf_performance = '''select business_date,nav,scheme_nav_name,scheme_code 
                                         from {} 
                                         where business_date in ({})''' \
            .format(sql_parser.mutual_funds, ",".join("?" for _ in self.business_days))
        all_scheme_performance = pd.read_sql(get_all_mf_performance, self.engine, params=self.business_days)
        all_scheme_performance_calc = self.perform_calculation(all_scheme_performance)
        all_scheme_performance_calc = self.add_scheme_details(all_scheme_performance_calc)
        all_scheme_performance_calc = self.refine_columns(all_scheme_performance_calc)
        all_scheme_performance_calc.fillna(0, inplace=True)
        return all_scheme_performance_calc.sort_values(by=sort_field,
                                                            ascending=False) if numcount == 'max' else all_scheme_performance_calc.sort_values(
            by=sort_field, ascending=False).head(numcount)

    def get_all_metrics_by_scheme_category(self, scheme_category, sort_field, numcount=5):
        ''' Gives performance metrics based on the scheme like ELSS etc.'''
        all_metrics = self.get_all_metrics(sort_field, 'max')
        filt = all_metrics['scheme_category'] == scheme_category
        all_metrics = all_metrics.loc[filt]
        return all_metrics.sort_values(by=sort_field, ascending=False).head(numcount)

    def get_all_metrics_by_scheme_type(self, scheme_type, sort_field, numcount=5):
        '''Gives performance metrics based on scheme_type i.e Open ended or Close Ended'''
        all_metrics = self.get_all_metrics('return(5yrs)', 'max')
        filt = all_metrics['scheme_type'] == scheme_type
        all_metrics = all_metrics.loc[filt]
        return all_metrics.sort_values(by=sort_field, ascending=False).head(numcount)

    def get_mf_history(self, mutual_fund_list=None):
        ''' Gives the historical data for the given mutual funds  '''
        get_scheme_code = '''select code 
                                  from {} 
                                  where scheme_nav_name in ({}) ''' \
            .format(sql_parser.mutual_funds_scheme, ",".join(['?' for _ in mutual_fund_list]))
        scheme_code = pd.read_sql(get_scheme_code, self.engine, params=[mutual_fund_list])
        scheme_code = scheme_code.loc[:, 'code'].tolist()
        get_scheme_details = ''' select business_date,nav,scheme_nav_name,scheme_code 
                                      from {} 
                                      where scheme_code in ({})''' \
            .format(sql_parser.mutual_funds, ",".join(['?' for _ in scheme_code]))
        scheme_details = pd.read_sql(get_scheme_details, self.engine, params=[scheme_code])
        return scheme_details

    def get_mf_history_with_scheme_details(self, mutual_fund_list=None):
        ''' Adds scheme dettails to get_mf_history'''
        mf_history = self.get_mf_history(mutual_fund_list)
        mf_with_scheme_details = self.add_scheme_details(mf_history)
        mf_columns = ['amc', 'scheme_code', 'scheme_nav_name', 'scheme_type', 'scheme_category', 'business_date', 'nav',
                      'scheme_minimum_amount', 'launch_date', '_closure_date']

        return mf_with_scheme_details.loc[:, mf_columns].sort_values(by='business_date', ascending=False)

    @staticmethod
    def filter_on_business_date(dataframe, business_date):
        ''' reutrns dataframe for a particular business date '''
        return dataframe.loc[dataframe['business_date'] == business_date]

    @staticmethod
    def get_cagr(current_nav, hist_nav, years):
        ''' retyrns cagr '''
        return (current_nav / hist_nav) ** years - 1

    def perform_calculation(self, scheme_dataframe):
        ''' This function is used to calculate cagr for past 5 years. '''

        # filter out the dataframe on 3 business day i.e today,3 years ago and 5 years ago
        todays_scheme_details, scheme_details_one_years_ago, scheme_details_two_years_ago, scheme_details_three_years_ago, scheme_details_four_years_ago, scheme_details_five_years_ago = map(
            lambda business_time: self.filter_on_business_date(scheme_dataframe, business_time), self.business_days)

        dataframes = [todays_scheme_details, scheme_details_one_years_ago,
                           scheme_details_two_years_ago, scheme_details_three_years_ago,
                           scheme_details_four_years_ago, scheme_details_five_years_ago]

        scheme_performance = self.merge_dataframes(dataframes)

        scheme_performance.rename(columns={'nav_0yrs': 'nav', 'business_date_0yrs': 'business_date'}, inplace=True)

        scheme_performance['cagr(1yrs)'] = self.get_cagr(scheme_performance['nav'],
                                                              scheme_performance['nav_1yrs'], 1)

        scheme_performance['cagr(2yrs)'] = self.get_cagr(scheme_performance['nav'],
                                                              scheme_performance['nav_2yrs'], 2)

        scheme_performance['cagr(3yrs)'] = self.get_cagr(scheme_performance['nav'],
                                                              scheme_performance['nav_3yrs'], 3)

        scheme_performance['cagr(4yrs)'] = self.get_cagr(scheme_performance['nav'],
                                                              scheme_performance['nav_4yrs'], 4)

        scheme_performance['cagr(5yrs)'] = self.get_cagr(scheme_performance['nav'],
                                                              scheme_performance['nav_5yrs'], 5)

        return scheme_performance

    def add_scheme_details(self, scheme_dataframe):
        ''' Adds scheme details to a given dataframe '''
        get_scheme_details = ''' select * from {}'''.format(sql_parser.mutual_funds_scheme)
        scheme_details = pd.read_sql(get_scheme_details, self.engine)

        scheme_details.rename(columns={'code': 'scheme_code'}, inplace=True)
        added_scheme_details = pd.merge(scheme_dataframe, scheme_details,
                                             on=['scheme_code', 'scheme_nav_name'])
        return added_scheme_details

    @staticmethod
    def merge_dataframes(dataframes):
        ''' Merges the dataframe based on scheme_nav_name and scheme_code '''
        merge_tables = dataframes[0]
        for i in range(len(dataframes) - 1):
            merge_tables = pd.merge(merge_tables, dataframes[i + 1],
                                    how='left',
                                    on=['scheme_nav_name', 'scheme_code'],
                                    suffixes=('_' + str(i) + 'yrs', '_' + str(i + 1) + 'yrs'))
        return merge_tables

    @staticmethod
    def refine_columns(scheme_details):
        ''' filter the columns as given in mf_columns '''
        scheme_details.columns = [re.sub('cagr\((\w+)\)', 'return(\g<1>)', x) for x in scheme_details.columns]
        scheme_details.rename(columns={'return(1yrs)': 'return(1yr)'}, inplace=True)
        # mf_columns = ['amc', 'scheme_code', 'scheme_nav_name', 'scheme_type', 'scheme_category', 'business_date', 'nav',
        #               'business_date_1yrs', 'return(1yr)', 'business_date_2yrs', 'return(2yrs)',
        #               'business_date_3yrs', 'return(3yrs)', 'business_date_4yrs', 'return(4yrs)', 'business_date_5yrs',
        #               'return(5yrs)', 'scheme_minimum_amount',
        #               'launch_date', '_closure_date']
        return scheme_details
        # return scheme_details.loc[:, mf_columns]


if __name__ == "__main__":
    mf = MutualFunds()
    mutual_funds_list = ['Axis Liquid Fund - Direct Plan - Daily IDCW',
                         'UTI - Master Equity Plan Unit Scheme']
    mf_pd = mf.get_scheme_metrics(mutual_funds_list)
    print(mf_pd)
    all_mf = mf.get_all_metrics('return(5yrs)', 300)
    # scheme_history = mf.get_mf_history_with_scheme_details(mutual_funds_list)
# elss = mf.get_all_metrics_by_scheme_category('Growth','%return(5yrs)')
# print(elss)
