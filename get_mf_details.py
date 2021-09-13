""" Get mutual fund details from database.

imports :
    sql_parser - The sql_parser contains the information of the driver,database
    and the tables

  Typical usage example:

  mf = MutualFunds()
"""

import datetime
import re
import pandas as pd
from pandas.tseries.offsets import BDay
from sql_parser import sql_parser


class MutualFunds:
    """ MutualFunds is the only class in this module.
        It contains functions that are used to calculate the mutual fund performance based on the data stored
        in the database
    """
    def __init__(self):
        """ intializes the sql engine and 5 business days - business_date, business_date_1yrs, business_date_2yrs, business_date_3yrs,
            business_date_4yrs, business_date_5yrs,
            Args:
                N.A
            Returns:
                N.A
            Raises:
                N.A
        """
        self.engine = sql_parser.engine()
        self.quality_business_dates = self.quality_business_date()
        self.today = (datetime.date.today() - BDay())
        self.business_days = self.initalize_business_days(self.today)


    def quality_business_date(self):
        self.fetch_quality_business_dates = '''select distinct [business_date] 
                                               from {} 
                                               where quality_issues='N' '''.format(sql_parser.mf_quality_issues)
        self.quality_business_dates = pd.read_sql(self.fetch_quality_business_dates, self.engine)
        return self.quality_business_dates['business_date'].dt.date.tolist()

    def initalize_business_days(self, today):
        """ returns  5 business days - business_date, business_date_1yrs, business_date_2yrs, business_date_3yrs,
            business_date_4yrs, business_date_5yrs

            Args:
                today - todays business date.
            Returns:
                returns a list with the following data
                business_date_1yrs - COB date 1 year ago.
                business_date_2yrs - COB date 2 year ago.
                business_date_3yrs - COB date 3 year ago.
                business_date_4yrs - COB date 4 year ago.
                business_date_5yrs - COB date 5 year ago.
            Raises:
                N.A
        """
        self.business_date = self.validate_business_date(today)
        self.business_date_1yrs = self.validate_business_date(today - BDay(262))
        self.business_date_2yrs = self.validate_business_date(today - BDay(524))
        self.business_date_3yrs = self.validate_business_date(today - BDay(786))
        self.business_date_4yrs = self.validate_business_date(today - BDay(1048))
        self.business_date_5yrs = self.validate_business_date(today - BDay(1310))
        self.business_date_6yrs = self.validate_business_date(today - BDay(1572))
        #.strftime('%Y-%m-%d')

        return [self.business_date, self.business_date_1yrs, self.business_date_2yrs, self.business_date_3yrs,
                self.business_date_4yrs, self.business_date_5yrs,self.business_date_5yrs]

    def validate_business_date(self, business_date):
        """ gets a business_date which has adequate data in our database.

            Args:
                business_date - Any date.
            Returns:
                business_date - The function checks if the business_date that has been passed doesn't have any quality issue.
                if it does have any quality issue then it keeps on subtracting 1 day from the business_date until it finds a
                good date.
            Raises:
                N.A
        """
        while business_date.date() not in self.quality_business_dates:
            business_date = (business_date - BDay(1))
        return business_date

    def get_mf_history(self,mutual_fund_list):
        """ Gives performance history of selected mutual fund..
            Args:
            mutual_fund_list - The list of mutual funds whose performance needs to be calculated.

            Returns:
                A dataframe with nav history of the mutual fund.
            Raises:
                N.A
        """
        get_scheme_code = '''select code 
                                          from {} 
                                          where scheme_nav_name in ({}) ''' \
            .format(sql_parser.mutual_funds_scheme, ",".join(['?' for _ in mutual_fund_list]))
        scheme_code = pd.read_sql(get_scheme_code, self.engine, params=[mutual_fund_list])
        scheme_code = scheme_code.loc[:, 'code'].to_list()
        get_performance_history = ''' select scheme_code, scheme_nav_name, nav, business_date from mf_india where 
                scheme_code in ({}) '''.format(",".join(['?' for _ in scheme_code]))
        return pd.read_sql(get_performance_history, self.engine, params=[scheme_code])

    def get_scheme_metrics(self, mutual_fund_list=None):
        """ Gives performance metrics for last 5 years for selected mutual funds.
            Args:
                mutual_fund_list - The list of mutual funds whose performance metrics is needed.
            Returns:
                scheme_performance - A dataframe which has business_date,nav,cagr for last 5 years.
            Raises:
                N.A
        """
        scheme_history = self.get_mf_history(mutual_fund_list)
        scheme_performance = self.perform_cagr_calculation(scheme_history)
        scheme_performance = self.add_scheme_details(scheme_performance)
        scheme_performance = self.refine_columns(scheme_performance)
        return scheme_performance

    def get_all_metrics(self, sort_field,scheme_category=None,scheme_type =None, numcount=5):
        """ Gives performance metrics for last 5 years for all mutual funds.
            Args:
                sort_field - sorts according to the field passed in this category.
                scheme_category - gives performance only for the selected scheme_category
                scheme_type - gives performance only for the selected scheme_type
                numcount - Limits the output to the number of rows passed in this parameter.

            Returns:
                all_scheme_performance_calc - A dataframe which has business_date,nav,cagr for last 5 years for all the mutual funds.
            Raises:
                N.A
        """
        get_all_mf_performance = '''select business_date,nav,scheme_nav_name,scheme_code 
                                         from {} 
                                         where business_date in ({})''' \
            .format(sql_parser.mutual_funds, ",".join("?" for _ in self.business_days))
        all_scheme_performance = pd.read_sql(get_all_mf_performance, self.engine, params=self.business_days)
        all_scheme_performance_calc = self.perform_cagr_calculation(all_scheme_performance)
        all_scheme_performance_calc = self.add_scheme_details(all_scheme_performance_calc)
        all_scheme_performance_calc = self.refine_columns(all_scheme_performance_calc)
        all_scheme_performance_calc.fillna(0, inplace=True)

        if scheme_category and scheme_type:
            filt = (all_scheme_performance_calc['scheme_category'] == scheme_category) & (all_scheme_performance_calc['scheme_type'] == scheme_type)
            all_scheme_performance_calc = all_scheme_performance_calc.loc[filt]
        elif scheme_category:
            filt = all_scheme_performance_calc['scheme_category'] == scheme_category
            all_scheme_performance_calc = all_scheme_performance_calc.loc[filt]
        elif scheme_type:
            filt =  all_scheme_performance_calc['scheme_type'] == scheme_type
            all_scheme_performance_calc = all_scheme_performance_calc.loc[filt]
        return all_scheme_performance_calc.sort_values(by=sort_field,ascending=False).head(numcount)

    def get_absolute_returns(self,mutual_fund_list=None):
        """ Gets absolute returns for the list of mutual funds.
            Args:
            mutual_fund_list - The list of mutual funds whose performance needs to be calculated.

            Returns:
                A dataframe with business_date,scheme_nav_name and absolute returns.
            Raises:
                N.A
        """
        df1 = self.get_mf_history(mutual_fund_list)
        df2 = df1.copy(deep=True)
        df1["comparision_business_date"] = df1["business_date"].apply(lambda x: x - BDay(262))
        df = pd.merge(df1, df2, how='inner',left_on=['comparision_business_date','scheme_code'], right_on=['business_date','scheme_code'],suffixes=('_l', '_r'))
        df["absolute_returns"] = (df["nav_l"] - df["nav_r"]) / df["nav_r"] * 100
        df.dropna(inplace=True)
        df.reset_index(drop=True,inplace=True)
        df = df.sort_values(by="business_date_l",ascending=False)

        return df.loc[:,["business_date_l","scheme_nav_name_l","absolute_returns"]]

    @staticmethod
    def filter_on_business_date(dataframe, business_date):
        """ Filter a dataframe on a particular business_date
            Args:
            dataframe - The dataframe that needs to be filter.
            business_date - the business_date dataframe needs to be filtered for
            Returns:
                The dataframe filtered on the business_date passed.
            Raises:
                N.A
        """
        return dataframe.loc[dataframe['business_date'] == business_date]

    @staticmethod
    def get_cagr(current_nav, hist_nav, years):
        """ Function for calculating cagr.
            Args:
            current_nav - The latest nav for the mutual fund.
            hist_nav - The historical nav for the mutual fund.
            years - The number of years the cagr needs to be calculated for.
            Returns:
                cagr value.
            Raises:
                N.A
        """
        #return ((current_nav / hist_nav) ** (1 / years)) - 1
        return ((current_nav / hist_nav) ** (1 / years) - 1) * 100

    def perform_cagr_calculation(self, scheme_dataframe):
        """ Function for calculating cagr for last 5 years.
            Args:
            scheme_dataframe - The dataframe which included nav history of the mutual fund.
            Returns:
            scheme_performance - A dataframe with calculated value for last 5 years.
            Raises:
                N.A
        """

        todays_scheme_details, \
        scheme_details_one_years_ago, \
        scheme_details_two_years_ago, \
        scheme_details_three_years_ago, \
        scheme_details_four_years_ago, \
        scheme_details_five_years_ago, \
        scheme_details_six_years_ago   = map(
            lambda business_time: self.filter_on_business_date(scheme_dataframe, business_time), self.business_days)


        dataframes = [todays_scheme_details, scheme_details_one_years_ago,
                      scheme_details_two_years_ago, scheme_details_three_years_ago,
                      scheme_details_four_years_ago, scheme_details_five_years_ago,
                      scheme_details_six_years_ago]

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
        """ Adds scheme_details like the scheme_type,scheme_category to exisitng datamframe
            Args:
            scheme_dataframe - The dataframe for which scheme_details needs to be added.
            Returns:
            added_scheme_details - A dataframe with added scheme_details.
            Raises:
                N.A
        """

        get_scheme_details = ''' select * from {}'''.format(sql_parser.mutual_funds_scheme)
        scheme_details = pd.read_sql(get_scheme_details, self.engine)

        scheme_details.rename(columns={'code': 'scheme_code'}, inplace=True)
        added_scheme_details = pd.merge(scheme_dataframe, scheme_details,
                                             on=['scheme_code', 'scheme_nav_name'])
        return added_scheme_details

    @staticmethod
    def merge_dataframes(dataframes):
        """ Merges the list of dataframe that is passed to this function
            Args:
            dataframes - A list containing dataframes.
            Returns:
            merge_tables - a merged dataframe.
            Raises:
                N.A
        """
        merge_tables = dataframes[len(dataframes)-1]
        for i in range(len(dataframes)-1,0,-1):
            merge_tables = pd.merge(merge_tables, dataframes[i - 1],
                                    how='left',
                                    on=['scheme_nav_name', 'scheme_code'],
                                    suffixes=('_' + str(i) + 'yrs', '_' + str(i - 1) + 'yrs'))
        return merge_tables

    @staticmethod
    def refine_columns(dataframe):
        """  Limit the column of dataframe to ["business_date","Fund Name","return(1yr)","return(2yrs)","return(3yrs)","return(4yrs)","return(5yrs)"]
            Args:
            dataframe - The dataframe whose column needs to be filters.
            Returns:
            dataframe - filtered dataframe.
            Raises:
                N.A
        """
        dataframe.columns = [re.sub('cagr\((\w+)\)', 'return(\g<1>)', x) for x in dataframe.columns]
        dataframe.rename(columns={'return(1yrs)': 'return(1yr)','scheme_nav_name':'Fund Name'}, inplace=True)
        return dataframe.loc[:,["business_date","Fund Name","scheme_category","scheme_type","return(1yr)","return(2yrs)","return(3yrs)","return(4yrs)","return(5yrs)"]]

    def load_mf_scheme_details(self):
        """ Loads all the data from table mutual_funds_scheme
            Args:
            N.A
            Returns:
            A datfarme with loaded values of mutual_funds_scheme
            Raises:
                N.A
        """
        get_mf_list = '''select *  from {}'''.format(sql_parser.mutual_funds_scheme)
        return pd.read_sql(get_mf_list,self.engine)


if __name__ == "__main__":
    mf = MutualFunds()
    mutual_funds_list = ['Axis Long Term Equity Fund - Regular Plan - Growth']
    mf_pd = mf.get_scheme_metrics(mutual_funds_list)
    df = mf.get_absolute_returns(mutual_funds_list)
    df = mf.get_all_metrics('return(5yrs)', 'Equity Scheme - ELSS')


