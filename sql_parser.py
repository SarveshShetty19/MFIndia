import sqlalchemy as sql
import pyodbc
import configparser

class sql_parser:

    sqlconfigparser = configparser.ConfigParser()
    sqlconfigparser.read('server_config.ini')
    server = sqlconfigparser['SQL']['server']
    driver = sqlconfigparser['SQL']['driver']
    db = sqlconfigparser['SQL']['db']
    mutual_funds = sqlconfigparser['Tables']['Mutual_Funds']
    mutual_funds_scheme = sqlconfigparser['Tables']['Mutual_Funds_Scheme']
    mf_quality_issues = sqlconfigparser['Tables']['Mutual_Funds_Quality']

    @classmethod
    def engine(cls):
        return sql.create_engine('mssql+pyodbc://{}/{}?driver={}'.format(cls.server, cls.db, cls.driver))


if __name__ == "__main__":
    pass


