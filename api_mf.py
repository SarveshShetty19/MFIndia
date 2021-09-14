""" The module contains api to get the performance of mutual funds.

imports :
    get_mf_details - The module that is used to get the mutualf und from database.

"""

from flask import request
import flask
import get_mf_details

app = flask.Flask(__name__)
app.config["DEBUG"] = True
mf_instance = get_mf_details.MutualFunds()

@app.route('/', methods=['GET'])
def home():
    ''' Home Page '''
    return '''<h1>Mutual funds site</h1></p>'''

@app.route('/performance', methods=['GET'])
def api_performance():
    """ returns cagr of all the mutual funds.
            Usage:
            /performance/head/sort_by/scheme_category/scheme_type
            Args:
                head - [optional] The number of rows to be displayed.
                sort_by - [optional] the field to be sorted by
                scheme_cateogry - [optional] filters the dataframe based on the scheme_category
                scheme_type - [optional] filters the dataframe based on the scheme_type
            Returns:
                json with performance of the mutual funds.
            Raises:
                N.A
    """
    head = int(request.args.get('head',10))
    sort_field= request.args.get('sort_by','return(5yrs)')
    scheme_category = request.args.get('scheme_category',None)
    scheme_type = request.args.get('scheme_type',None)
    all_mf = mf_instance.get_all_metrics(sort_field,scheme_category=scheme_category,scheme_type =scheme_type, numcount=head)
    return all_mf.to_json()

@app.route('/fund_performance', methods=['GET'])
def api_fund_performance(mflist=['Axis Long Term Equity Fund - Direct Plan - Growth Option']):
    """ returns cagr of selected funds.
            Usage:
            /performance/head/sort_by/scheme_category/scheme_type
            Args:
                mflist - The list of funds whose performance needs to be displayed.
            Returns:
                json with performance of the given mutual funds.
            Raises:
                N.A
    """
    all_mf = mf_instance.get_scheme_metrics(mflist)
    return all_mf.to_json()

@app.route('/absolute_returns', methods=['GET'])
@app.route('/absolute_returns/<mflist>', methods=['GET'])
def api_absolute_returns(mflist=['Axis Long Term Equity Fund - Direct Plan - Growth Option']):
    """ returns performance of selected funds.
            Usage:
            /performance/head/sort_by/scheme_category/scheme_type
            Args:
                mflist - The list of funds whose performance needs to be displayed.
            Returns:
                json with performance of the given mutual funds.
            Raises:
                N.A
    """
    absolute_returns = mf_instance.get_absolute_returns(mflist)

    return absolute_returns.to_json()

if __name__ == "__main__":
    app.run()
