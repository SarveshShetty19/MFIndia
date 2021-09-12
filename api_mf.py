''' The Module contains API to access mutual funds data  '''
from flask import request
import flask
import get_mf_details

app = flask.Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
    ''' Home Page '''
    return '''<h1>Mutual funds site</h1></p>'''


# A route to return all of the available entries in our catalog.
@app.route('/performance', methods=['GET'])
#@app.route('/performance/<sort_field>', methods=['GET'])
#@app.route('/performance/<sort_field>/<header>', methods=['GET'])
def api_performance():
    ''' Get performance of all the funds '''
    mf_instance = get_mf_details.Get_MutualFunds()
    header = int(request.args.get('header',10))
    sort_field= request.args.get('sort_by','return(5yrs)')
    scheme_category = request.args.get('scheme_category',None)
    scheme_type = request.args.get('scheme_type',None)
    print(scheme_category)
    print(scheme_type)
    all_mf = mf_instance.get_all_metrics(sort_field,scheme_category=scheme_category,scheme_type =scheme_type, numcount=header)
    return all_mf.to_html()

@app.route('/fund_performance', methods=['GET'])
#@app.route('/scheme_performance/<mflist>', methods=['GET'])
def api_fund_performance(mflist='Axis Long Term Equity Fund - Direct Plan - Growth Option'):
    ''' Get performance of a single fund '''
    mf_instance = get_mf_details.Get_MutualFunds()
    mflist = mflist.split(",")
    all_mf = mf_instance.get_scheme_metrics(mflist)
    return all_mf.to_html()

# @app.route('/performance/scheme_category/<category>', methods=['GET'])
# # @app.route('/performance/<scheme_category>/<header>', methods=['GET'])
# # @app.route('/performance/<scheme_category>/<header>/<sort_field>', methods=['GET'])
# def get_all_metrics_by_scheme_category(category='ELSS'):
#     ''' Get performance of funds by category '''
#     mf_instance = get_mf_details.Get_MutualFunds()
#     header = request.args.get('header',10)
#     sort_field= request.args.get('sort_by','return(5yrs)')
#     all_mf = mf_instance.get_all_metrics_by_scheme_category(category,sort_field,int(header))
#     return all_mf.to_html()

# @app.route('/performance/scheme_type/<fund_type>', methods=['GET'])
# # @app.route('/type', methods=['GET'])
# # @app.route('/type/<scheme_type>', methods=['GET'])
# # @app.route('/type/<scheme_type>/<header>', methods=['GET'])
# # @app.route('/type/<scheme_type>/<header>/<sort_field>', methods=['GET'])
# def get_all_metrics_by_scheme_type(fund_type='Open Ended'):
#     '''Get performance of funds by type '''
#     mf_instance = get_mf_details.Get_MutualFunds()
#     header = request.args.get('header',10)
#     sort_field= request.args.get('sort_by','return(5yrs)')
#     all_mf = mf_instance.get_all_metrics_by_scheme_type(fund_type,sort_field,int(header))
#     return all_mf.to_html()

@app.route('/nav_history', methods=['GET'])
@app.route('/nav_history/<mflist>', methods=['GET'])
def api_nav_history(mflist='Axis Long Term Equity Fund - Direct Plan - Growth Option'):
    ''' Get nav_history of a fund '''
    mf_instance = get_mf_details.Get_MutualFunds()
    mflist = mflist.split(",")
    nav_history = mf_instance.get_mf_history_with_scheme_details(mflist)

    return nav_history.to_html()

if __name__ == "__main__":
    app.run()
