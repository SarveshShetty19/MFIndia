import flask
import matplotlib_inline
from flask import request
import get_mf_details


app = flask.Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
    return '''<h1>Mutual funds site</h1></p>'''


# A route to return all of the available entries in our catalog.
@app.route('/performance', methods=['GET'])
@app.route('/performance/<sort_field>', methods=['GET'])
@app.route('/performance/<sort_field>/<header>', methods=['GET'])
def api_all(header=5,sort_field='return(5yrs)'):
    mf = get_mf_details.MutualFunds()
    print(header)
    print(sort_field)
    all_mf = mf.get_all_metrics(sort_field,int(header))
    return all_mf.to_html()

@app.route('/scheme_performance', methods=['GET'])
@app.route('/scheme_performance/<mflist>', methods=['GET'])
def api_single_mf(mflist='Axis Long Term Equity Fund - Direct Plan - Growth Option'):
    mf = get_mf_details.MutualFunds()
    mflist = mflist.split(",")
    all_mf = mf.get_scheme_metrics(mflist)
    return all_mf.to_html()

@app.route('/category', methods=['GET'])
@app.route('/category/<scheme_category>', methods=['GET'])
@app.route('/category/<scheme_category>/<header>', methods=['GET'])
@app.route('/category/<scheme_category>/<header>/<sort_field>', methods=['GET'])
def get_all_metrics_by_scheme_category(scheme_category='ELSS',header=5,sort_field='return(5yrs)'):
    mf = get_mf_details.MutualFunds()
    print(header)
    print(sort_field)
    all_mf = mf.get_all_metrics_by_scheme_category(scheme_category,sort_field,int(header))
    return all_mf.to_html()

@app.route('/type', methods=['GET'])
@app.route('/type/<scheme_type>', methods=['GET'])
@app.route('/type/<scheme_type>/<header>', methods=['GET'])
@app.route('/type/<scheme_type>/<header>/<sort_field>', methods=['GET'])
def get_all_metrics_by_scheme_type(scheme_type='Close Ended',header=5,sort_field='return(5yrs)'):
    mf = get_mf_details.MutualFunds()
    print(header)
    print(sort_field)
    all_mf = mf.get_all_metrics_by_scheme_type(scheme_type,sort_field,int(header))
    return all_mf.to_html()

@app.route('/nav_history', methods=['GET'])
@app.route('/nav_history/<mflist>', methods=['GET'])
def api_nav_history(mflist='Axis Long Term Equity Fund - Direct Plan - Growth Option'):
    mf = get_mf_details.MutualFunds()
    mflist = mflist.split(",")
    nav_history = mf.get_mf_history_with_scheme_details(mflist)

    return nav_history.to_html()

if __name__ == "__main__":
    app.run()
