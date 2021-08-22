import flask
from flask import request
import get_mf_details

app = flask.Flask(__name__)
app.config["DEBUG"] = True

@app.route('/', methods=['GET'])
def home():
    return '''<h1>Mutual funds site</h1></p>'''


# A route to return all of the available entries in our catalog.
@app.route('/allmf', methods=['GET'])
@app.route('/allmf/<header>', methods=['GET'])
@app.route('/allmf/<header>/<sort_field>', methods=['GET'])
def api_all(header=5,sort_field='cagr(5yrs)'):
    mf = get_mf_details.MutualFunds()
    print(header)
    print(sort_field)
    all_mf = mf.get_all_metrics(sort_field,int(header))
    return all_mf.to_html()

@app.route('/lmf', methods=['GET'])
@app.route('/lmf/<mflist>', methods=['GET'])
def api_mf(mflist='Axis Long Term Equity Fund - Direct Plan - Growth Option'):
    mf = get_mf_details.MutualFunds()
    mflist = mflist.split(",")
    all_mf = mf.get_scheme_metrics(mflist)
    return all_mf.to_html()

if __name__ == "__main__":
    app.run()
