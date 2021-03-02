
from flask import *
import pandas as pd
import numpy
import os 
import fetch2 as fs
app = Flask(__name__)

@app.route('/')
def home():
    return render_template('main.html')



@app.route("/predict",methods=['POST'])
def predict():
    input_month = request.form['month']
    input_year  = request.form['year']
    data1=fs.rules_fetch(int(input_month),int(input_year))
    return render_template('main.html' , tables=[data1.to_html(classes='data1')])  

@app.errorhandler(500)
def internal_error(e):
    return render_template('error500.html')

@app.errorhandler(404)
def page_not_found(e):
    return render_template('error404.html')

if __name__ == "__main__":
    app.run(debug=True, use_reloader=False)