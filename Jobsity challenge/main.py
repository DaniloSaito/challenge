from flask import Flask, flash, request, redirect, url_for, send_from_directory
import os
from werkzeug.utils import secure_filename
from datetime import datetime
from ingest import *

# UPLOAD_FOLDER is the directory where the uploaded files will be stored
UPLOAD_FOLDER = r'C:\Users\Visagio\Desktop\Jobsity challenge\uploads'
ALLOWED_EXTENSIONS = {'parquet', 'csv'}

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/', methods=['GET', 'POST'])
def upload_file():
	if request.method == 'POST':
		file = request.files['file']
		filename = secure_filename(file.filename)
		filename_with_timestamp = filename.replace(".csv","") + "_" + datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f") + ".csv"
		file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename_with_timestamp))
		read_file_spark(UPLOAD_FOLDER + '\\' + filename_with_timestamp)
		return redirect(url_for('download_file', name=filename))
	return '''
	<!doctype html>
	<title>Upload File</title>
	<h1>Upload your csv File</h1>
	<form method=post enctype=multipart/form-data>
	  <input type=file name=file>
	  <input type=submit value=Upload>
	</form>
	'''

@app.route('/uploads/<name>')
def download_file(name):
	return '''
	<!doctype html>
	<title>OK</title>
	<h1>File uploaded!</h1>
	'''

if __name__ == "__main__":
	app.run(debug=True)
	