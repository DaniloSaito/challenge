from flask import Flask, request, redirect, url_for
import os
from werkzeug.utils import secure_filename
from datetime import datetime
from ingest import *

# UPLOAD_FOLDER is the directory where the uploaded files will be stored
print(os.path.dirname(os.path.realpath(__file__)) +r'\uploads')
UPLOAD_FOLDER = os.path.dirname(os.path.realpath(__file__)) +r'\uploads'

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
		return redirect(url_for('status_ok', name=filename))
	return '''
	<!doctype html>
	<title>Upload File</title>
	<h1>Upload your csv File</h1>
	<form method=post enctype=multipart/form-data>
	  <input type=file name=file>
	  <input type=submit value=Upload>
	</form>
	<br>
	After uploading, you can check the job status on the <a href="http://localhost:4040/jobs/">Spark Interface</a> 
	'''

@app.route('/uploads/<name>')
def status_ok(name):
	return '''
	<!doctype html>
	<title>OK</title>
	<h1>File uploaded!</h1>
	'''

if __name__ == "__main__":
	app.run(debug=True)
	