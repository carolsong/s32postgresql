import os
from flask import Flask, render_template

_file_dir = os.path.dirname(os.path.realpath(__file__))
app = Flask(__name__, template_folder=os.path.join(_file_dir, "templates"))


@app.route('/')
def index():
    return render_template('map.html', title='Tone booking')


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)
