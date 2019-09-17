from flask import Flask, render_template

app = Flask(__name__, template_folder='../pages/templates')


@app.route('/')
def index():
    return render_template('map.html', title='Tone booking')


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5001)
