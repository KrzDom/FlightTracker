from flask import Flask

app = Flask(__name__)


@app.route("/")
def print():
    return "<p> This is Sparta and the Future!<p>"

@app.route("/home")
def local():
    return "<p> This is not Sparta and also not the Future!<p>"
