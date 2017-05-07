import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
basedir = os.path.abspath(os.path.dirname(__file__))

SQLALCHEMY_DATABASE_URI =''
db = None
app = None

class Config(object):
    global SQLALCHEMY_DATABASE_URI
    global db
    global app
    
    DEBUG = False
    TESTING = False
    CSRF_ENABLED = True
    SECRET_KEY = 'mykey'
    SQLALCHEMY_DATABASE_URI = os.environ['DATABASE_URL']
    app = Flask(__name__)

    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    app.config['SQLALCHEMY_DATABASE_URI'] = SQLALCHEMY_DATABASE_URI
    app.config['UPLOADED_PRICES_DEST'] = 'C:\\temp'

    db = SQLAlchemy(app)
    


class ProductionConfig(Config):
    DEBUG = False


class StagingConfig(Config):
    DEVELOPMENT = True
    DEBUG = True


class DevelopmentConfig(Config):
    DEVELOPMENT = True
    DEBUG = True


class TestingConfig(Config):
    TESTING = True
