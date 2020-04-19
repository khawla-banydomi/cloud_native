from flask import Flask
import json
from flask import jsonify
from flask_caching import Cache
from model import createModelFromMongo
#from model import createModelFromFile
config = {
    "DEBUG": True,
    "CACHE_TYPE": "simple",
    "CACHE_DEFAULT_TIMEOUT": 300
}
app = Flask(__name__)
app.config.from_mapping (config)
cache = Cache (app)


@ app.route ('/')
@ cache.cached (timeout = 50)
def hello_world ():
    return jsonify ('Hello, World!'), 200


@ app.route ('/ service / v1 / prediction / 24 hours /')
@ cache.cached (timeout = 50)
def pred_24Hours ():
    model = createModelFromMongo (24)
    return model, 200

@ app.route ('/ service / v1 / prediction / 48 hours /')
@ cache.cached (timeout = 50)
def pred_48Hours ():
    model = createMongoModel (48)
    return model, 200

@ app.route ('/ service / v1 / prediction / 72 hours /')
@ cache.cached (timeout = 50)
def pred_72Hours ():
    model = createMongoModel (72)
    return model, 200

if __name__ == '__main__':
    app.run ()
