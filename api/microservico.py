import time
import bid
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/bid/',methods=['POST'])
def calcular():
  parametro = request.json
  cotacao = bid.cotar(parametro)
  return cotacao

if __name__ == '__main__':
  app.run()