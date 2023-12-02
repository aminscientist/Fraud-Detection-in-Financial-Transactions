from flask import Flask, jsonify
import os
import json

def load_json(file_name, directory='data'):
    file_path = os.path.join(os.path.dirname(__file__), directory, file_name)
    with open(file_path, 'r') as file:
        return json.load(file)

app = Flask(__name__)

# Charger les données depuis les fichiers JSON
transactions = load_json('transactions.json', 'data')
customers = load_json('customers.json', 'data')
external_data = load_json('external_data.json', 'data')

@app.route('/api/transactions', methods=['GET'])
def get_transactions():
    return jsonify(transactions)

@app.route('/api/customers', methods=['GET'])
def get_customers():
    return jsonify(customers)

@app.route('/api/externalData', methods=['GET'])
def get_external_data():
    return jsonify(external_data)

if __name__ == '__main__':
    app.run(debug=True)