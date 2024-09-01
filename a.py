from flask import Flask, jsonify
from flask_cors import CORS
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from datetime import datetime

# Initialize Flask app
app = Flask(__name__)

# Enable CORS
CORS(app)

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('2kapahada')

@app.route('/')
def home():
    return "Welcome to the DynamoDB Flask App!"

@app.route('/all-data', methods=['GET'])
def get_all_data():
    try:
        # Fetch all data from DynamoDB with pagination handling
        response = table.scan()
        data = response.get('Items', [])

        # Handle pagination to fetch all items if LastEvaluatedKey is present
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            data.extend(response.get('Items', []))

        # Return all data
        if data:
            return jsonify(data)
        else:
            return jsonify({'message': 'No data found.'})

    except NoCredentialsError:
        return jsonify({'error': 'Credentials not available.'}), 500
    except PartialCredentialsError:
        return jsonify({'error': 'Incomplete credentials provided.'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/one', methods=['GET'])
def get_latest_data():
    try:
        # Fetch all data from DynamoDB
        response = table.scan()
        data = response.get('Items', [])

        # If data exists, find the latest item by sorting on 'time' in descending order
        if data:
            latest_item = max(data, key=lambda x: datetime.fromisoformat(x['time']))
            return jsonify(latest_item)
        else:
            return jsonify({'message': 'No data found.'})

    except NoCredentialsError:
        return jsonify({'error': 'Credentials not available.'}), 500
    except PartialCredentialsError:
        return jsonify({'error': 'Incomplete credentials provided.'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    # Run the Flask app
    app.run(host='0.0.0.0', port=5000)
