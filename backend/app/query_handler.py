from flask import Flask, jsonify
from app.data_processor import DataProcessor

app = Flask(__name__)
data_processor = DataProcessor('/path/to/your/csv/file.csv')  # Update path as needed

@app.route('/api/data', methods=['GET'])
def get_data():
    df = data_processor.load_data()
    # Perform queries
    result_1 = data_processor.query_1(df)
    result_2 = data_processor.query_2(df)
    return jsonify({'result_1': result_1, 'result_2': result_2})

if __name__ == '__main__':
    app.run(debug=True)

