from flask import Flask, request, jsonify
from crate import client
import os
from dotenv import load_dotenv
from datetime import datetime
import pytz
import time

load_dotenv()

CRATE_HOST = os.getenv('CRATE_HOST')
app = Flask(__name__)

def exec_query(query, params=None):
    try:
        with client.connect(CRATE_HOST, username="crate", error_trace=True) as connection:
            connection.autocommit = True
            cursor = connection.cursor()
            cursor.execute(query, params or ())
            if query.strip().upper().startswith("SELECT"):
                result = cursor.fetchall()
                return result
            else:
                print(params[2])
                print(cursor.rowcount)
                print("Query executed successfully")
                return cursor.rowcount  # Log how many rows were updated
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cursor.close()

@app.route('/v2/notify', methods=['POST'])
def home():
    body_data = request.get_json()
    headers = request.headers

    service = headers.get("fiware-service", "").lower()
    if not service:
        return jsonify({"error": "Missing Fiware-Service header"}), 400

    service = f"mt{service}"
    data = body_data.get("data", [])
    if not data:
        return jsonify({"error": "Missing data field in request"}), 400

    obj = data[0]
    entity_id = obj.get('id')
    entity_type = obj.get('type')
    if not entity_id or not entity_type:
        return jsonify({"error": "Missing id or type in entity data"}), 400

    entity = f"et{entity_type.lower()}"

    check_query = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = ?
        AND table_schema = ?
        AND column_name = 'rssi';
    """
    add_column_query = f'ALTER TABLE "{service}"."{entity}" ADD COLUMN rssi float;'

    if not exec_query(check_query, [entity, service]):
        exec_query(add_column_query)

    rssi = obj.get('longueur', {}).get('value')
    date = obj.get('date', {}).get('value')
    if rssi is None or date is None:
        return jsonify({"error": "Missing rssi or date in entity data"}), 400

    try:
        date_epoch = int(datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.UTC).timestamp() * 1000)
    except ValueError:
        return jsonify({"error": "Invalid date format"}), 400

    # Check if row exists with the given entity_id and date
    check_row_query = """
        SELECT 1 FROM "{service}"."{entity}" 
        WHERE "entity_id" = ? AND "date" = ?;
    """.format(service=service, entity=entity)

    max_retries = 5  # Maximum number of retries
    retries = 0

    while retries < max_retries:
        row_exists = exec_query(check_row_query, [entity_id, date_epoch])

        if row_exists:
            # If row exists, update the row
            update_query = f"""
                UPDATE "{service}"."{entity}"
                SET "rssi" = ?
                WHERE "entity_id" = ? AND "date" = ?;
            """
            exec_query(update_query, [rssi, entity_id, date_epoch])
            return jsonify({"status": "done"})
    else:
        retries += 1
        time.sleep(1)
        # Row does not exist, optionally handle insertion or return error
        return jsonify({"error": "Row not found for update"}), 404

    return jsonify({"status": "done"})

if __name__ == '__main__':
    app.run(debug=True)
