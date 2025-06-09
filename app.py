from flask import Flask, request, jsonify, send_from_directory, redirect, url_for, session
from flask_cors import CORS
import os
import atexit

os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

from apscheduler.schedulers.background import BackgroundScheduler
from google_auth_oauthlib.flow import Flow

import database_utils as db_utils
import database_setup
import gdrive_service

app = Flask(__name__, static_folder='public', static_url_path='')
CORS(app) # Enable CORS for all routes
# Secret key is needed for session management in Flask
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "super-secret-key-for-dev")

# --- Database Initialization ---
try:
    if not os.path.exists(db_utils.DB_PATH):
        print(f"Database not found at {db_utils.DB_PATH}, initializing...")
        database_setup.init_db(populate_with_sample_data=True)
    else:
        database_setup.init_db(populate_with_sample_data=False)
    print("Database schema checked/initialized from app.py.")
except Exception as e:
    print(f"Failed to initialize database schema: {e}")


# --- Scheduler Setup ---
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler

# Define the persistent job store using our existing SQLite database
jobstores = {
    'default': SQLAlchemyJobStore(url=f'sqlite:///{db_utils.DB_PATH}')
}

# Configure executors (how jobs are run)
executors = {
    'default': {'type': 'threadpool', 'max_workers': 20}
}

# Create the scheduler with the persistent job store
scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, daemon=True)
scheduler.start()


# --- Google Drive Backup API Routes ---
@app.route('/api/backup/authorize')
def authorize():
    if not os.path.exists(gdrive_service.CREDENTIALS_FILE):
         return jsonify({"error": "credentials.json not found on server."}), 500

    flow = Flow.from_client_secrets_file(
        gdrive_service.CREDENTIALS_FILE,
        scopes=gdrive_service.SCOPES,
        redirect_uri=url_for('oauth2callback', _external=True)
    )
    authorization_url, state = flow.authorization_url(
        access_type='offline',
        include_granted_scopes='true'
    )
    session['state'] = state
    return redirect(authorization_url)

@app.route('/oauth2callback')
def oauth2callback():
    state = session['state']
    flow = Flow.from_client_secrets_file(
        gdrive_service.CREDENTIALS_FILE,
        scopes=gdrive_service.SCOPES,
        state=state,
        redirect_uri=url_for('oauth2callback', _external=True)
    )
    flow.fetch_token(authorization_response=request.url)
    credentials = flow.credentials
    # Save credentials using pickle
    with open(gdrive_service.TOKEN_PICKLE_FILE, 'wb') as token_file:
        import pickle
        pickle.dump(credentials, token_file)
    
    # Redirect back to the main app page, ideally to the backup tab
    return redirect('/#backup')

@app.route('/api/backup/status', methods=['GET'])
def backup_status():
    is_authorized = os.path.exists(gdrive_service.TOKEN_PICKLE_FILE)
    return jsonify({"isAuthorized": is_authorized})

@app.route('/api/backup/now', methods=['POST'])
def backup_now():
    result = gdrive_service.upload_db_to_drive()
    if result.get("success"):
        return jsonify(result)
    else:
        return jsonify(result), 500

@app.route('/api/backup/schedule/get', methods=['GET'])
def get_schedule():
    job = scheduler.get_job('daily-db-backup')
    if job:
        # The trigger object has the run time info
        # job.trigger.fields is a list of field objects from the cron trigger
        # We need to convert them to strings before formatting
        hour_field = job.trigger.fields[5]   # Field for 'hour'
        minute_field = job.trigger.fields[6] # Field for 'minute'

        # Convert the field objects to integers to format them correctly
        hour = int(str(hour_field))
        minute = int(str(minute_field))
        
        return jsonify({
            "isScheduled": True,
            "time": f"{hour:02d}:{minute:02d}",
            "nextRun": job.next_run_time.isoformat() if job.next_run_time else None
        })
    else:
        return jsonify({"isScheduled": False})

@app.route('/api/backup/schedule/set', methods=['POST'])
def set_schedule():
    data = request.json
    backup_time = data.get('time') # Expected format "HH:MM"
    if not backup_time:
        return jsonify({"error": "Time not provided"}), 400
    
    try:
        hour, minute = map(int, backup_time.split(':'))
        # Remove existing job before adding a new one
        if scheduler.get_job('daily-db-backup'):
            scheduler.remove_job('daily-db-backup')

        scheduler.add_job(
            func=gdrive_service.upload_db_to_drive,
            trigger='cron',
            hour=hour,
            minute=minute,
            id='daily-db-backup',
            replace_existing=True
        )
        print(f"Backup job scheduled for {hour:02d}:{minute:02d} daily.")
        return jsonify({"success": True, "message": f"Backup scheduled for {hour:02d}:{minute:02d} daily."})
    except Exception as e:
        print(f"Error setting schedule: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/backup/schedule/cancel', methods=['POST'])
def cancel_schedule():
    if scheduler.get_job('daily-db-backup'):
        scheduler.remove_job('daily-db-backup')
        print("Backup job cancelled.")
        return jsonify({"success": True, "message": "Backup schedule cancelled."})
    return jsonify({"success": False, "message": "No schedule was set."})


# --- Core App API Routes (Unchanged) ---
@app.route('/api/all-data', methods=['GET'])
def get_all_data_route():
    try:
        data = db_utils.get_all_data()
        return jsonify(data)
    except Exception as e:
        print(f"Error fetching all data: {e}")
        return jsonify({"error": "Failed to fetch data"}), 500

# ... (all other member, payment, writeoff, history routes remain here, unchanged) ...
@app.route('/api/members', methods=['POST'])
def add_member_route():
    try:
        member_data = request.json
        if not member_data.get('id'):
            member_data['id'] = db_utils.generate_id()
        
        for key in ['statusHistory', 'monthlyFeeHistory', 'paymentCycleDayHistory']:
            if member_data.get(key):
                for entry in member_data[key]:
                    if not entry.get('id'):
                        entry['id'] = db_utils.generate_id()
        
        new_member = db_utils.upsert_member(member_data)
        return jsonify(new_member), 201
    except Exception as e:
        print(f"Error adding member: {e}")
        return jsonify({"error": f"Failed to add member: {str(e)}"}), 500

@app.route('/api/members/<member_id>', methods=['PUT'])
def update_member_route(member_id):
    try:
        member_data = request.json
        member_data['id'] = member_id # Ensure ID from URL is used

        for key in ['statusHistory', 'monthlyFeeHistory', 'paymentCycleDayHistory']:
            if member_data.get(key):
                for entry in member_data[key]:
                    if not entry.get('id'):
                        entry['id'] = db_utils.generate_id()

        updated_member = db_utils.upsert_member(member_data)
        if updated_member:
            return jsonify(updated_member)
        else:
            return jsonify({"error": "Member not found"}), 404
    except Exception as e:
        print(f"Error updating member {member_id}: {e}")
        return jsonify({"error": f"Failed to update member: {str(e)}"}), 500

@app.route('/api/members/<member_id>', methods=['DELETE'])
def delete_member_route(member_id):
    try:
        success = db_utils.delete_member(member_id)
        if success:
            return "", 204
        else:
            return jsonify({"error": "Member not found"}), 404
    except Exception as e:
        print(f"Error deleting member {member_id}: {e}")
        return jsonify({"error": "Failed to delete member"}), 500

# --- Payments ---
@app.route('/api/payments', methods=['POST'])
def add_payment_route():
    try:
        payment_data = request.json
        if not payment_data.get('id'):
            payment_data['id'] = db_utils.generate_id()
        new_payment = db_utils.upsert_payment(payment_data)
        return jsonify(new_payment), 201
    except Exception as e:
        print(f"Error adding payment: {e}")
        return jsonify({"error": "Failed to add payment"}), 500

@app.route('/api/payments/<payment_id>', methods=['PUT'])
def update_payment_route(payment_id):
    try:
        payment_data = request.json
        payment_data['id'] = payment_id
        updated_payment = db_utils.upsert_payment(payment_data)
        if updated_payment: # upsert_payment returns the data if successful
            return jsonify(updated_payment)
        else: # This case might not be hit if upsert always returns or raises error
            return jsonify({"error": "Payment not found or no changes made"}), 404
    except Exception as e:
        print(f"Error updating payment {payment_id}: {e}")
        return jsonify({"error": "Failed to update payment"}), 500

@app.route('/api/payments/<payment_id>', methods=['DELETE'])
def delete_payment_route(payment_id):
    try:
        success = db_utils.delete_payment(payment_id)
        if success:
            return "", 204
        else:
            return jsonify({"error": "Payment not found"}), 404
    except Exception as e:
        print(f"Error deleting payment {payment_id}: {e}")
        return jsonify({"error": "Failed to delete payment"}), 500

# --- WriteOffs ---
@app.route('/api/writeoffs', methods=['POST'])
def add_writeoff_route():
    try:
        writeoff_data = request.json
        if not writeoff_data.get('id'):
            writeoff_data['id'] = db_utils.generate_id()
        new_writeoff = db_utils.upsert_writeoff(writeoff_data)
        return jsonify(new_writeoff), 201
    except Exception as e:
        print(f"Error adding write-off: {e}")
        return jsonify({"error": "Failed to add write-off"}), 500

@app.route('/api/writeoffs/<writeoff_id>', methods=['PUT'])
def update_writeoff_route(writeoff_id):
    try:
        writeoff_data = request.json
        writeoff_data['id'] = writeoff_id
        updated_writeoff = db_utils.upsert_writeoff(writeoff_data)
        if updated_writeoff:
            return jsonify(updated_writeoff)
        else:
            return jsonify({"error": "Write-off not found or no changes made"}), 404
    except Exception as e:
        print(f"Error updating write-off {writeoff_id}: {e}")
        return jsonify({"error": "Failed to update write-off"}), 500

@app.route('/api/writeoffs/<writeoff_id>', methods=['DELETE'])
def delete_writeoff_route(writeoff_id):
    try:
        success = db_utils.delete_writeoff(writeoff_id)
        if success:
            return "", 204
        else:
            return jsonify({"error": "Write-off not found"}), 404
    except Exception as e:
        print(f"Error deleting write-off {writeoff_id}: {e}")
        return jsonify({"error": "Failed to delete write-off"}), 500

# --- Member History Entry Edits/Deletes ---
@app.route('/api/members/<member_id>/history/<history_type>/<entry_id>', methods=['PUT'])
def update_member_history_route(member_id, history_type, entry_id):
    try:
        data = request.json
        new_effective_date = data.get('newEffectiveDate')
        if not new_effective_date:
            return jsonify({"error": "newEffectiveDate is required"}), 400
        
        updated_member = db_utils.update_history_entry(member_id, entry_id, history_type, new_effective_date)
        if updated_member:
            return jsonify(updated_member)
        else: # This case might be if the member_id was valid but entry_id wasn't, or no change made.
            return jsonify({"error": "History entry or member not found, or no change made"}), 404
    except ValueError as ve: # Catch specific errors from db_utils
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        print(f"Error updating history entry ({history_type}) for member {member_id}: {e}")
        return jsonify({"error": f"Failed to update history entry: {str(e)}"}), 500

@app.route('/api/members/<member_id>/history/<history_type>/<entry_id>', methods=['DELETE'])
def delete_member_history_route(member_id, history_type, entry_id):
    try:
        updated_member = db_utils.delete_specific_history_entry(member_id, entry_id, history_type)
        if updated_member: # delete_specific_history_entry now should return the member or raise error
            return jsonify(updated_member)
        else: # Should not be hit if db_utils raises ValueError
             return jsonify({"error": "History entry or member not found"}), 404
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400 # e.g., "Cannot delete the only entry"
    except Exception as e:
        print(f"Error deleting history entry ({history_type}) for member {member_id}: {e}")
        return jsonify({"error": f"Failed to delete history entry: {str(e)}"}), 500


# --- Serve SPA ---
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def serve_spa(path):
    if path != "" and os.path.exists(os.path.join(app.static_folder, path)):
        return send_from_directory(app.static_folder, path)
    else:
        return send_from_directory(app.static_folder, 'index.html')

def shutdown_app():
    print("Application shutting down...")
    db_utils.create_checkpoint()
    print("Final database checkpoint successful.")
    scheduler.shutdown()
    print("Scheduler shut down.")

atexit.register(shutdown_app)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, port=port, use_reloader=False) # use_reloader=False is important for APScheduler