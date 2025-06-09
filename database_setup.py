import sqlite3
import os
import random
import string
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), 'gym_data.sqlite')

def generate_id():
    # Generates a random ID similar to the JS version '_' + Math.random().toString(36).substr(2, 9)
    # For more robustness, consider using uuid.uuid4().hex
    return '_' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=9))

def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row # Access columns by name
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db(populate_with_sample_data=False):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS members (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            gender TEXT,
            mobile TEXT,
            email TEXT,
            cnic TEXT,
            admissionFee REAL,
            joinDate TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS member_status_history (
            id TEXT PRIMARY KEY,
            memberId TEXT NOT NULL,
            value TEXT NOT NULL,
            effectiveDate TEXT NOT NULL,
            FOREIGN KEY (memberId) REFERENCES members(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS member_monthly_fee_history (
            id TEXT PRIMARY KEY,
            memberId TEXT NOT NULL,
            value REAL NOT NULL,
            effectiveDate TEXT NOT NULL,
            FOREIGN KEY (memberId) REFERENCES members(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS member_payment_cycle_day_history (
            id TEXT PRIMARY KEY,
            memberId TEXT NOT NULL,
            value INTEGER NOT NULL,
            effectiveDate TEXT NOT NULL,
            FOREIGN KEY (memberId) REFERENCES members(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS payments (
            id TEXT PRIMARY KEY,
            memberId TEXT NOT NULL,
            date TEXT NOT NULL,
            appliedToPeriodStartDate TEXT,
            paymentType TEXT NOT NULL,
            amount REAL NOT NULL,
            FOREIGN KEY (memberId) REFERENCES members(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS writeoffs (
            id TEXT PRIMARY KEY,
            memberId TEXT NOT NULL,
            periodStartDate TEXT NOT NULL,
            periodEndDate TEXT NOT NULL,
            amount REAL NOT NULL,
            date TEXT NOT NULL,
            notes TEXT,
            FOREIGN KEY (memberId) REFERENCES members(id) ON DELETE CASCADE
        );
    """)
    conn.commit()
    print("Database schema checked/initialized.")

    if populate_with_sample_data:
        cursor.execute("SELECT COUNT(*) as count FROM members")
        count = cursor.fetchone()['count']
        if count == 0:
            print("Populating database with sample data...")
            today = datetime.utcnow() # Use UTC for consistency if dates are stored as UTC

            def to_input_date_string(date_obj):
                return date_obj.strftime('%Y-%m-%d')

            def add_days(date_obj, days):
                return date_obj + timedelta(days=days)

            d1 = to_input_date_string(add_days(today, -90))
            d2 = to_input_date_string(add_days(today, -60))
            d3 = to_input_date_string(add_days(today, -30))

            sample_members = [
                { 'id': generate_id(), 'name': 'Aisha Khan', 'gender': 'Female', 'mobile': '03001234567', 'email': 'aisha.k@example.com', 'cnic': '35202-1234567-1', 'admissionFee': 2000, 'joinDate': d1,
                    'monthlyFeeHistory': [{ 'id': generate_id(), 'value': 5000, 'effectiveDate': d1 }],
                    'statusHistory': [{ 'id': generate_id(), 'value': 'Active', 'effectiveDate': d1 }],
                    'paymentCycleDayHistory': [{ 'id': generate_id(), 'value': 15, 'effectiveDate': d1 }]
                },
                { 'id': generate_id(), 'name': 'Bilal Ahmed', 'gender': 'Male', 'mobile': '03219876543', 'email': 'bilal.ahmed@email.com', 'cnic': '35201-7654321-2', 'admissionFee': 1500, 'joinDate': d2,
                    'monthlyFeeHistory': [{ 'id': generate_id(), 'value': 4500, 'effectiveDate': d2 }],
                    'statusHistory': [
                        { 'id': generate_id(), 'value': 'Active', 'effectiveDate': d2 },
                        { 'id': generate_id(), 'value': 'Inactive', 'effectiveDate': d3 }
                    ],
                    'paymentCycleDayHistory': [{ 'id': generate_id(), 'value': 10, 'effectiveDate': d2 }]
                },
            ]

            try:
                conn.execute("BEGIN TRANSACTION")
                for member in sample_members:
                    cursor.execute("""
                        INSERT INTO members (id, name, gender, mobile, email, cnic, admissionFee, joinDate)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (member['id'], member['name'], member['gender'], member['mobile'], member['email'], member['cnic'], member['admissionFee'], member['joinDate']))

                    for h_entry in member['statusHistory']:
                        cursor.execute("INSERT INTO member_status_history (id, memberId, value, effectiveDate) VALUES (?, ?, ?, ?)",
                                       (h_entry.get('id', generate_id()), member['id'], h_entry['value'], h_entry['effectiveDate']))
                    for h_entry in member['monthlyFeeHistory']:
                        cursor.execute("INSERT INTO member_monthly_fee_history (id, memberId, value, effectiveDate) VALUES (?, ?, ?, ?)",
                                       (h_entry.get('id', generate_id()), member['id'], h_entry['value'], h_entry['effectiveDate']))
                    for h_entry in member['paymentCycleDayHistory']:
                         cursor.execute("INSERT INTO member_payment_cycle_day_history (id, memberId, value, effectiveDate) VALUES (?, ?, ?, ?)",
                                       (h_entry.get('id', generate_id()), member['id'], h_entry['value'], h_entry['effectiveDate']))

                    if member['admissionFee'] > 0:
                        cursor.execute("""
                            INSERT INTO payments (id, memberId, date, appliedToPeriodStartDate, paymentType, amount)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (generate_id(), member['id'], member['joinDate'], member['joinDate'], 'Admission Fee', member['admissionFee']))
                    
                    if member['name'] == 'Aisha Khan': # Example initial monthly payment
                        fee_entry = next((f for f in member['monthlyFeeHistory'] if f['effectiveDate'] == member['joinDate']), None)
                        if fee_entry:
                            cursor.execute("""
                                INSERT INTO payments (id, memberId, date, appliedToPeriodStartDate, paymentType, amount)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (generate_id(), member['id'], member['joinDate'], member['joinDate'], 'Monthly Fee', fee_entry['value']))
                conn.commit()
                print("Sample data populated.")
            except Exception as e:
                conn.rollback()
                print(f"Error populating sample data: {e}")
        else:
            print("Database already contains data. Skipping sample data population.")
    
    conn.close()

if __name__ == '__main__':
    # Initialize DB with sample data if run directly
    init_db(populate_with_sample_data=True)