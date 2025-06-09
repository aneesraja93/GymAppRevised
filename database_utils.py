import sqlite3
import os
import random
import string
from contextlib import contextmanager

DB_PATH = os.path.join(os.path.dirname(__file__), 'gym_data.sqlite')

def generate_id():
    return '_' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=9))

@contextmanager
def get_db_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
    finally:
        conn.close()

def get_all_data():
    with get_db_connection() as conn:
        cursor = conn.cursor()
        members_list = [dict(row) for row in cursor.execute('SELECT * FROM members ORDER BY name').fetchall()]
        payments_list = [dict(row) for row in cursor.execute('SELECT * FROM payments ORDER BY date DESC').fetchall()]
        writeoffs_list = [dict(row) for row in cursor.execute('SELECT * FROM writeoffs ORDER BY date DESC').fetchall()]

        status_histories = {}
        for row in cursor.execute('SELECT * FROM member_status_history ORDER BY effectiveDate, id').fetchall():
            if row['memberId'] not in status_histories:
                status_histories[row['memberId']] = []
            status_histories[row['memberId']].append(dict(row))

        fee_histories = {}
        for row in cursor.execute('SELECT * FROM member_monthly_fee_history ORDER BY effectiveDate, id').fetchall():
            if row['memberId'] not in fee_histories:
                fee_histories[row['memberId']] = []
            fee_histories[row['memberId']].append(dict(row))
        
        cycle_day_histories = {}
        for row in cursor.execute('SELECT * FROM member_payment_cycle_day_history ORDER BY effectiveDate, id').fetchall():
            if row['memberId'] not in cycle_day_histories:
                cycle_day_histories[row['memberId']] = []
            cycle_day_histories[row['memberId']].append(dict(row))

        for member in members_list:
            member['statusHistory'] = status_histories.get(member['id'], [])
            member['monthlyFeeHistory'] = fee_histories.get(member['id'], [])
            member['paymentCycleDayHistory'] = cycle_day_histories.get(member['id'], [])
            
        return {"members": members_list, "payments": payments_list, "writeoffs": writeoffs_list}

def get_member_by_id(member_id):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        member_row = cursor.execute('SELECT * FROM members WHERE id = ?', (member_id,)).fetchone()
        if member_row:
            member = dict(member_row)
            member['statusHistory'] = [dict(r) for r in cursor.execute('SELECT * FROM member_status_history WHERE memberId = ? ORDER BY effectiveDate, id', (member_id,)).fetchall()]
            member['monthlyFeeHistory'] = [dict(r) for r in cursor.execute('SELECT * FROM member_monthly_fee_history WHERE memberId = ? ORDER BY effectiveDate, id', (member_id,)).fetchall()]
            member['paymentCycleDayHistory'] = [dict(r) for r in cursor.execute('SELECT * FROM member_payment_cycle_day_history WHERE memberId = ? ORDER BY effectiveDate, id', (member_id,)).fetchall()]
            return member
        return None

def upsert_member(member_data):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        member_id = member_data.get('id', generate_id()) # Ensure ID exists
        member_data['id'] = member_id

        cursor.execute("""
            INSERT INTO members (id, name, gender, mobile, email, cnic, admissionFee, joinDate)
            VALUES (:id, :name, :gender, :mobile, :email, :cnic, :admissionFee, :joinDate)
            ON CONFLICT(id) DO UPDATE SET
                name = excluded.name, gender = excluded.gender, mobile = excluded.mobile, email = excluded.email,
                cnic = excluded.cnic, admissionFee = excluded.admissionFee, joinDate = excluded.joinDate
        """, member_data)

        history_tables_columns = {
            'statusHistory': ('member_status_history', ['value', 'effectiveDate']),
            'monthlyFeeHistory': ('member_monthly_fee_history', ['value', 'effectiveDate']),
            'paymentCycleDayHistory': ('member_payment_cycle_day_history', ['value', 'effectiveDate'])
        }

        for key, (table_name, columns) in history_tables_columns.items():
            cursor.execute(f"DELETE FROM {table_name} WHERE memberId = ?", (member_id,))
            if member_data.get(key) and isinstance(member_data[key], list):
                for entry in member_data[key]:
                    entry['id'] = entry.get('id') or generate_id()
                    entry['memberId'] = member_id
                    cols_str = ', '.join(['id', 'memberId'] + columns)
                    placeholders = ', '.join(['?'] * (len(columns) + 2))
                    values_to_insert = [entry['id'], entry['memberId']] + [entry.get(col) for col in columns]
                    cursor.execute(f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})", values_to_insert)
        conn.commit()
        return get_member_by_id(member_id)


def delete_member(member_id):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        result = cursor.execute('DELETE FROM members WHERE id = ?', (member_id,))
        conn.commit()
        return result.rowcount > 0

def upsert_payment(payment_data):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        payment_data['id'] = payment_data.get('id') or generate_id()
        cursor.execute("""
            INSERT INTO payments (id, memberId, date, appliedToPeriodStartDate, paymentType, amount)
            VALUES (:id, :memberId, :date, :appliedToPeriodStartDate, :paymentType, :amount)
            ON CONFLICT(id) DO UPDATE SET
                memberId = excluded.memberId, date = excluded.date, 
                appliedToPeriodStartDate = excluded.appliedToPeriodStartDate, 
                paymentType = excluded.paymentType, amount = excluded.amount
        """, payment_data)
        conn.commit()
        # Return the data that was passed in, as the original JS does (or fetch it)
        return payment_data 

def delete_payment(payment_id):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        result = cursor.execute('DELETE FROM payments WHERE id = ?', (payment_id,))
        conn.commit()
        return result.rowcount > 0

def upsert_writeoff(writeoff_data):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        writeoff_data['id'] = writeoff_data.get('id') or generate_id()
        cursor.execute("""
            INSERT INTO writeoffs (id, memberId, periodStartDate, periodEndDate, amount, date, notes)
            VALUES (:id, :memberId, :periodStartDate, :periodEndDate, :amount, :date, :notes)
            ON CONFLICT(id) DO UPDATE SET
                memberId = excluded.memberId, periodStartDate = excluded.periodStartDate, periodEndDate = excluded.periodEndDate,
                amount = excluded.amount, date = excluded.date, notes = excluded.notes
        """, writeoff_data)
        conn.commit()
        return writeoff_data

def delete_writeoff(writeoff_id):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        result = cursor.execute('DELETE FROM writeoffs WHERE id = ?', (writeoff_id,))
        conn.commit()
        return result.rowcount > 0

def update_history_entry(member_id, entry_id, history_type, new_effective_date):
    table_map = {
        'statusHistory': 'member_status_history',
        'monthlyFeeHistory': 'member_monthly_fee_history',
        'paymentCycleDayHistory': 'member_payment_cycle_day_history'
    }
    if history_type not in table_map:
        raise ValueError('Invalid history type for update')
    table_name = table_map[history_type]

    with get_db_connection() as conn:
        cursor = conn.cursor()
        result = cursor.execute(f"UPDATE {table_name} SET effectiveDate = ? WHERE id = ? AND memberId = ?",
                                (new_effective_date, entry_id, member_id))
        conn.commit()
        if result.rowcount > 0:
            return get_member_by_id(member_id)
        print(f"No changes made for history update: memberId={member_id}, entryId={entry_id}, historyType={history_type}")
        return get_member_by_id(member_id) # Return member anyway

def delete_specific_history_entry(member_id, entry_id, history_type):
    table_map = {
        'statusHistory': 'member_status_history',
        'monthlyFeeHistory': 'member_monthly_fee_history',
        'paymentCycleDayHistory': 'member_payment_cycle_day_history'
    }
    if history_type not in table_map:
        raise ValueError('Invalid history type for deletion')
    table_name = table_map[history_type]

    with get_db_connection() as conn:
        cursor = conn.cursor()
        count_row = cursor.execute(f"SELECT COUNT(*) as count FROM {table_name} WHERE memberId = ?", (member_id,)).fetchone()
        if count_row['count'] <= 1:
            raise ValueError("Cannot delete the only history entry of this type for the member.")
        
        result = cursor.execute(f"DELETE FROM {table_name} WHERE id = ? AND memberId = ?", (entry_id, member_id))
        conn.commit()
        if result.rowcount > 0:
            return get_member_by_id(member_id)
        print(f"No entry deleted: memberId={member_id}, entryId={entry_id}, historyType={history_type}")
        return get_member_by_id(member_id)

def create_checkpoint():
    with get_db_connection() as conn:
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            print("Database checkpoint (TRUNCATE) successful.")
        except sqlite3.Error as e:
            print(f"Error during WAL checkpoint (TRUNCATE): {e}")
            try:
                conn.execute("PRAGMA wal_checkpoint(FULL)")
                print("Database checkpoint (FULL) successful after TRUNCATE failed.")
            except sqlite3.Error as e_fallback:
                print(f"Error during WAL checkpoint (FULL) fallback: {e_fallback}")
                raise e_fallback
        conn.commit() # Ensure checkpoint is written