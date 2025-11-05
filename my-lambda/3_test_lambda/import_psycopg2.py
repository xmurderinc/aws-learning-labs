import psycopg2

def lambda_handler(event, context):
    conn = psycopg2.connect(
        host="database-1.cr4uuou0e8u3.us-east-2.rds.amazonaws.com",
        port=5432,
        database="postgres",
        user="postgres",
        password="BCLYGdgXA0O2og00VVsb67"
    )
    cur = conn.cursor()
    cur.execute("SELECT 1;")
    result = cur.fetchone()
    conn.close()
    return {"result": result}