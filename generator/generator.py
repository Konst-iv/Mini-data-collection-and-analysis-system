import os
import time
import random
import psycopg2
from psycopg2.extras import RealDictCursor
from faker import Faker

fake = Faker()

DB_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
    'host': os.getenv('DB_HOST', 'db')
}

def get_connection():
    while True:
        try:
            return psycopg2.connect(**DB_CONFIG)
        except:
            time.sleep(2)

def run():
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT COUNT(*) FROM gamer")
    if cur.fetchone()['count'] < 100:
        for _ in range(100):
            cur.execute(
                "INSERT INTO gamer (nickname, email) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (fake.user_name(), fake.email())
            )
        conn.commit()

    cur.execute("SELECT id FROM gamer")
    gamers = [r['id'] for r in cur.fetchall()]
    cur.execute("SELECT id FROM action")
    actions = [r['id'] for r in cur.fetchall()]
    cur.execute("SELECT id FROM points")
    points = [r['id'] for r in cur.fetchall()]

    for i in range(2000):
        try:
            cur.execute("""
                INSERT INTO game_log (gamer_id, action_id, points_id, reward_value, current_level, session_duration_sec)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                random.choice(gamers), 
                random.choice(actions), 
                random.choice(points), 
                random.randint(10, 1000), 
                random.randint(1, 50), 
                random.randint(30, 3600)
            ))
            if i % 100 == 0:
                conn.commit()
        except:
            continue

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    run()