import os
import time
import random
import psycopg2
from psycopg2.extras import RealDictCursor
from faker import Faker
from datetime import datetime, timedelta

fake = Faker()

DB_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB', 'game_data'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'super_secret_password'),
    'host': os.getenv('DB_HOST', 'db'),
    'port': int(os.getenv('DB_PORT', 5432))
}

def get_connection():
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            return conn
        except Exception as e:
            print(f"Ожидание базы данных... {e}")
            time.sleep(2)

def run():
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("SELECT COUNT(*) FROM gamer")
    if cur.fetchone()['count'] < 20:
        print("Инициализация начальных игроков...")
        for _ in range(20):
            cur.execute(
                "INSERT INTO gamer (nickname, email, level) VALUES (%s, %s, 1)",
                (fake.user_name(), fake.email())
            )
        conn.commit()

    cur.execute("SELECT id FROM action")
    actions = [row['id'] for row in cur.fetchall()]
    
    cur.execute("SELECT id, level FROM gamer")
    gamer_levels = {row['id']: row['level'] for row in cur.fetchall()}

 
    virtual_time = datetime.now() - timedelta(hours=12)

    print("Генерация данных запущена (Механика: Виртуальное время + Прокачка)...")
    
    while True:
        try:
            for g_id in list(gamer_levels.keys()):
                action_id = random.choice(actions)
                session_time_sec = random.randint(1800, 7200)
                current_lvl = gamer_levels[g_id]

                virtual_time += timedelta(seconds=random.randint(60, 300)) 
                
   
                exp_gain = (session_time_sec // 60) * random.randint(5, 15)
                
                cur.execute("""
                    INSERT INTO game_log (gamer_id, action_id, points_id, reward_value, current_level, session_duration_sec, created_at)
                    VALUES (%s, %s, 1, %s, %s, %s, %s)
                """, (g_id, action_id, exp_gain, current_lvl, session_time_sec, virtual_time))

                if random.random() < 0.7:
                    extra_point_id = random.choice([2, 3]) 
                    cur.execute("""
                        INSERT INTO game_log (gamer_id, action_id, points_id, reward_value, current_level, session_duration_sec, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (g_id, action_id, extra_point_id, random.randint(10, 500), current_lvl, session_time_sec, virtual_time))

                cur.execute("SELECT SUM(reward_value) FROM game_log WHERE gamer_id = %s AND points_id = 1", (g_id,))
                total_exp = cur.fetchone()['sum'] or 0
                
                new_level = int(total_exp // 2000) + 1
                new_level = min(new_level, 50) 

                if new_level > current_lvl:
                    gamer_levels[g_id] = new_level
                    cur.execute("UPDATE gamer SET level = %s WHERE id = %s", (new_level, g_id))
                    print(f"🌟 [{virtual_time.strftime('%H:%M')}] Игрок {g_id} достиг {new_level} уровня!")
            
            conn.commit()
            time.sleep(1) 

        except Exception as e:
            print(f"Ошибка генерации: {e}")
            conn.rollback()
            time.sleep(5)

if __name__ == "__main__":
    run()