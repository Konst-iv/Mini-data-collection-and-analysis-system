import os
import time
import random
import psycopg2
from psycopg2.extras import RealDictCursor
from faker import Faker

fake = Faker()

DB_CONFIG = {
    'dbname': os.getenv('POSTGRES_DB', 'game_data'),
    'user': os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD', 'super_secret_password'),
    'host': os.getenv('DB_HOST', 'db'),
    'port': int(os.getenv('DB_PORT', 5432))
}

def get_connection():
    """Функция для безопасного подключения к БД с ожиданием готовности"""
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
                "INSERT INTO gamer (nickname, email, level) VALUES (%s, %s, 1) ON CONFLICT DO NOTHING",
                (fake.user_name(), fake.email())
            )
        conn.commit()

    cur.execute("SELECT id, level FROM gamer")
    rows = cur.fetchall()
    gamer_levels = {r['id']: r['level'] for r in rows}

    cur.execute("SELECT id FROM action")
    actions = [r['id'] for r in cur.fetchall()]
    cur.execute("SELECT id FROM points")
    points = [r['id'] for r in cur.fetchall()]

    if not actions or not points:
        print("Ошибка: Справочники (action, points) пусты! Проверьте init.sql")
        return

    print("Генерация данных запущена...")
    
    while True:
        try:
            for g_id in gamer_levels.keys():
                if random.random() < 0.05 and gamer_levels[g_id] < 50:
                    gamer_levels[g_id] += 1
                    cur.execute("UPDATE gamer SET level = %s WHERE id = %s", (gamer_levels[g_id], g_id))

                cur.execute("""
                    INSERT INTO game_log (
                        gamer_id, 
                        action_id, 
                        points_id, 
                        reward_value, 
                        current_level, 
                        session_duration_sec
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    g_id, 
                    random.choice(actions), 
                    random.choice(points), 
                    random.randint(10, 500), 
                    gamer_levels[g_id],      
                    random.randint(60, 1800) 
                ))
            
            conn.commit()
            print(f"Пакет данных записан. Уровни синхронизированы с БД.")
            time.sleep(1) 

        except Exception as e:
            print(f"Ошибка при генерации: {e}")
            conn.rollback()
            time.sleep(5)

if __name__ == '__main__':
    run()