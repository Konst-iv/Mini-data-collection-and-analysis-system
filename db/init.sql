CREATE DATABASE redash_internal;

CREATE TABLE IF NOT EXISTS gamer (
    id SERIAL PRIMARY KEY,
    nickname VARCHAR(50) UNIQUE NOT NULL,
    level INTEGER DEFAULT 1,
    email VARCHAR(100) UNIQUE,
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS action (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS points (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS game_log (
    id SERIAL PRIMARY KEY,
    gamer_id INT REFERENCES gamer(id),
    action_id INT REFERENCES action(id),
    points_id INT REFERENCES points(id),
    reward_value INT NOT NULL,
    current_level INT NOT NULL,
    session_duration_sec INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO action (name) VALUES ('boss_kill'), ('resource_gather'), ('quest_complete');
INSERT INTO points (name) VALUES ('exp'), ('gold'), ('crystals');
