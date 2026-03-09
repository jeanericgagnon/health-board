PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS whoop_daily (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  day TEXT NOT NULL UNIQUE,
  pulled_at_utc TEXT,
  user_id INTEGER,
  recovery_score REAL,
  hrv_rmssd REAL,
  resting_hr REAL,
  sleep_performance REAL,
  sleep_efficiency REAL,
  sleep_consistency REAL,
  strain REAL,
  weight_kg REAL,
  raw_json TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS swim_daily (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  day TEXT NOT NULL,
  distance_value REAL NOT NULL,
  unit TEXT NOT NULL DEFAULT 'yd',
  source TEXT NOT NULL DEFAULT 'apple_health_shortcut',
  message_ts TEXT,
  raw_text TEXT,
  created_at TEXT DEFAULT (datetime('now')),
  UNIQUE(day, source)
);

CREATE TABLE IF NOT EXISTS notes_daily (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  day TEXT NOT NULL,
  category TEXT,
  note TEXT NOT NULL,
  created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_swim_day ON swim_daily(day);
CREATE INDEX IF NOT EXISTS idx_notes_day ON notes_daily(day);
