-- Create the database if it doesn't exist
-- Check if the database exists
SELECT datname FROM pg_catalog.pg_database WHERE datname = 'rocket_data';

-- If the database doesn't exist, create it
DO $$BEGIN
  IF NOT EXISTS (SELECT datname FROM pg_catalog.pg_database WHERE datname = 'rocket_data') THEN
    CREATE DATABASE rocket_data;
  END IF;
END$$;

-- Connect to the database
\c rocket_data;

-- Create a table if it doesn't exist
CREATE TABLE IF NOT EXISTS speed_change (
  channel uuid,
  messageNumber int,
  messageType varchar(80),
  messageTime timestamp with time zone,
  changeValue int
);

CREATE TABLE IF NOT EXISTS rocket_exploded (
  channel uuid,
  messageNumber int,
  messageType varchar(80),
  messageTime timestamp with time zone,
  reason varchar(100)
);

CREATE TABLE IF NOT EXISTS mission_changed (
  channel uuid,
  messageNumber int,
  messageType varchar(80),
  messageTime timestamp with time zone,
  newMission varchar(100)
);

CREATE TABLE IF NOT EXISTS rocket_launched (
  launchId serial PRIMARY KEY,
  channel uuid,
  messageNumber int,
  messageType varchar(80),
  messageTime timestamp with time zone,
  type varchar(80),
  launchSpeed int,
  mission varchar(80)
);

COMMIT;