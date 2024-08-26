CREATE Extension IF NOT EXISTS MobilityDB CASCADE;

CREATE TABLE vesselcountbyareaandtime (
    area STBOX NOT NULL,
    count INTEGER NOT NULL,
    time TIMESTAMP NOT NULL
);
