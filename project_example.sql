CREATE TABLE sensor_definitions (
    address text,
    data_source text,
    PRIMARY KEY (data_source, address)
);

CREATE TABLE measurements (
    address text,
    data_source text,
    timestamp timestamp without time zone,
    value integer,
    PRIMARY KEY (data_source, address, timestamp),
    CONSTRAINT measurements_sensor_fkey
      FOREIGN KEY(data_source, address) 
      REFERENCES sensor_definitions(data_source, address)
);

INSERT INTO sensor_definitions
SELECT g::text, 
        CASE 
            WHEN random() < 0.4 THEN 'source_a'
            WHEN random() < 0.7 THEN 'source_b'
            ELSE 'source_c'
        END
FROM generate_series(1, 100) g;

-- this query can take some time!
INSERT INTO measurements(timestamp, address, data_source, value)
SELECT timestamp, sd.address, sd.data_source, val 
FROM
    (
        SELECT *, 
            ceil(random() * 100) as address, 
            ceil(random() * 1000) as val
        FROM generate_series('2020-01-22', '2021-01-21', interval '1 sec') timestamp
    ) q
INNER JOIN sensor_definitions sd ON sd.address = q.address::text;

-- actual queries to test at other database
SELECT  data_source, address, 
        count(*), avg(value),
        percentile_cont(0.5) WITHIN GROUP (ORDER BY value) as median,
        percentile_cont(0.25) WITHIN GROUP (ORDER BY value) as first_quartile
FROM measurements
WHERE timestamp between '2020-03-01 00:00:00' and '2020-03-31 00:00'
GROUP BY data_source, address;

SELECT  day_interval, data_source, address, 
        count(*), avg(value),
        percentile_cont(0.5) WITHIN GROUP (ORDER BY value) as median,
        percentile_cont(0.25) WITHIN GROUP (ORDER BY value) as first_quartile
FROM generate_series('2020-03-01 00:00:00', '2020-03-31 00:00', interval '1 day') day_interval
INNER JOIN measurements m ON m.timestamp > day_interval AND m.timestamp <= day_interval + interval '1 day'
GROUP BY day_interval, data_source, address;