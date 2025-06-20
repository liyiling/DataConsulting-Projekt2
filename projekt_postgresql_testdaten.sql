-- Anlage A: SQL-Skripte zur Testdatengenerierung (PostgreSQL)

-- 1. 100.000 Aufträge einfügen
INSERT INTO auftrag (kundeid, bestelldatum, geplantesfertigstellungsdatum, tatsaechlichesfertigstellungsdatum)
SELECT 
  (random() * 9 + 1)::int,
  date '2024-04-01' + (random() * 30)::int,
  date '2024-05-01' + (random() * 30)::int,
  NULL
FROM generate_series(1, 100000);

-- 2. 100.000 Auftragspositionen einfügen
INSERT INTO auftrag_position (auftragid, zahnradtypid, bestelltemenge, geplanteproduktionsdauer)
SELECT 
  (random() * 99999 + 1)::int,
  (random() * 19 + 1)::int,
  (random() * 100 + 1)::int,
  (random() * 150 + 50)::int
FROM generate_series(1, 100000);

-- 3. Produktionsdatensätze mit Ausschuss generieren
DO $$
DECLARE
    i INT := 1;
    max_id INT := 100000;
    prod_start TIMESTAMP;
    prod_end TIMESTAMP;
    pos_id INT;
    masch_id INT;
    is_ausschuss BOOLEAN;
BEGIN
    WHILE i <= max_id LOOP
        pos_id := (random() * 99999 + 1)::INT;
        masch_id := (random() * 9 + 1)::INT;
        prod_start := TIMESTAMP '2024-05-01 08:00:00' + ((random() * 30 * 24 * 60)::INT || ' minutes')::INTERVAL;
        prod_end := prod_start + ((random() * 55 + 5)::INT || ' minutes')::INTERVAL;
        is_ausschuss := (random() < 0.1);
        INSERT INTO produktion (auftragpositionid, maschineid, startzeit, endzeit, ausschuss)
        VALUES (pos_id, masch_id, prod_start, prod_end, is_ausschuss);
        i := i + 1;
    END LOOP;
END $$;

-- 4. Sequenz für ProduktionID korrigieren (optional)
SELECT setval('produktion_produktionid_seq', (SELECT MAX(produktionid) FROM produktion));
