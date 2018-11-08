CREATE TABLE intvals(val int, color text);

-- Test empty table
SELECT median(val) FROM intvals;

-- Integers with odd number of values
INSERT INTO intvals VALUES
       (1, 'a'),
       (2, 'c'),
       (9, 'b'),
       (7, 'c'),
       (2, 'd'),
       (-3, 'd'),
       (2, 'e');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Integers with NULLs and even number of values
INSERT INTO intvals VALUES
       (99, 'a'),
       (NULL, 'a'),
       (NULL, 'e'),
       (NULL, 'b'),
       (7, 'c'),
       (0, 'd');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Text values
CREATE TABLE textvals(val text, color int);

INSERT INTO textvals VALUES
       ('erik', 1),
       ('mat', 3),
       ('rob', 8),
       ('david', 9),
       ('lee', 2);

SELECT * FROM textvals ORDER BY val;
SELECT median(val) FROM textvals;

-- Test large table with timestamps
CREATE TABLE timestampvals (val timestamptz);

INSERT INTO timestampvals(val)
SELECT TIMESTAMP 'epoch' + (i * INTERVAL '1 second')
FROM generate_series(0, 100000) as T(i);

SELECT median(val) FROM timestampvals;

-- interpolate numeric values
with numerics as (select generate_series(0.0, 101.0) num)
select median(num::numeric) from numerics order by random();

-- pick middle numeric value
with numerics as (select generate_series(0.0, 100.0) num)
select median(num::numeric) from numerics order by random();

-- pick middle bigint values
with bigints as (select generate_series(0, 100) num)
select median(num::bigint) from bigints order by random();

-- interpolate bigint values
with bigints as (select generate_series(0, 101) num)
select median(num::bigint) from bigints order by random();

-- interpolate real values
with reals as (select generate_series(0.0, 101.0) num)
select median(num::real) from reals order by random();

-- pick middle real values
with reals as (select generate_series(0.0, 100.0) num)
select median(num::real) from reals order by random();

-- pick middle smallint values
with smallints as (select generate_series(0, 100) num)
select median(num::smallint) from smallints order by random();

-- interpolate smallint values
with smallints as (select generate_series(0, 101) num)
select median(num::smallint) from smallints order by random();

-- interpolate double precision values
with double_precision as (select generate_series(0.0, 101.0) num)
select median(cast(num as double precision)) from double_precision order by random();

-- pick middle double precision values
with double_precision as (select generate_series(0.0, 100.0) num)
select median(cast(num as double precision)) from double_precision order by random();

-- pick middle interval values
with smallints as (select generate_series(0, 100) num)
select median(num * '1 minute'::interval) from smallints order by random();

-- interpolate interval values
with smallints as (select generate_series(0, 101) num)
select median(num * '1 minute'::interval) from smallints order by random();

-- huge data values
with bigints as (select generate_series(0, 20000000) num)
select median(num::bigint) from bigints order by random();

-- does not interpolate text
INSERT INTO textvals VALUES
       ('sam', 1);
SELECT * FROM textvals ORDER BY val;
SELECT median(val) FROM textvals;