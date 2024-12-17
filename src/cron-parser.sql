-- ================================================================
-- Cron expression parser for PostgreSQL
--
-- Copyright (c) 2024 Yelyzaveta Veis <liza.veis14@gmail.com>
--
-- Licensed under the MIT License. See LICENSE file in the repository.
-- ================================================================

CREATE SCHEMA IF NOT EXISTS cron_parser;

-- Description: Calculates the next applicable value in a cron field based on constraints
-- Parameters:
--   current_value: Starting point for the search
--   cron_field: Cron field definition (e.g., "*/5")
--   min_value: Minimum value allowed in the field
--   max_value: Maximum value allowed in the field
-- Returns:
--   Next valid value or NULL if none exist
CREATE OR REPLACE FUNCTION cron_parser.get_cron_field_next_value(
	current_value INT,
	cron_field TEXT,
	min_value INT,
	max_value INT
) RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
	next_value INT;
	parts TEXT[];
	part TEXT;
	part_range TEXT;
	part_range_min INT;
	part_range_max INT;
	part_step INT;
	part_step_next_value INT;
	part_value INT;
BEGIN
	-- Validate current value
	IF current_value < min_value OR current_value > max_value THEN
		RAISE EXCEPTION 'Invalid range: current value % for field % is out of range %-%', current_value, cron_field, min_value, max_value;
	END IF;

	-- Split cron field into individual parts
	parts := STRING_TO_ARRAY(cron_field, ',');

	-- Select the smallest value for each part that equals or is more than current value
	FOREACH part IN ARRAY parts LOOP
		-- Handle step notation (/)
		IF POSITION('/' IN part) > 0 THEN
			part_range := SPLIT_PART(part, '/', 1);
			part_step := SPLIT_PART(part, '/', 2)::INT;

			-- Validate step value
			IF part_step < min_value OR part_step > max_value THEN
				RAISE EXCEPTION 'Invalid range: value % in field % is out of range %-%', part_step, cron_field, min_value, max_value;
			END IF;

			-- Handle range (-) within step (e.g., "1-10/2")
			IF POSITION('-' IN part_range) > 0 THEN
				part_range_min := SPLIT_PART(part_range, '-', 1)::INT;
				part_range_max := SPLIT_PART(part_range, '-', 2)::INT;
			-- Handle wildcard (*) within step (e.g., "*/5")
			ELSIF part_range = '*' THEN
				part_range_min := min_value;
				part_range_max := max_value;
			-- If single numeric value is provided, use it as min boundary (e.g., "1/3")
			ELSE
				part_range_min := part_range::INT;
				part_range_max := max_value;
			END IF;

			-- Validate range values
			IF part_range_min < min_value OR part_range_min > max_value THEN
				RAISE EXCEPTION 'Invalid range: value % in field % is out of range %-%', part_range_min, cron_field, min_value, max_value;
			END IF;

			IF part_range_max < min_value OR part_range_max > max_value THEN
				RAISE EXCEPTION 'Invalid range: value % in field % is out of range %-%', part_range_max, cron_field, min_value, max_value;
			END IF;

			-- Calculate next step-based value within the range
			part_step_next_value := current_value - ((current_value - part_range_min) % part_step);

			IF part_step_next_value < current_value THEN
				part_step_next_value := part_step_next_value + part_step;
			END IF;

			IF part_step_next_value >= part_range_min AND part_step_next_value <= part_range_max THEN
				next_value := LEAST(next_value, part_step_next_value);
			ELSIF current_value < part_range_min THEN
				next_value := LEAST(next_value, part_range_min);
			END IF;

		-- Handle range notation (-)
		ELSIF POSITION('-' IN part) > 0 THEN
			part_range_min := SPLIT_PART(part, '-', 1)::INT;
			part_range_max := SPLIT_PART(part, '-', 2)::INT;

			-- Validate range values
			IF part_range_min < min_value OR part_range_min > max_value THEN
				RAISE EXCEPTION 'Invalid range: value % in field % is out of range %-%', part_range_min, cron_field, min_value, max_value;
			END IF;

			IF part_range_max < min_value OR part_range_max > max_value THEN
				RAISE EXCEPTION 'Invalid range: value % in field % is out of range %-%', part_range_max, cron_field, min_value, max_value;
			END IF;

			-- Calculate next value within the range
			IF current_value >= part_range_min AND current_value <= part_range_max THEN
				next_value := LEAST(next_value, current_value);
			ELSIF current_value < part_range_min THEN
				next_value := LEAST(next_value, part_range_min);
			END IF;

		-- Handle wildcard notation (*)
		ELSIF part = '*' THEN
			next_value := LEAST(next_value, current_value);

		-- Handle single numeric values
		ELSE
			part_value := part::INT;

			-- Validate the value
			IF part_value < min_value OR part_value > max_value THEN
				RAISE EXCEPTION 'Invalid range: value % is out of range %-%', part_value, min_value, max_value;
			END IF;

			IF part_value >= current_value THEN
				next_value := LEAST(next_value, part_value);
			END IF;
		END IF;
	END LOOP;

	-- Return NULL if no valid value is found
	IF next_value > max_value THEN
		RETURN NULL;
	END IF;

	RETURN next_value;
END;
$$;

-- Description: Calculates the next run date for a cron expression
-- Parameters:
--   cron_expression: Cron pattern (e.g., "*/5 * * * *")
--   reference_date: Starting point for calculation
--   max_search_depth_months: Maximum number of months to search forward
-- Returns:
--   Next run date as a TIMESTAMPTZ or NULL if no valid date is found or exception occurs
CREATE OR REPLACE FUNCTION cron_parser.get_cron_next_run_date(
	cron_expression TEXT,
	reference_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
	max_search_depth_months INT DEFAULT 12
) RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
AS $$
DECLARE
	cron_fields TEXT[];
	cron_minute_index INT := 1;
	cron_hour_index INT := 2;
	cron_day_of_month_index INT := 3;
	cron_month_index INT := 4;
	cron_day_of_week_index INT := 5;

	cron_field_min INT[] := '{0, 0, 1, 1, 0}';
	cron_field_max INT[] := '{59, 23, 31, 12, 6}';

	next_run_date TIMESTAMPTZ := DATE_TRUNC('minute', reference_date);
	next_run_date_by_week TIMESTAMPTZ;

	next_minute INT;
	next_hour INT;
	next_day_of_month INT;
	next_day_of_month_by_week INT;
	next_month INT;
	next_day_of_week INT;

	last_day_of_month INT;
	search_depth_interval INTERVAL;
	search_depth_months INT;
BEGIN
	-- Split cron expression into fields
	cron_fields := STRING_TO_ARRAY(cron_expression, ' ');

	 -- Validate the cron expression
	IF ARRAY_LENGTH(cron_fields, 1) != 5 THEN
		RAISE EXCEPTION 'Invalid cron expression: must contain 5 parts';
	END IF;

	-- Loop through the mathes to find the next valid date
	LOOP
		-- Calculate current search depth
		search_depth_interval := AGE(DATE_TRUNC('month', next_run_date), DATE_TRUNC('month', reference_date));
		search_depth_months := EXTRACT(YEAR FROM search_depth_interval)::INT * 12 + EXTRACT(MONTH FROM search_depth_interval)::INT;

		-- Return NULL if max search depth is exceeded
		IF search_depth_months > max_search_depth_months THEN
			RETURN NULL;
		END IF;

		-- Calculate next valid month
		next_month := cron_parser.get_cron_field_next_value(
			EXTRACT(MONTH FROM next_run_date)::INT,
			cron_fields[cron_month_index],
			cron_field_min[cron_month_index],
			cron_field_max[cron_month_index]
		);

		-- Handle case when no valid month is found within year
		IF next_month IS NULL THEN
			next_run_date := DATE_TRUNC('year', next_run_date + INTERVAL '1 year');
			CONTINUE;

		-- If month has changed, check if search depth is exceeded and update next run date
		ELSIF next_month != EXTRACT(MONTH FROM next_run_date)::INT THEN
			IF search_depth_months + next_month - EXTRACT(MONTH FROM next_run_date)::INT > max_search_depth_months THEN
				RETURN NULL;
			END IF;

			next_run_date := DATE_TRUNC('year', next_run_date) + INTERVAL '1 month' * (next_month - 1);
		END IF;

		-- Calculate next valid day of month if value is provided or day of week value is not provided
		IF (
			cron_fields[cron_day_of_week_index] = '*'
			OR cron_fields[cron_day_of_month_index] != '*'
		) THEN
			last_day_of_month := EXTRACT(DAY FROM (DATE_TRUNC('month', next_run_date) + INTERVAL '1 month - 1 day'))::INT;

			next_day_of_month := cron_parser.get_cron_field_next_value(
				EXTRACT(DAY FROM next_run_date)::INT,
				cron_fields[cron_day_of_month_index],
				cron_field_min[cron_day_of_month_index],
				cron_field_max[cron_day_of_month_index]
			);

			-- If day of month is not found within month, reset invalid value
			IF next_day_of_month > last_day_of_month THEN
				next_day_of_month := NULL;
			END IF;
		END IF;

		-- Calculate next valid day of month based on day of week
		IF cron_fields[cron_day_of_week_index] != '*' THEN
			next_day_of_week := cron_parser.get_cron_field_next_value(
				EXTRACT(DOW FROM next_run_date)::INT,
				cron_fields[cron_day_of_week_index],
				cron_field_min[cron_day_of_week_index],
				cron_field_max[cron_day_of_week_index]
			);

			-- If no valid value is found in the current week, search the next week
			IF next_day_of_week IS NULL THEN
				next_day_of_week := cron_parser.get_cron_field_next_value(
					cron_field_min[cron_day_of_week_index],
					cron_fields[cron_day_of_week_index],
					cron_field_min[cron_day_of_week_index],
					cron_field_max[cron_day_of_week_index]
				);
			END IF;

			-- Adjust day of month based on day of week value
			IF next_day_of_week IS NOT NULL THEN
				last_day_of_month := EXTRACT(DAY FROM (DATE_TRUNC('month', next_run_date) + INTERVAL '1 month - 1 day'))::INT;
				next_day_of_month_by_week := EXTRACT(DAY FROM next_run_date)::INT + ((7 - EXTRACT(DOW FROM next_run_date)::INT + next_day_of_week) % 7);

				-- If only day of week is found within month, set value
				IF (
					next_day_of_month_by_week <= last_day_of_month
					AND cron_fields[cron_day_of_month_index] = '*'
				) THEN
					next_day_of_month := next_day_of_month_by_week;

				-- If both day of month and day of week are found within month, set earliest value
				ELSIF next_day_of_month_by_week <= last_day_of_month THEN
					next_day_of_month := LEAST(next_day_of_month, next_day_of_month_by_week);

				-- If day of week is not found within month, reset invalid value
				ELSE
					next_day_of_month_by_week := NULL;
				END IF;
			END IF;
		END IF;

		-- Handle case when no valid day of month is found within month
		IF next_day_of_month IS NULL THEN
			next_run_date := DATE_TRUNC('month', next_run_date + INTERVAL '1 month');
			CONTINUE;

		-- If day of month has changed, update next run date
		ELSIF next_day_of_month != EXTRACT(DAY FROM next_run_date)::INT THEN
			next_run_date := DATE_TRUNC('month', next_run_date) + INTERVAL '1 day' * (next_day_of_month - 1);
		END IF;

		-- Calculate next valid hour
		next_hour := cron_parser.get_cron_field_next_value(
			EXTRACT(HOUR FROM next_run_date)::INT,
			cron_fields[cron_hour_index],
			cron_field_min[cron_hour_index],
			cron_field_max[cron_hour_index]
		);

		-- Handle case when no valid hour is found within day
		IF next_hour IS NULL THEN
			next_run_date := DATE_TRUNC('day', next_run_date + INTERVAL '1 day');
			CONTINUE;

		-- If hour has changed, update next run date
		ELSIF next_hour != EXTRACT(HOUR FROM next_run_date)::INT THEN
			next_run_date := DATE_TRUNC('day', next_run_date) + INTERVAL '1 hour' * next_hour;
		END IF;

		-- Calculate next valid minute
		next_minute := cron_parser.get_cron_field_next_value(
			EXTRACT(MINUTE FROM next_run_date)::INT,
			cron_fields[cron_minute_index],
			cron_field_min[cron_minute_index],
			cron_field_max[cron_minute_index]
		);

		-- Handle case when no valid minute is found within hour
		IF next_minute IS NULL THEN
			next_run_date := DATE_TRUNC('hour', next_run_date + INTERVAL '1 hour');
			CONTINUE;

		-- If minute has changed, update next run date
		ELSIF next_minute != EXTRACT(MINUTE FROM next_run_date)::INT THEN
			next_run_date := DATE_TRUNC('hour', next_run_date) + INTERVAL '1 minute' * next_minute;
		END IF;

		-- Return next run date if all fields are found
		RETURN next_run_date;

	END LOOP;

	-- Return NULL and log error if exception occurs
	EXCEPTION
		WHEN OTHERS THEN
			RAISE NOTICE 'Error in get_cron_next_run_date. Cron Expression: "%", Reference Date: "%". Error: %', cron_expression, reference_date, SQLERRM;
			RETURN NULL;
END;
$$;
