-- ================================================================
-- Cron expression parser for PostgreSQL - Tests
--
-- Copyright (c) 2024 Yelyzaveta Veis <liza.veis14@gmail.com>
--
-- Licensed under the MIT License. See LICENSE file in the repository.
-- ================================================================

CREATE OR REPLACE PROCEDURE cron_parser.test_get_cron_next_run_date()
LANGUAGE plpgsql
AS $$
DECLARE
	test_record RECORD;
	result TIMESTAMPTZ;
	passed_tests_count INT := 0;
	total_tests_count INT := 0;
BEGIN
	CREATE TEMPORARY TABLE get_cron_next_run_date_tests (
		id SERIAL PRIMARY KEY,
		cron_expression TEXT,
		reference_date TIMESTAMPTZ,
		max_search_depth_months INT,
		expected_result TIMESTAMPTZ,
		has_exception BOOLEAN
	)
	ON COMMIT DROP;

	INSERT INTO get_cron_next_run_date_tests(
		cron_expression,
		reference_date,
		max_search_depth_months,
		expected_result,
		has_exception
	)
	VALUES
		-- Regular cron expressions
		('* * * * *', '2024-12-14 00:00:00+00', NULL, '2024-12-14 00:00:00+00', FALSE),
		('* * * * *', '2024-12-14 00:00:59+00', NULL, '2024-12-14 00:00:00+00', FALSE),
		('0 * * * *', '2024-12-14 00:00:00+00', NULL, '2024-12-14 00:00:00+00', FALSE),
		('2 * * * *', '2024-12-14 00:00:00+00', NULL, '2024-12-14 00:02:00+00', FALSE),
		('59 * * * *', '2024-12-14 00:00:00+00', NULL, '2024-12-14 00:59:00+00', FALSE),
		('0 0 * * *', '2024-12-14 00:00:00+00', NULL, '2024-12-14 00:00:00+00', FALSE),
		('0 2 * * *', '2024-12-14 00:00:00+00', NULL, '2024-12-14 02:00:00+00', FALSE),
		('59 2 * * *', '2024-12-14 00:00:00+00', NULL, '2024-12-14 02:59:00+00', FALSE),
		('0 23 14 * *', '2024-12-14 00:00:00+00', NULL, '2024-12-14 23:00:00+00', FALSE),
		('59 1 14 * *', '2024-12-14 00:00:00+00', NULL, '2024-12-14 01:59:00+00', FALSE),
		('59 1 16 * *', '2024-12-14 00:00:00+00', NULL, '2024-12-16 01:59:00+00', FALSE),
		('* * 16 * *', '2024-12-14 00:00:00+00', NULL, '2024-12-16 00:00:00+00', FALSE),
		('* * * 12 *', '2024-12-14 00:00:00+00', NULL, '2024-12-14 00:00:00+00', FALSE),
		('* * 14 12 *', '2024-12-14 00:00:00+00', NULL, '2024-12-14 00:00:00+00', FALSE),
		('* * 16 12 *', '2024-12-14 00:00:00+00', NULL, '2024-12-16 00:00:00+00', FALSE),
		('59 23 16 12 *', '2024-12-14 00:00:00+00', NULL, '2024-12-16 23:59:00+00', FALSE),
		('* * * * 6', '2024-12-14 00:00:00+00', NULL, '2024-12-14 00:00:00+00', FALSE),
		('59 23 * * 6', '2024-12-14 00:00:00+00', NULL, '2024-12-14 23:59:00+00', FALSE),
		('* * * * *', '2024-12-14 23:59:59+00', NULL, '2024-12-14 23:59:00+00', FALSE),
		('59 * * * *', '2024-12-14 23:58:00+00', NULL, '2024-12-14 23:59:00+00', FALSE),
		('10 23 * * *', '2024-12-14 22:58:00+00', NULL, '2024-12-14 23:10:00+00', FALSE),
		('* 23 * * *', '2024-12-14 22:58:00+00', NULL, '2024-12-14 23:00:00+00', FALSE),
		('10 * * * *', '2024-12-14 22:59:00+00', NULL, '2024-12-14 23:10:00+00', FALSE),
		('10 * * * *', '2024-12-14 23:59:00+00', NULL, '2024-12-15 00:10:00+00', FALSE),
		('* 12 * * *', '2024-12-14 23:59:00+00', NULL, '2024-12-15 12:00:00+00', FALSE),
		('10 12 * * *', '2024-12-14 23:59:00+00', NULL, '2024-12-15 12:10:00+00', FALSE),
		('59 23 14 * *', '2024-12-14 23:59:00+00', NULL, '2024-01-14 23:59:00+00', FALSE),
		('59 22 14 12 *', '2024-12-14 23:59:00+00', NULL, '2025-12-14 22:59:00+00', FALSE),
		('59 23 14 12 6', '2024-12-14 23:59:00+00', NULL, '2024-12-14 23:59:00+00', FALSE),
		('59 22 14 12 1', '2024-12-14 23:59:00+00', NULL, '2024-12-15 22:59:00+00', FALSE), -- Sunday is closer than 14th of 12th month
		('59 23 14 12 1', '2024-12-14 22:59:00+00', NULL, '2024-12-14 23:59:00+00', FALSE),
		('59 * * 1 *', '2024-12-14 22:59:00+00', NULL, '2025-01-01 00:59:00+00', FALSE),
		('* * 31 * *', '2024-11-30 00:00:00+00', NULL, '2024-12-31 00:00:00+00', FALSE),
		('0 8 * * 2', '2024-12-04 15:41:16+00', NULL, '2024-12-10 08:00:00+00', FALSE),
		('0 8 * 10 1', '2024-12-04 15:41:16+00', NULL, '2025-10-06 08:00:00+00', FALSE),
		('0 16 * * 3', '2024-12-04 16:00:16+00', NULL, '2024-12-04 16:00:00+00', FALSE),

		-- Search depth cases
		('* * 29 2 *', '2025-01-31 23:59:00+00', NULL, NULL, FALSE),
		('* * 29 2 *', '2025-01-31 23:59:00+00', 38, '2025-01-31 23:59:00', FALSE),
		('0 8 * 10 1', '2024-12-04 15:41:16+00', 5, NULL, FALSE),
		('0 8 * 10 1', '2024-12-04 15:41:16+00', 6, '2025-10-06 08:00:00+00', FALSE),
		('* * * * *', '2024-12-14 00:00:00+00', 0, '2024-12-14 00:00:00+00', FALSE),
		('59 * * 1 *', '2024-12-14 22:59:00+00', 0, NULL, FALSE),

		-- Complex cron expressions
		('*/5 * * * *', '2024-12-14 00:00:00+00', NULL, '2024-12-14 00:00:00+00', FALSE),
		('*/5 * * * *', '2024-12-14 00:09:00+00', NULL, '2024-12-14 00:10:00+00', FALSE),
		('*/5 * * * *', '2024-12-14 00:10:00+00', NULL, '2024-12-14 00:10:00+00', FALSE),
		('1/5 * * * *', '2024-12-14 00:00:00+00', NULL, '2024-12-14 00:01:00+00', FALSE),
		('1/5 * * * *', '2024-12-14 00:02:00+00', NULL, '2024-12-14 00:06:00+00', FALSE),
		('1-10/5 * * * *','2024-12-14 00:00:00+00', NULL, '2024-12-14 00:01:00+00', FALSE),
		('1-10/5 * * * *','2024-12-14 00:10:00+00', NULL, '2024-12-14 01:01:00+00', FALSE),
		('* 2,3,5 * * *','2024-12-14 10:00:00+00', NULL, '2024-12-15 02:00:00+00', FALSE),
		('* 22,1,10 * * *','2024-12-31 23:00:00+00', NULL, '2025-01-01 01:00:00+00', FALSE),
		('* * * 2-10 *','2024-12-14 10:00:00+00', NULL, '2025-02-01 00:00:00+00', FALSE),
		('59 * * 2-5,10-12,1-3 *','2024-12-14 10:00:00+00', NULL, '2024-12-14 00:59:00+00', FALSE),
		('59 * 3-15 2-5 *','2024-12-14 10:00:00+00', NULL, '2025-02-03 00:59:00+00', FALSE),
		('* * * * */2,*/3','2024-12-14 10:00:00+00', NULL, '2024-12-14 10:00:00+00', FALSE),
		('* * * * */2,*/3','2024-12-15 10:00:00+00', NULL, '2024-12-15 10:00:00+00', FALSE),
		('* * * * */2,*/3','2024-12-16 10:00:00+00', NULL, '2024-12-17 00:00:00+00', FALSE),
		('0 0 * * */2,*/3','2024-12-17 10:00:00+00', NULL, '2024-12-18 00:00:00+00', FALSE),
		('0 0 * * 1,6','2024-12-17 00:00:00+00', NULL, '2024-12-21 00:00:00+00', FALSE),
		('0 0 * * 2-4','2024-12-17 10:00:00+00', NULL, '2024-12-17 10:00:00+00', FALSE),
		('0 0 * * 3-4','2024-12-17 23:00:00+00', NULL, '2024-12-18 00:00:00+00', FALSE),
		('0-12/4 * * * *','2024-12-17 04:04:00+00', NULL, '2024-12-17 04:04:00+00', FALSE),
		('4-59/3 * * * *','2024-12-17 04:04:00+00', NULL, '2024-12-17 04:04:00+00', FALSE),
		('4-59/3 * * * *','2024-12-17 04:05:00+00', NULL, '2024-12-17 04:07:00+00', FALSE),
		('1-10/2 */2 * * *','2024-12-17 02:59:00+00', NULL, '2024-12-17 04:01:00+00', FALSE),
		('* * 30 11 *', '2024-12-14 00:00:00+00', NULL, NULL, FALSE),

		-- Invalid cases
		('* * * * *', '2024-12-14 00:00:00+00', NULL, NULL, FALSE),
		('60 * * * *', '2024-12-14 00:00:00+00', NULL, NULL, FALSE),
		('* 24 * * *', '2024-12-14 00:00:00+00', NULL, NULL, FALSE),
		('* * 0 * *', '2024-12-14 00:00:00+00', NULL, NULL, FALSE),
		('* * 13 * *', '2024-12-14 00:00:00+00', NULL, NULL, FALSE),
		('* * * 0 *', '2024-12-14 00:00:00+00', NULL, NULL, FALSE),
		('* * * 32 *', '2024-12-14 00:00:00+00', NULL, NULL, FALSE),
		('* * * * 7', '2024-12-14 00:00:00+00', NULL, NULL, FALSE),
		('0-60 * * * *', '2024-12-14 00:00:00+00', NULL, NULL, FALSE),
		('* 1-24/2 * * *', '2024-12-14 00:00:00+00', NULL, NULL, FALSE),
		('* * 1,2,13,4 * *', '2024-12-14 00:00:00+00', NULL, NULL, FALSE),
		('* * * 2/32 *', '2024-12-14 00:00:00+00', NULL, NULL, FALSE),
		('* * * * 0-7/2', '2024-12-14 00:00:00+00', NULL, NULL, FALSE),
		('10-2 * * * *', '2024-12-14 00:00:00+00', NULL, NULL, FALSE);

	FOR test_record IN (
		SELECT * FROM get_cron_next_run_date_tests
	) LOOP
		BEGIN
			total_tests_count := total_tests_count + 1;
			result := cron_parser.get_cron_next_run_date(test_record.cron_expression, test_record.reference_date, test_record.max_search_depth_months);

			IF test_record.has_exception THEN
				RAISE INFO 'Test Case Failed: cron_expression = %, reference_date = %, max_search_depth_months = %. Expected exception, got %.',
					test_record.cron_expression, test_record.reference_date, test_record.max_search_depth_months, result;
			END IF;

			IF test_record.expected_result IS NOT DISTINCT FROM result THEN
				passed_tests_count := passed_tests_count + 1;
			ELSE
				RAISE INFO 'Test Case Failed: cron_expression = %, reference_date = %, max_search_depth_months = %. Expected %, got %.',
					test_record.cron_expression, test_record.reference_date, test_record.max_search_depth_months, test_record.expected_result, result;
			END IF;

			EXCEPTION
				WHEN OTHERS THEN
					IF test_record.has_exception THEN
						passed_tests_count := passed_tests_count + 1;
					ELSE
						RAISE INFO 'Test Case Failed: cron_expression = %, reference_date = %, max_search_depth_months = %. Expected %, got exception. Error: %',
							test_record.cron_expression, test_record.reference_date, test_record.max_search_depth_months, test_record.expected_result, SQLERRM;
					END IF;
		END;
	END LOOP;

	RAISE INFO 'Test Case Summary: % test case(s) executed, % successed, % failed.',
		total_tests_count, passed_tests_count, total_tests_count - passed_tests_count;

	ROLLBACK;
END;
$$

CALL cron_parser.test_get_cron_next_run_date();
