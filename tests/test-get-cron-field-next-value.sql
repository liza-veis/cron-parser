-- ================================================================
-- Cron expression parser for PostgreSQL - Tests
--
-- Copyright (c) 2024 Yelyzaveta Veis <liza.veis14@gmail.com>
--
-- Licensed under the MIT License. See LICENSE file in the repository.
-- ================================================================

CREATE OR REPLACE PROCEDURE cron_parser.test_get_cron_field_next_value()
LANGUAGE plpgsql
AS $$
DECLARE
	test_record RECORD;
	result INT;
	passed_tests_count INT := 0;
	total_tests_count INT := 0;
BEGIN
	CREATE TEMPORARY TABLE get_cron_field_next_value_tests (
		id SERIAL PRIMARY KEY,
		field TEXT,
		current_value INT,
		min_value INT,
		max_value INT,
		expected_result INT,
		has_exception BOOLEAN
	)
	ON COMMIT DROP;

	INSERT INTO get_cron_field_next_value_tests(
		field,
		current_value,
		min_value,
		max_value,
		expected_result,
		has_exception
	)
	VALUES
		-- Regular values
		('*', 2, 0, 59, 2, FALSE), -- Wildcard
		('5', 3, 0, 59, 5, FALSE), -- Value lower than number
		('3', 5, 0, 59, NULL, FALSE), -- Value higher than number
		('5', 5, 0, 59, 5, FALSE), -- Value equals number
		('3', 3, 3, 59, 3, FALSE), -- Min boundary numeric value, value equals number
		('59', 0, 0, 59, 59, FALSE), -- Max boundary numeric value
		('59', 59, 0, 59, 59, FALSE), -- Max boundary numeric value, value equals number

		-- Regular values - Exception cases
		('*', 2, 5, 59, NULL, TRUE), -- Value lower than min boundary
		('*', 5, 0, 2,  NULL, TRUE), -- Value higher than max boundary
		('5', 3, 10, 59, NULL, TRUE), -- Number lower than min boundary
		('60', 10, 0, 59, NULL, TRUE), -- Number higher than max boundary

		-- Range values
		('1-5', 2, 0, 59, 2, FALSE), -- Value in range
		('0-59', 0, 0, 59, 0, FALSE), -- Min boundary value
		('0-59', 59, 0, 59, 59, FALSE), -- Max boundary value
		('3-5', 2, 2, 9, 3, FALSE), -- Value lower than range
		('3-5', 6, 2, 9, NULL, FALSE), -- Value higher than range
		('1-5', 5, 0, 59, 5, FALSE), -- Max range boundary value
		('1-5', 1, 0, 59, 1, FALSE), -- Min range boundary value

		-- Range values - Exception cases
		('0-5', 6, 5, 59, NULL, TRUE), -- Range lower than min boundary
		('1-60', 6, 1, 59, NULL, TRUE), -- Range higher than max boundary
		('1-', 6, 1, 59, NULL, TRUE), -- Invalid range notation
		('-59', 6, 1, 59, NULL, TRUE), -- Invalid range notation

		-- Step values
		('*/3', 0, 0, 59, 0, FALSE), -- Min boundary is even, value matches step
		('*/3', 2, 0, 59, 3, FALSE), -- Min boundary is even, value does not match step
		('*/3', 1, 1, 31, 1, FALSE), -- Min boundary is odd, value matches step
		('*/3', 5, 1, 31, 7, FALSE), -- Min boundary is odd, value does not match step
		('*/3', 59, 0, 59, NULL, FALSE), -- Max boundary value, value does not match step
		('*/3', 58, 1, 58, 58, FALSE), -- Min boundary is odd, max boundary value, value matches step
		('*/2', 0, 0, 59, 0, FALSE), -- Min boundary is even, value matches step
		('*/2', 2, 1, 59, 3, FALSE), -- Min boundary is odd, value does not match step
		('*/7', 2, 0, 59, 7, FALSE), -- Min boundary is odd, value does not match step
		('5/2', 2, 0, 59, 5, FALSE), -- Non standard, numeric value in a range, value lower than range
		('5/2', 6, 0, 59, 7, FALSE), -- Non standard, numeric value in a range, value in range

		-- Step values - Exception cases
		('*/0', 6, 1, 59, NULL, TRUE), -- Step lower than min boundary
		('*/60', 6, 1, 59, NULL, TRUE), -- Step higher than max boundary
		('*/*', 6, 1, 59, NULL, TRUE), -- Invalid step notation
		('0/2', 6, 1, 59, NULL, TRUE), -- Range lower than min boundary
		('60/2', 6, 1, 59, NULL, TRUE), -- Range higher than max boundary

		-- Range with step
		('2-9/2', 2, 0, 59, 2, FALSE), -- Min range boundary value, value matches step
		('1-9/2', 9, 0, 59, 9, FALSE), -- Max range boundary value, value matches step
		('2-9/2', 9, 0, 59, NULL, FALSE), -- Max range boundary value, value does not match step
		('1-9/3', 2, 0, 59, 4, FALSE), -- Max range boundary value, value does not match step
		('5-19/2', 3, 1, 59, 5, FALSE), -- Value lower than range
		('2-9/2', 10, 0, 59, NULL, FALSE), -- Value higher than range
		('2-19/2', 3, 1, 59, 4, FALSE), -- Lower limit even, min boundary odd

		-- Range with step - Exception cases
		('0-5/2', 6, 1, 59, NULL, TRUE), -- Range lower than min boundary
		('1-60/2', 6, 1, 59, NULL, TRUE), -- Range higher than max boundary
		('1-/2', 6, 1, 59, NULL, TRUE), -- Invalid range notation
		('-30/2', 6, 1, 59, NULL, TRUE), -- Invalid range notation

		-- Complex values with comma
		('1,2,4', 1, 0, 59, 1, FALSE), -- Value matches option
		('1,2,4', 3, 0, 59, 4, FALSE), -- Value lower than one of the options
		('1,5,7', 2, 0, 59, 5, FALSE), -- Value lower than one of the options
		('1,5,7', 8, 0, 59, NULL, FALSE), -- Value higher than any option
		('10,5,7', 8, 0, 59, 10, FALSE), -- Unordered range
		('10,5,7', 6, 0, 59, 7, FALSE), -- Unordered range
		('1-3,6,10', 2, 0, 59, 2, FALSE), -- Value in range
		('1-3,6,10', 6, 0, 59, 6, FALSE), -- Value matches non-range option
		('1-3,5-9,10', 4, 0, 59, 5, FALSE), -- Value is near one of the ranges
		('1-5,3-9,10', 4, 0, 59, 4, FALSE), -- Value in ranges that intersect
		('*/5,3-9,10', 0, 0, 59, 0, FALSE), -- Value matches step
		('*/5,3-9,10', 1, 0, 59, 3, FALSE), -- Value is near the range
		('1-59/2,3-9,10', 1, 0, 59, 1, FALSE), -- Value matches complex step
		('10,1-59/2,5-9', 2, 0, 59, 3, FALSE), -- Value is near complex step
		('*/2,1/3',  3, 0, 59, 4, FALSE), -- Value is near steps that intersect

		-- Complex values with comma - Exception cases
		('1,3,0-59',  3, 1, 59, NULL, TRUE), -- Range lower than min boundary
		('1,3,1-60',  3, 0, 59, NULL, TRUE), -- Range higher than max boundary
		('5,6,*/2',  3, 3, 12, NULL, TRUE), -- Step lower than min boundary
		('1,3,*/13',  3, 1, 12, NULL, TRUE), -- Step higher than max boundary
		('1,0,*/2',  3, 1, 12, NULL, TRUE), -- Number lower than min boundary
		('1,30,*/2',  3, 1, 12, NULL, TRUE), -- Number higher than max boundary
		('1,2,0-12/2',  3, 1, 12, NULL, TRUE), -- Range lower than min boundary
		('1,2,1-13/2',  3, 1, 12, NULL, TRUE); -- Range higher than max boundary

	FOR test_record IN (
		SELECT * FROM get_cron_field_next_value_tests
	) LOOP
		BEGIN
			total_tests_count := total_tests_count + 1;
			result := cron_parser.get_cron_field_next_value(test_record.current_value, test_record.field, test_record.min_value, test_record.max_value);

			IF test_record.has_exception THEN
				RAISE INFO 'Test Case Failed: field = %, current_value = %, min_value = %, max_value = %. Expected exception, got %.',
					test_record.field, test_record.current_value, test_record.min_value, test_record.max_value, result;
			END IF;

			IF test_record.expected_result IS NOT DISTINCT FROM result THEN
				passed_tests_count := passed_tests_count + 1;
			ELSE
				RAISE INFO 'Test Case Failed: field = %, current_value = %, min_value = %, max_value = %. Expected %, got %.',
					test_record.field, test_record.current_value, test_record.min_value, test_record.max_value, test_record.expected_result, result;
			END IF;

			EXCEPTION
				WHEN OTHERS THEN
					IF test_record.has_exception THEN
						passed_tests_count := passed_tests_count + 1;
					ELSE
						RAISE INFO 'Test Case Failed: field = %, current_value = %, min_value = %, max_value = %. Expected %, got exception. Error: %',
							test_record.field, test_record.current_value, test_record.min_value, test_record.max_value, test_record.expected_result, SQLERRM;
					END IF;
		END;
	END LOOP;

	RAISE INFO 'Test Case Summary: % test case(s) executed, % successed, % failed.',
		total_tests_count, passed_tests_count, total_tests_count - passed_tests_count;

	ROLLBACK;
END;
$$

CALL cron_parser.test_get_cron_field_next_value();
