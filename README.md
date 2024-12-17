# Cron Expression Parser for PostgreSQL

PostgreSQL function to parse cron expression and calculate the next run date based on the provided reference date.

## Usage

### Parameters

- **cron_expression** (TEXT): The cron expression to be parsed (e.g., `*/5 * * * *` for every 5 minutes).
- **reference_date** (TIMESTAMPTZ, optional): The date and time from which to calculate the next run date. Defaults to `CURRENT_TIMESTAMP`.
- **max_search_depth_months** (INT, optional): The maximum number of months to search for the next valid run. Defaults to `12`.

### Notes

- The seconds from the reference_date are ignored in calculations.
- If an exception occurs, the error is logged, and the function returns NULL.

### Cron Expression Format

A cron expression consists of five fields:

1. Minute: 0-59
2. Hour: 0-23
3. Day of Month: 1-31
4. Month: 1-12
5. Day of Week: 0-6 (Sunday to Saturday)

Each field can include:

- A specific value within field's boundaries (e.g., `5`),
- A wildcard (`*` for any value),
- A range of values (e.g., `5-10`),
- A step value (e.g., `*/5`, `1-29/5`, `1/5`),
- A list of combined values (e.g., `1,2,3-7,*/2`).

For more information on the cron format, refer [Cron](https://en.wikipedia.org/wiki/Cron).

### Examples

```sql
-- Every day at 00:00
SELECT cron_parser.get_cron_next_run_date('0 0 * * *', '2024-12-04 15:41:16+00')
-- Result: 2024-12-05 00:00:00+00

-- Every Tuesday at 08:00
SELECT cron_parser.get_cron_next_run_date('0 8 * * 2', '2024-12-04 15:41:16+00')
-- Result: 2024-12-10 08:00:00+00

-- Every Monday at 08:00 in October
SELECT cron_parser.get_cron_next_run_date('0 8 * 10 1', '2024-12-04 15:41:16+00')
-- Result: 2025-10-06 08:00:00+00

-- Every Wednesday at 16:00
SELECT cron_parser.get_cron_next_run_date('0 16 * * 3', '2024-12-04 16:00:16+00');
-- Result: 2024-12-04 16:00:00+00

-- At every minute on 29th in February
SELECT cron_parser.get_cron_next_run_date('* * 29 2 *', '2025-01-31 23:59:00+00');
-- Result: NULL
SELECT cron_parser.get_cron_next_run_date('* * 29 2 *', '2025-01-31 23:59:00+00', 38);
-- Result: 2028-02-29 00:00:00+00
```

## Algorithm

### Finding the Next Run Date

1. Split cron expression into its individual fields (minute, hour, day of month, month, day of week).
2. Initialize the next run date with the reference date.
3. Start from the highest field (month) and search for the next valid value within the corresponding higher unit (e.g., find the next month in the current year). If a match is found, update the reference date and reset lower fields (e.g., day, hour, minute) to their minimum valid values. Move to the next lower field.
4. If no valid value is found within the current unit, increment the top-level unit (e.g., year, month) by 1 and start searching from the highest field (month) again.
5. When a valid minute value is found, the next run date is determined.

**Exception**: If both `day_of_month` and `day_of_week` are specified, the closest matching date is chosen, considering the minute, hour, and month constraints.

### Finding the Next Valid Value for a Field

1. Split the field by commas to handle multiple values.
2. Iterate through each part, finding the closest value greater than or equal to the current value. If a smaller value is found, update the next possible value.
3. If the next value fits within the field's boundaries, return it.
