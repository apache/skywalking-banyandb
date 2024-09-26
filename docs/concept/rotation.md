# Data Rotation

Data rotation is the process of managing the size of data stored in BanyanDB by removing old data and keeping only the most recent data. Data rotation is essential to prevent the database from running out of disk space and to maintain query performance.

## Overview

BanyanDB partitions its data into multiple [**segments**](tsdb.md#segment). These segments are time-based, allowing efficient management of data retention and querying. The `segment_interval` and retention policy (`ttl`) for each [group](../interacting/data-lifecycle.md#measures-and-streams) determine how data is segmented and retained in the database.

## Formulation

To express the relationship between the **number of segments**, the **segment interval**, and the **time-to-live (TTL)** in BanyanDB, we can derive a simple formula.

### General Formula for Number of Segments

The relationship between the number of segments, segment interval, and TTL can be expressed as:

```
S = (T / I) rounded up + 1
```

Where:

- `S` is the **number of segments**.
- `I` is the **segment interval** (in the same unit as the TTL).
- `T` is the **TTL** (time-to-live, in the same unit as the segment interval).

### Explanation

1. **T / I**: This represents the number of full segments needed to cover the TTL. For example, if the TTL is 7 days and the segment interval is 3 days, you would need at least 2.33 segments to cover the 7-day period.

2. **Rounded up**: We round up the result of `T / I` because partial segments still require a full segment to store the data.

3. **+ 1 segment**: We add 1 additional segment to account for the next segment being created to store incoming data as the current period closes.

### General Insights

- **Smaller segment intervals** (e.g., 1 day) lead to a larger number of segments because more segments are needed to cover the TTL.
- **Larger segment intervals** (e.g., 3 days) reduce the number of segments, but you still need 1 additional segment to handle data as it transitions between periods.

Thus, the formula effectively balances the need for both data retention and the number of segments based on the chosen segment interval.

### Example 1: Segment Interval = 3 Days, TTL = 7 Days

```
S = (7 / 3) rounded up + 1
S = 2.33 rounded up + 1
S = 3 + 1
S = 4
```

| Time (Day)    | Action                                | Number of Segments |
|---------------|---------------------------------------|--------------------|
| Day 1 (00:00) | Segment for Days 1–3 is created       | 1                  |
| Day 3 (23:00) | New segment for Days 4–6 is created   | 2                  |
| Day 6 (23:00) | New segment for Days 7–9 is created   | 3                  |
| Day 9 (23:00) | New segment for Days 10–12 is created | 4                  |
| Day 10 (00:00) | Oldest segment for Days 1–3 is removed | 3                  |
| Day 12 (23:00) | New segment for Days 13–15 is created | 4                  |
| Day 13 (00:00) | Oldest segment for Days 4–6 is removed | 3                  |

So, **4 segments** are required to retain data for 7 days with a 3-day segment interval.

### Example 2: Segment Interval = 1 Day, TTL = 7 Days

```
S = (7 / 1) rounded up + 1
S = 7 + 1
S = 8
```

| Time (Day)    | Action                            | Number of Segments |
|---------------|-----------------------------------|--------------------|
| Day 1 (23:00) | New segment for Day 2 is created  | 1                  |
| Day 2 (00:00) | Oldest segment (if any) removed   | 1                  |
| Day 2 (23:00) | New segment for Day 3 is created  | 2                  |
| Day 3 (00:00) | Oldest segment (if any) removed   | 2                  |
| ...           | ...                               | ...                |
| Day 7 (23:00) | New segment for Day 8 is created  | 7                  |
| Day 8 (00:00) | Oldest segment for Day 1 removed  | 7                  |
| Day 8 (23:00) | New segment for Day 9 is created  | 8                  |
| Day 9 (00:00) | Oldest segment for Day 2 removed  | 7                  |

At any given time, there will be a maximum of **8 segments**: 1 for the new day and 7 for the last 7 days of data.

### Example 3: Segment Interval = 2 Days, TTL = 7 Days

```
S = (7 / 2) rounded up + 1
S = 3.5 rounded up + 1
S = 4 + 1
S = 5
```

So, **5 segments** are required to retain data for 7 days with a 2-day segment interval.

### Generalization for Any Time Unit

To use this formula with time units like hours and days , make sure **both the segment interval (I)** and **TTL (T)** use the same unit of time. If they don’t, convert one of them so that they match.

#### Steps

1. **Convert both the segment interval and TTL to the same time unit**, if necessary.
   - For example, if the TTL is in days but the segment interval is in hours, convert the TTL to hours (e.g., 3 days = 72 hours).

2. **Apply the formula** to get the number of segments.

### Example 4: Mixed Units (Segment Interval in Hours, TTL in Days)

- **Segment Interval** = 12 hours
- **TTL** = 3 days

First, convert the TTL to hours:

```
3 days = 3 * 24 = 72 hours
```

Now, apply the formula:

```
S = (72 / 12) rounded up + 1
S = 6 + 1
S = 7
```

So, **7 segments** are required to retain data for 3 days with a 12-hour segment interval.


### Example 5: Minimum Number of Segments

```
S = (7 / 8) rounded up + 1
S = 0.875 rounded up + 1
S = 1 + 1
S = 2
```

So, **2 segments** are required to retain data for 7 days with an 8-day segment interval. 2 segments are the minimum number whatever the TTL and segment interval are. When the TTL is less than the segment interval, you can have the minimum number of segments.

## Conclusion

Data rotation is a critical aspect of managing data in BanyanDB. By understanding the relationship between the number of segments, segment interval, and TTL, you can effectively manage data retention and query performance in the database. The formula provided here offers a simple way to calculate the number of segments required based on the chosen segment interval and TTL.

For more information on data management and lifecycle in BanyanDB, refer to the [Data Lifecycle](../interacting/data-lifecycle.md) documentation.
