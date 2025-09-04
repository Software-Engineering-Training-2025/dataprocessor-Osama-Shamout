[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/e89AXlC-)
# **Task: Data Processor with Strategy Pattern**

## **Objective**

You will implement a **data processing pipeline** using the **Strategy Pattern**.
The pipeline will:

1. **Clean** the input data using a chosen cleaning strategy,
2. **Analyze** the cleaned data using one of five analysis strategies,
3. **Output** the result to either the console or a text file.

Your job is to implement **one method** that ties these strategies together.

---

## **Method Signature**

```java
public double process(
    CleaningType cleaningType,
    AnalysisType analysisType,
    OutputType outputType,
    List<Integer> data
) throws Exception
```

* **Parameters**:

  * `cleaningType` → which cleaning strategy to use
  * `analysisType` → which analysis to perform
  * `outputType` → where to send the result (console or file)
  * `data` → the list of integers to process

* **Return**:

  * The **numeric result** of the chosen analysis as a `double`.

* **Order of operations**: **Clean → Analyze → Output → Return result**

---

## **Cleaning Strategies**

* **REMOVE\_NEGATIVES**: drop all values `< 0`
* **REPLACE\_NEGATIVES\_WITH\_ZERO**: replace all values `< 0` with `0`

---

## **Analysis Strategies (5 Total)**

1. **MEAN**

   * Arithmetic average of all numbers.
   * Empty list → `NaN`.

2. **MEDIAN**

   * Middle value after sorting (even length → average of two middles).
   * Empty list → `NaN`.

3. **STD\_DEV**

   * Population standard deviation (divide by **N**, not N−1).
   * Empty list → `NaN`.

4. **P90\_NEAREST\_RANK**

   * Sort ascending; `rank = ceil(0.90 * N)` (1-based); return value at that rank.
   * Empty list → `NaN`.

5. **TOP3\_FREQUENT\_COUNT\_SUM**

   * Count frequencies of each distinct value.
   * Sort by **count desc**, break ties by **value asc**.
   * Take the top 3 frequencies and sum them (fewer than 3 → sum existing).
   * Empty list → `0.0`.

---

## **Output Strategies**

* **CONSOLE**: Print `Result = <value>` to standard output.
* **TEXT\_FILE**: Write `Result = <value>` to `target/result.txt`.

  * Overwrite if file exists.
  * Create directories if needed.

---

## **Rules**

* Use the **Strategy Pattern** and the provided factory; no long `if/else` ladders.
* Do **not** modify the input list directly.
* Handle empty input according to the rules above.
* Write output exactly in the format `Result = <value>`.

---

## **Examples**

| CleaningType                   | AnalysisType               | OutputType | Input Data                  | Result          |
| ------------------------------ | -------------------------- | ---------- | --------------------------- | --------------- |
| REMOVE\_NEGATIVES              | MEAN                       | CONSOLE    | \[5, -2, 7, 8]              | Mean = 6.666…   |
| REPLACE\_NEGATIVES\_WITH\_ZERO | MEDIAN                     | TEXT\_FILE | \[5, -2, 7, 8]              | Median = 6.0    |
| REMOVE\_NEGATIVES              | P90\_NEAREST\_RANK         | CONSOLE    | \[1,3,5,7,9,11,13,15,17,19] | 17.0            |
| REPLACE\_NEGATIVES\_WITH\_ZERO | STD\_DEV                   | TEXT\_FILE | \[2,4,4,4,5,5,7,9]          | StdDev = 2.0    |
| REMOVE\_NEGATIVES              | TOP3\_FREQUENT\_COUNT\_SUM | CONSOLE    | \[5,5,5,1,1,2,2,3]          | Count sum = 7.0 |

---
