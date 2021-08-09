// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
Package bqtablevalidator allows users to validate certain properties of a
BigQuery table, such as number of rows or other statistical properties about
specific columns.  It performs these validations by constructing a query and
processing the results of that query.

Note that this package makes no efforts to sanitize the values passed as
validation constraints, so it's possible that a malicious user allowed
to supply arbitrary inputs to this could straight up Bobby Tables
(https://xkcd.com/327/) their way into a SQL injection attack.

In case it's not obvious, the more validation constraints you add the more
slow and costly the validation result may be to compute.  Due to how the
validation query is constructed, there's an upper bound of 10,000 constraints
per validation.


Examples of Use


Prequisites:  This tool takes arguments from the cloud.google.com/go/bigquery package
for operation, so an example of setting them up is included here:

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, "my-project-id")
	table := client.Dataset("mydataset").Table("mytable")

With that, you can run validation of the overall table shape and check the number of rows:

	// Validate the table has 1000 rows, with an error range of 5 (995-1005 rows):
	results, err := bqtablevalidator.ValidateTableConstraints(ctx, client, table,
		bqtablevalidator.WithRowCount(1000,5))
	if err != nil {
		// The validation process itself failed.
	}
	if results.HasFailures() {
		// The validation process completed, and some of the constraints failed.
	}

You can also make assertions about individual columns:

	// Some constraints can check the data shape within a given column:
	results, err := bqtablevalidator.ValidateTableConstraintsa(ctx, client, table,
		bqtablevalidator.WithNullCount("mycolumn", 10, 0), // column has exactly 10 NULLs
		bqtablevalidator.WithDistinctValues("mycolumn", 50, 10)) // column has ~40-60 distinct values
	)
	if err != nil {
		// The validation process itself failed.
	}
	if results.HasFailures() {
		// The validation process completed, and some of the constraints failed.
	}

*/
package bqtablevalidator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"cloud.google.com/go/bigquery"
)

var (
	failedConstraint          = "constraint %q failed, got %d want %d"
	failedConstraintWithBound = "constraint %q failed, got %d want %d ±%d"
)

// ValidateTableConstraints validates a set of constraints against a supplied BigQuery table.
func ValidateTableConstraints(ctx context.Context, client *bigquery.Client, table *bigquery.Table, opts ...ConstraintOption) (*ValidationResults, error) {
	vr := &ValidationResults{
		constraints: make(map[string]*constraint),
	}
	for _, o := range opts {
		o(vr)
	}

	if len(vr.constraints) == 0 {
		return nil, errors.New("no constraints were supplied")
	}

	// construct our validation SQL.
	sql := new(bytes.Buffer)
	sql.WriteString("SELECT\n")
	var i int
	for _, c := range vr.constraints {
		if i > 0 {
			sql.WriteString(",")
		}
		sql.WriteString(c.projection)
		i++
	}
	sql.WriteString(fmt.Sprintf("\nFROM `%s`.%s.%s", table.ProjectID, table.DatasetID, table.TableID))
	q := client.Query(sql.String())
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to issue validation query: %v", err)
	}
	var resultrow []bigquery.Value
	err = it.Next(&resultrow)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve validation results from query: %v", err)
	}

	for colname, _ := range vr.constraints {
		offset := -1
		for k, v := range it.Schema {
			if v.Name == colname {
				offset = k
				break
			}
		}
		if offset == -1 {
			return nil, fmt.Errorf("missing constraint %q value from results", colname)
		}
		val, ok := resultrow[offset].(int64)
		if !ok {
			return nil, fmt.Errorf("constraint %q type mismatch, got %T", colname, val)
		}
		vr.constraints[colname].actualValue = val
	}
	return vr, nil
}

// constraint is a specific table constraint.
type constraint struct {

	// id is the name of the constraint
	id string
	// sql fragment that projects a result value
	projection string

	// all validation constraints must eval as int64.
	expectedValue int64

	// if nonzero, the constraint value must be within allowedError distance of expectedValue.
	allowedError int64

	// value retrieved from validation attempt.
	actualValue int64
}

func (c *constraint) Failed() bool {
	delta := c.actualValue - c.expectedValue
	if delta < 0 {
		delta = -delta
	}
	return delta > c.allowedError
}

func (c *constraint) FailureString() string {
	if c.allowedError == 0 {
		return fmt.Sprintf(failedConstraint, c.id, c.actualValue, c.expectedValue)
	}
	return fmt.Sprintf(failedConstraintWithBound, c.id, c.actualValue, c.expectedValue, c.allowedError)
}

func (c *constraint) FailureAsError() error {
	if !c.Failed() {
		return nil
	}
	return errors.New(c.FailureString())
}

// ValidationResults contains the results of the validation query.
type ValidationResults struct {
	constraints map[string]*constraint
}

// Failures returns a slice of strings corresponding to individual constraints
// that failed validation.
func (vr *ValidationResults) Failures() []string {
	if vr == nil {
		return nil
	}
	var fails []string
	for _, c := range vr.constraints {
		if c.Failed() {
			fails = append(fails, c.FailureString())
		}
	}
	return fails
}

// HasFailures reports whether any of the constraints specified in the validation failed.
func (vr *ValidationResults) HasFailures() bool {
	return len(vr.Failures()) > 0
}

func (vr *ValidationResults) errString() string {
	fails := vr.Failures()
	if len(fails) == 0 {
		return ""
	}
	showCount := 3
	if len(fails) < showCount {
		showCount = len(fails)
	}
	return fmt.Sprintf("validation failed for %d constraints, first %d: %s", len(fails), showCount, strings.Join(fails[:showCount], "\n"))
}

// AsError will return an error if validation contained failures.
func (vr *ValidationResults) AsError() error {
	s := vr.errString()
	if s == "" {
		return nil
	}
	return errors.New(s)
}

// AsTestingError will cause emitting testing errors or fatal response in case of validation failure.
//
// If fatal is true, this will fail test via t.Fatal.  Otherwise, it will emit t.Error.
func (vr *ValidationResults) AsTestingError(t *testing.T, fatal bool) {
	s := vr.errString()
	if s == "" {
		return
	}
	if fatal {
		t.Fatal(s)
	}
	t.Error(s)
}

// ConstraintOption allows defining validation constraints using an option
// pattern.
type ConstraintOption func(*ValidationResults)

// WithRowCount asserts the exact total row count of the table, and allows an
// optional errDelta to specify a success +/- range from expectedRows.
func WithRowCount(expectedRows, errDelta int64) ConstraintOption {
	return func(vr *ValidationResults) {
		resultCol := "total_rows"
		vr.constraints[resultCol] = &constraint{
			id:            "WithRowCount",
			projection:    fmt.Sprintf("COUNT(1) AS %s", resultCol),
			expectedValue: expectedRows,
			allowedError:  errDelta,
		}
	}
}

// WithNullCount asserts the number of null values in a column, with an option
func WithNullCount(colname string, expectedNullCount int64, errDelta int64) ConstraintOption {
	return func(vr *ValidationResults) {
		resultCol := fmt.Sprintf("nullcol_count_%s", colname)
		vr.constraints[resultCol] = &constraint{
			id:            fmt.Sprintf("WithNullCount-%s", colname),
			projection:    fmt.Sprintf("COUNTIF(%s IS NULL) AS %s", colname, resultCol),
			expectedValue: expectedNullCount,
			allowedError:  errDelta,
		}
	}
}

// WithNonNullCount asserts the number of non null values in a column.
func WithNonNullCount(colname string, expectedNonNullCount, errDelta int64) ConstraintOption {
	return func(vr *ValidationResults) {
		resultCol := fmt.Sprintf("nonnullcol_count_%s", colname)
		vr.constraints[resultCol] = &constraint{
			id:            fmt.Sprintf("WithNonNullCount-%s", colname),
			projection:    fmt.Sprintf("COUNTIF(%s IS NOT NULL) AS %s", colname, resultCol),
			expectedValue: expectedNonNullCount,
			allowedError:  errDelta,
		}
	}
}

// WithDistinctValues validates the exact cardinality of a column, within an optional error delta.
func WithDistinctValues(colname string, distinctVals, errDelta int64) ConstraintOption {
	return func(vr *ValidationResults) {
		resultCol := fmt.Sprintf("distinct_count_%s", colname)
		vr.constraints[resultCol] = &constraint{
			id:            fmt.Sprintf("WithDistinctValues-%s", colname),
			projection:    fmt.Sprintf("COUNT(DISTINCT %s) AS %s", colname, resultCol),
			expectedValue: distinctVals,
			allowedError:  errDelta,
		}
	}
}

// WithApproxDistinctValues validates the approximate cardinality of a column with an error bound.  For
// high cardinality tables, this may be more performant than WithDistinctValues.
func WithApproxDistinctValues(colname string, distinctValues, errDelta int64) ConstraintOption {
	return func(vr *ValidationResults) {
		resultCol := fmt.Sprintf("approx_distinct_count_%s", colname)
		vr.constraints[resultCol] = &constraint{
			id:            fmt.Sprintf("WithApproxDistinctValues-%s", colname),
			projection:    fmt.Sprintf("APPROX_COUNT_DISTINCT(%s) AS %s", colname, resultCol),
			expectedValue: distinctValues,
			allowedError:  errDelta,
		}
	}
}
