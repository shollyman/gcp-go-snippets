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

package bqtablevalidator

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
)

var (
	client      *bigquery.Client
	projectID   string
	dataset     *bigquery.Dataset
	testTimeout = 5 * time.Minute
)

func TestMain(m *testing.M) {
	cleanup := testSetupIntegration()
	r := m.Run()
	cleanup()
	os.Exit(r)
}

func testSetupIntegration() func() {
	var ok bool
	projectID, ok = os.LookupEnv("GOOGLE_CLOUD_PROJECT")
	if !ok {
		return func() {}
	}
	ctx := context.Background()
	bqc, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("couldn't instantiate client in project %q: %v", projectID, err)
		return func() {}
	}
	client = bqc

	dsName := fmt.Sprintf("validator_test_%d", time.Now().UnixNano())
	ds := client.Dataset(dsName)
	if err := ds.Create(ctx, nil); err != nil {
		log.Fatalf("couldn't create dataset %q: %v", dsName, err)
		return func() {}
	}
	dataset = ds
	return func() {
		ds.DeleteWithContents(ctx)
	}
}

func TestValidationResults(t *testing.T) {

	vr := &ValidationResults{
		constraints: map[string]*constraint{
			"constraint1": {id: "id1", projection: "p1", expectedValue: 99, actualValue: 99},
		},
	}

	if vr.HasFailures() {
		t.Error("expected success, but HasFailures was true")
	}
	if len(vr.Failures()) > 0 {
		t.Errorf("wanted zero failures, got %d", len(vr.Failures()))
	}
	// add a failed constraint
	vr.constraints["constraint2"] = &constraint{id: "id1", projection: "p2", expectedValue: 1, actualValue: 2}
	if !vr.HasFailures() {
		t.Error("expected HasFailures true, was false")
	}
	wantFailCount := 1
	if len(vr.Failures()) != wantFailCount {
		t.Errorf("got %d failures, want %d failures", len(vr.Failures()), wantFailCount)
	}
}

func TestIntegration_Validation(t *testing.T) {
	if client == nil {
		t.Skip("integration test skipped, set GOOGLE_CLOUD_PROJECT and credentials")
	}
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	testTable := dataset.Table("testvalidation")
	sql := fmt.Sprintf("CREATE TABLE `%s`.%s.%s", testTable.ProjectID, testTable.DatasetID, testTable.TableID) +
		` AS
	 SELECT 1 as testkey, CAST(NULL as string) as testval
	 UNION ALL
	 SELECT 1 as testkey, "foo" as testval
	 UNION ALL
	 SELECT 2 as testkey, "bar" as testval
	`
	q := client.Query(sql)
	_, err := q.Read(ctx)
	if err != nil {
		t.Fatalf("failed to setup table: %v", err)
	}

	for _, tc := range []struct {
		description   string
		opts          []ConstraintOption
		wantErr       bool
		wantFailCount int
	}{
		{
			description: "no constraint",
			opts:        nil,
			wantErr:     true,
		},
		{
			description: "only correct row count",
			opts:        []ConstraintOption{WithRowCount(3, 0)},
		},
		{
			description:   "only invalid row count",
			opts:          []ConstraintOption{WithRowCount(99, 10)},
			wantFailCount: 1,
		},
		{
			description: "all options succeed",
			opts: []ConstraintOption{
				WithRowCount(3, 0),
				WithNullCount("testval", 1, 0),
				WithNonNullCount("testval", 2, 0),
				WithDistinctValues("testkey", 2, 0),
				WithApproxDistinctValues("testkey", 1, 1),
			},
		},
		{
			description: "all options fail",
			opts: []ConstraintOption{
				WithRowCount(99, 10),
				WithNullCount("testkey", 99, 11),
				WithNonNullCount("testkey", 99, 12),
				WithDistinctValues("testkey", 99, 13),
				WithApproxDistinctValues("testkey", 99, 14),
			},
			wantFailCount: 5,
		},
	} {
		result, err := ValidateTableConstraints(ctx, client, testTable, tc.opts...)
		if err != nil && !tc.wantErr {
			t.Errorf("%q: got err: %v", tc.description, err)
			continue
		}
		if err == nil && tc.wantErr {
			t.Errorf("%q: expected err from validate, got result", tc.description)
			continue
		}
		fails := result.Failures()
		if len(fails) != tc.wantFailCount {
			t.Errorf("%q: got %d fails, want %d fails.  Reported fails: %s", tc.description, len(fails), tc.wantFailCount, strings.Join(fails, " | "))
		}
	}
}
