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
package catalogexpansion

import (
	"context"
	"log"
	"os"
	"sync"
	"testing"

	"cloud.google.com/go/bigquery"
)

var projectFilter = "parent.type:folder parent.id:1020941983720"

func TestSimpleProjectList(t *testing.T) {
	if os.Getenv("PROJECT_ID") == "" {
		t.Skip("PROJECT_ID not set")
	}

	ctx := context.Background()

	projects, err := SimpleProjectList(ctx, projectFilter, 0)
	if err != nil {
		t.Fatal(err)
	}
	log.Printf("projects has %d elements", len(projects))
}

func TestSimpleExpansion(t *testing.T) {
	if os.Getenv("PROJECT_ID") == "" {
		t.Skip("PROJECT_ID not set")
	}

	ctx := context.Background()

	var projectsProcessed, datasetsProcessed, tablesProcessed int

	bqClient, err := bigquery.NewClient(ctx, os.Getenv("PROJECT_ID"))
	if err != nil {
		t.Fatalf("failed to construct BQ client: %v", err)
	}
	projects, err := SimpleProjectList(ctx, projectFilter, 0)
	if err != nil {
		t.Fatalf("project list error: %v", err)
	}
	log.Printf("received %d projects", len(projects))
	for _, p := range projects {
		//log.Printf("starting %s: processed %d projects, %d datasets, %d tables", p, projectsProcessed, datasetsProcessed, tablesProcessed)
		projectsProcessed = projectsProcessed + 1
		datasets, err := SimpleDatasetsList(ctx, bqClient, p)
		if err != nil {
			t.Fatalf("failed dataset listing in %s: %v", p, err)
		}
		datasetsProcessed = datasetsProcessed + len(datasets)
		for _, d := range datasets {
			tables, err := SimpleTablesList(ctx, bqClient, d)
			if err != nil {
				t.Fatalf("failed table listing in %s.%s: %v", d.ProjectID, d.DatasetID, err)
			}
			tablesProcessed = tablesProcessed + len(tables)
		}
	}
	log.Printf("done: %d projects, %d datasets, %d tables", projectsProcessed, datasetsProcessed, tablesProcessed)
}

func TestChannelProjectList(t *testing.T) {
	if os.Getenv("PROJECT_ID") == "" {
		t.Skip("PROJECT_ID not set")
	}

	ctx := context.Background()
	ch := make(chan string)

	var err error

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = ChannelProjectList(ctx, projectFilter, ch)
	}()

	var results int64
	wg.Add(1)
	go func() {
		defer wg.Done()
	OuterLoop:
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					break OuterLoop
				}
				results = results + 1
			}
		}
	}()

	wg.Wait()
	if err != nil {
		t.Fatalf("projects list errored: %v", err)
	}
	log.Printf("projects has %d elements", results)
}

func TestConcurrentExpansion(t *testing.T) {
	if os.Getenv("PROJECT_ID") == "" {
		t.Skip("PROJECT_ID not set")
	}

	ctx := context.Background()

	bqClient, err := bigquery.NewClient(ctx, os.Getenv("PROJECT_ID"))
	if err != nil {
		t.Fatalf("failed to construct BQ client: %v", err)
	}

	err = Process(ctx, bqClient, projectFilter)
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
}
