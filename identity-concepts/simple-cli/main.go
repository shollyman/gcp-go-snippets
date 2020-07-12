// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// simple-cli demonstrates some basic interactions with a GCP service and helps
// to demonstrate Application Default Credentials (ADC) in action.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/oauth2/v1"
)

var flagProject = flag.String("project_id", "", "Project ID to use")

func main() {

	// Basic validation for flags/environment.
	flag.Parse()
	projectID := *flagProject
	if projectID == "" {
		// We're not being provided with a project ID on the command line, so
		// we'll fallback to using the projectID associated with BigQuery
		// public datasets.  This would be problematic in many cases, but in
		// this case all we're doing is requesting a list of datasets, which
		// in this case are public and accessible to all authenticated users.
		projectID = "bigquery-public-data"
	}
	ctx := context.Background()

	// Ask the Google oauth endpoint who it thinks this process is.
	fmt.Println()
	identity, err := WhoAmI(ctx)
	if err != nil {
		log.Printf("Couldn't resolve identity: %v", err)
	} else {
		log.Printf("Using identity: %s\n", identity)
	}

	fmt.Println()
	// Instantiate a BigQuery client
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	// Create a dataset iterator and walk it.
	limit := 5
	fmt.Printf("Listing up to %d datasets:\n", limit)
	it := client.Datasets(ctx)
	for limit > 0 {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("iterator failed: %v", err)
		}
		fmt.Printf("dataset: %s\n", ds.DatasetID)
		limit = limit - 1
	}
}

// WhoAmI returns the identity of the invoking process based on ADC.
// In practice, if we find an identity source, it will get attached
// to the HTTP request to the oauth2 userinfo endpoint as a token
// in the HTTP headers.
func WhoAmI(ctx context.Context) (string, error) {

	svc, err := oauth2.NewService(ctx)
	if err != nil {
		return "", err
	}

	// Interrogate the oauth2 userinfo info endpoint.
	call := svc.Userinfo.Get()
	resp, err := call.Do()
	if err != nil {
		return "", err
	}

	// We got a response back, but the identity is somehow empty.
	// Treat this as an error.
	if resp.Email == "" {
		return "", errors.New("could not resolve identity")
	}

	return resp.Email, nil
}
