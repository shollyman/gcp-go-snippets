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
	"fmt"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/iterator"
)

func SimpleProjectList(ctx context.Context, filter string, pageSize int64) ([]string, error) {

	service, err := cloudresourcemanager.NewService(ctx)
	if err != nil {
		return nil, fmt.Errorf("cloudresourcemanager.NewService: %v", err)
	}

	var projects []string
	pageToken := ""
	for {
		call := service.Projects.List().PageToken(pageToken).Filter(filter)
		if pageSize > 0 {
			call = call.PageSize(pageSize)
		}
		resp, err := call.Do()
		if err != nil {
			return nil, fmt.Errorf("call error: %v", err)
		}
		//log.Printf("resourcemanager page had %d elements", len(resp.Projects))
		for _, v := range resp.Projects {
			projects = append(projects, v.ProjectId)
		}

		if resp.NextPageToken == "" {
			break
		}

		pageToken = resp.NextPageToken
	}
	return projects, nil
}

func SimpleDatasetsList(ctx context.Context, client *bigquery.Client, projectID string) ([]*bigquery.Dataset, error) {

	var datasets []*bigquery.Dataset
	it := client.DatasetsInProject(ctx, projectID)
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		// we proceed on errors rather than failing.
		if err != nil {
			log.Printf("dataset iteration error in project %s: %v", projectID, err)
			break
		}
		datasets = append(datasets, ds)
	}
	return datasets, nil
}

func SimpleTablesList(ctx context.Context, client *bigquery.Client, ds *bigquery.Dataset) ([]*bigquery.Table, error) {
	var tables []*bigquery.Table
	it := ds.Tables(ctx)
	for {
		tbl, err := it.Next()
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Printf("table iteration error in %s.%s: %v", ds.ProjectID, ds.DatasetID, err)
			break
		}
		tables = append(tables, tbl)
	}
	return tables, nil
}

func ChannelProjectList(ctx context.Context, filter string, ch chan string) error {
	service, err := cloudresourcemanager.NewService(ctx)
	if err != nil {
		return fmt.Errorf("cloudresourcemanager.NewService: %v", err)
	}

	pageToken := ""
	for {
		log.Printf("call")
		call := service.Projects.List().PageToken(pageToken).Filter(filter)
		resp, err := call.Do()
		if err != nil {
			return fmt.Errorf("call error: %v", err)
		}
		log.Printf("resourcemanager page had %d elements", len(resp.Projects))
		for _, v := range resp.Projects {
			ch <- v.ProjectId
		}

		if resp.NextPageToken == "" {
			break
		}

		pageToken = resp.NextPageToken
	}
	// signal we're done producing projects.
	close(ch)
	return nil
}

type Update struct {
	// only set for project updates.
	Project string

	// set for dataset updates.
	Dataset     *bigquery.Dataset
	TotalTables int64
}

func Process(ctx context.Context, client *bigquery.Client, projectFilter string) error {

	// TODO: should we use buffered channels?
	projectChan := make(chan string, 100)
	datasetChan := make(chan *bigquery.Dataset, 100)
	updateChan := make(chan *Update, 100)

	projectScanners := 2
	var projectWg sync.WaitGroup

	datasetScanners := 2
	var datasetWg sync.WaitGroup

	var updateWg sync.WaitGroup

	// stats
	var processedProjects, processedDatasets, processedTables int64

	// start an accumulator for tallying updates.
	// in a "real" catalog function we'd be populating the catalog, but in
	// this example we're just counting resources.
	updateWg.Add(1)
	go func() {
		defer updateWg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Printf("context was cancelled in updater")
				return
			case <-time.After(500 * time.Millisecond):
				log.Printf("in progress: %d projects, %d datasets, %d tables", processedProjects, processedDatasets, processedTables)
			case update, ok := <-updateChan:
				if !ok {
					// channel closed.
					return
				}
				if update.Project != "" {
					processedProjects = processedProjects + 1
				}
				if update.Dataset != nil {
					processedDatasets = processedDatasets + 1
					processedTables = processedTables + update.TotalTables
				}
			}
		}
	}()

	// start producing project values.
	// It will close the project channel when it's done.
	var projectErr error
	go func() {
		projectErr = ChannelProjectList(ctx, projectFilter, projectChan)
	}()

	// start pool for processing project entries.
	for i := 0; i < projectScanners; i++ {
		projectWg.Add(1)
		go func() {
			defer projectWg.Done()
			processProjects(ctx, client, projectChan, datasetChan, updateChan)
		}()
	}

	// start pool for processing datasets.
	for i := 0; i < datasetScanners; i++ {
		datasetWg.Add(1)
		go func() {
			defer datasetWg.Done()
			processDatasets(ctx, client, datasetChan, updateChan)
		}()
	}

	// wait for all project scanners to finish walking projects.
	// When all are done, we expect no further dataset entries, so can
	// close that channel.
	projectWg.Wait()
	log.Printf("done with project enumeration")
	close(datasetChan)

	// Now, do the same for the update channel.  When all datasets are
	// processed., we can close the update channel.
	datasetWg.Wait()
	log.Printf("done with dataset enumeration")
	close(updateChan)

	// Now, we wait on the updateWg.  When it's done, we're done.
	updateWg.Wait()
	log.Printf("final: %d projects, %d datasets, %d tables enumerated", processedProjects, processedDatasets, processedTables)
	return projectErr
}

func processProjects(ctx context.Context, client *bigquery.Client, projectChan <-chan string, datasetChan chan<- *bigquery.Dataset, updateChan chan<- *Update) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("processProjects context cancelled")
			return
		case project, ok := <-projectChan:
			if !ok {
				// input closed, return.
				return
			}
			it := client.DatasetsInProject(ctx, project)
			for {
				ds, err := it.Next()
				if err == iterator.Done {
					break
				}
				// we proceed on errors rather than failing.
				if err != nil {
					log.Printf("dataset iteration error in project %s: %v", project, err)
					break
				}
				datasetChan <- ds
			}
			// notify we've walked this project.
			//log.Printf("dataset traversal of project %s done", project)
			updateChan <- &Update{
				Project: project,
			}
		}
	}
}

func processDatasets(ctx context.Context, client *bigquery.Client, datasetChan <-chan *bigquery.Dataset, updateChan chan<- *Update) {
	for {
		select {
		case <-ctx.Done():
			log.Printf("processDatasets context cancelled")
			return
		case dataset, ok := <-datasetChan:
			if !ok {
				// input closed, return.
				return
			}
			var tblCount int64
			it := dataset.Tables(ctx)
			for {
				_, err := it.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					log.Printf("table iteration error in %s.%s: %v", dataset.ProjectID, dataset.DatasetID, err)
					break
				}
				tblCount = tblCount + 1
			}
			//log.Printf("table traversal of %s.%s done", dataset.ProjectID, dataset.DatasetID)
			updateChan <- &Update{
				Dataset:     dataset,
				TotalTables: tblCount,
			}
		}
	}

}
