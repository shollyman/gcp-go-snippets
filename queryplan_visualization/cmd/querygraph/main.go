// Copyright 2021 Google LLC
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

// querygraph demonstrates visualizing BigQuery query plans by converting them
// into graphviz "dot" file notation and using graphviz tooling to emit images.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"html"
	"log"
	"os"
	"os/exec"
	"strings"

	"cloud.google.com/go/bigquery"
)

type jobRef struct {
	Project  string
	Location string
	JobID    string
}

func (ref *jobRef) FullJobID() string {
	if ref.Location == "" {
		return fmt.Sprintf("%s:%s", ref.Project, ref.JobID)
	}
	return fmt.Sprintf("%s:%s.%s", ref.Project, ref.Location, ref.JobID)
}

func parseJobStr(in string) (*jobRef, error) {
	jr := &jobRef{}
	i := strings.LastIndex(in, ":")
	if i <= 0 || len(in) < i+1 {
		return nil, errors.New("malformed job string, expected form of 'project:location.jobid'")
	}
	jr.Project = in[0:i]
	in = in[i+1:]
	i = strings.LastIndex(in, ".")
	if i <= 0 || len(in) < i+1 {
		// no location split
		jr.JobID = in
		return jr, nil
	}
	jr.Location = in[0:i]
	jr.JobID = in[i+1:]
	return jr, nil
}

// fetchQueryDetails is responsible for getting the job metadata from BigQuery.
func fetchQueryDetails(ctx context.Context, ref *jobRef) ([]*bigquery.ExplainQueryStage, error) {

	// construct a BigQuery client using the project referenced in the job reference.
	client, err := bigquery.NewClient(ctx, ref.Project)
	if err != nil {
		return nil, fmt.Errorf("bigquery.NewClient: %v", err)
	}

	// Fetch job metadata.
	job, err := client.JobFromIDLocation(ctx, ref.JobID, ref.Location)
	if err != nil {
		return nil, fmt.Errorf("couldn't get job metadata: %w", err)
	}

	status, err := job.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("couldn't get status: %w", err)
	}

	queryStats, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok {
		return nil, fmt.Errorf("couldn't retrieve query statistics: %w", err)
	}

	if len(queryStats.QueryPlan) == 0 {
		return nil, fmt.Errorf("query plan was empty")
	}
	return queryStats.QueryPlan, nil

}

// nameToColor is used for mapping a dominant query stage description to a hex color string.
func nameToColor(name string) string {
	parts := strings.Split(name, ":")
	// Remove spaces and plus symbols (indicate complex stages).
	opType := strings.Trim(parts[1], " +")
	switch opType {
	case "Input":
		return "#f50057"
	case "Repartition":
		return "#4caf50"
	case "Join":
		return "#ff6f00"
	default:
		return "#2196f3"
	}
}

// stagesToDot converts the queryDetails datastructure into a dot representation.
func stagesToDot(stages []*bigquery.ExplainQueryStage, title string) []byte {

	shufOut := make(map[int64]int64)
	flushOut := make(map[int64]int64)
	var buf bytes.Buffer

	buf.WriteString("digraph {\n")
	buf.WriteString("  labelloc=\"t\"\n")
	buf.WriteString(fmt.Sprintf("  label=<<font point-size=\"30\">%s</font>>\n", title))
	buf.WriteString("\tnode[shape=record]\n")
	for _, s := range stages {

		shufOut[s.ID] = s.ShuffleOutputBytes
		flushOut[s.ID] = s.ShuffleOutputBytesSpilled

		buf.WriteString(fmt.Sprintf("\ts%d[label=<\n", s.ID))
		buf.WriteString("\t\t<TABLE>\n")
		top := fmt.Sprintf("<font color=\"white\"><font point-size=\"36\">%s</font><br/>%d/%d workers, %d records read</font>", s.Name, s.CompletedParallelInputs, s.ParallelInputs, s.RecordsRead)
		buf.WriteString(fmt.Sprintf("\t\t<TR><TD COLSPAN=\"2\" COLOR=\"#FFF\" BGCOLOR=\"%s\">%s</TD></TR>\n", nameToColor(s.Name), top))

		if len(s.Steps) > 0 {
			for _, st := range s.Steps {
				l := len(st.Substeps)
				if l > 0 {
					buf.WriteString(fmt.Sprintf("<tr><td ROWSPAN=\"%d\">%s</td><td>%s</td></tr>\n", l, st.Kind, html.EscapeString(st.Substeps[0])))
					if l > 1 {
						for _, sub := range st.Substeps[1:] {
							buf.WriteString(fmt.Sprintf("<tr><td>%s</td></tr>\n", html.EscapeString(sub)))
						}
					}
				} else {
					buf.WriteString(fmt.Sprintf("<tr><td>%s</td><td>&nbsp;</td></tr>\n", st.Kind))
				}

			}
		}
		buf.WriteString("\t\t</TABLE>\n\t>\n")
		if strings.Contains(s.Name, "Repartition") {
			buf.WriteString(" style=dotted")
		}
		buf.WriteString("]\n")
	}
	buf.WriteString("\n")
	for _, s := range stages {
		for _, p := range s.InputStages {
			buf.WriteString(fmt.Sprintf("\ts%d->s%d [label=\"%d MB\"]\n", p, s.ID, shufOut[p]/1024/1024))
		}
	}
	buf.WriteString("}\n")
	return buf.Bytes()

}

func dotToPNG(in []byte, dotBinPath, outputFile string) error {
	cmd := exec.Command(dotBinPath, "-Tpng", "-Gdpi=72", "-o", outputFile)
	cmd.Stdin = bytes.NewReader(in)
	_, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("failed to generate: %w", err)
	}
	return nil
}

// resolveOutput determines the output file used for saving the visualized plan.
func resolveOutput(ref *jobRef, outFlag string) string {
	if outFlag != "" {
		return outFlag
	}
	if ref.Location == "" {
		return fmt.Sprintf("%s__%s.png", ref.Project, ref.JobID)
	}
	return fmt.Sprintf("%s__%s__%s.png", ref.Project, ref.Location, ref.JobID)
}

func main() {

	var (
		dotPath    = flag.String("dot_path", "/usr/bin/dot", "path to the dot (graphviz) binary")
		outputFile = flag.String("out", "", "path to the output PNG file.  By default, creates a file based on the job ID in the current directory.")
	)
	flag.Usage = func() {
		buf := new(bytes.Buffer)
		buf.WriteString(fmt.Sprintf("Usage: %s <job>\n\n", os.Args[0]))
		buf.WriteString("Allowed format of the <job> argument:\n")
		buf.WriteString(" - project:location.jobid\n")
		buf.WriteString(" - project:jobid\n")
		buf.WriteString("\n\nOptional Flags:\n\n")
		fmt.Fprint(os.Stderr, buf.String())
		flag.PrintDefaults()
	}
	flag.Parse()

	ctx := context.Background()

	jobArg := flag.Arg(0)
	if jobArg == "" {
		flag.Usage()
		return
	}
	jobRef, err := parseJobStr(jobArg)
	if err != nil {
		log.Fatalf("job parse error (%q): %v", jobArg, err)
	}

	stages, err := fetchQueryDetails(ctx, jobRef)
	if err != nil {
		log.Fatalf("failed to get query stages: %v", err)
	}

	b := stagesToDot(stages, jobRef.FullJobID())

	outputName := resolveOutput(jobRef, *outputFile)
	if err := dotToPNG(b, *dotPath, outputName); err != nil {
		log.Fatalf("failed to render output image: %v", err)
	}

}
