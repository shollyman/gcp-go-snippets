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

package main

import "testing"

func TestParseJobString(t *testing.T) {
	testCases := []struct {
		description string
		in          string
		wantRef     *jobRef
		wantErr     bool
	}{
		{
			"fully qualified",
			"foo:bar.baz",
			&jobRef{"foo", "bar", "baz"},
			false,
		},
		{
			"no location",
			"foo:baz",
			&jobRef{"foo", "", "baz"},
			false,
		},
		{
			"invalid",
			"foo.bar",
			nil,
			true,
		},
	}

	for _, tc := range testCases {
		got, gotErr := parseJobStr(tc.in)
		if tc.wantErr {
			if gotErr == nil {
				t.Errorf("case %q failed.  Expected error, got result for: %s", tc.description, tc.in)
			}
			continue
		}
		if gotErr != nil {
			t.Errorf("case %q failed.  Expected result, got error: %v", tc.description, gotErr)
			continue
		}
		if *got != *tc.wantRef {
			t.Errorf("case %q failed.  got %+v want %+v", tc.description, got, tc.wantRef)
		}
	}
}
