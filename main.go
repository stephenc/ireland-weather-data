// Copyright 2025 Stephen Connolly
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	dataSourcesFile = "data-sources.yaml"
	metadataFile    = ".metadata.yaml"
	dateFormat      = "2006-01-02"
)

type MetadataEntry struct {
	LastModified string `yaml:"last_modified,omitempty"`
	ETag         string `yaml:"etag,omitempty"`
}

type Metadata map[string]MetadataEntry
type DataSources map[string][]string

func main() {
	log.SetFlags(0)
	today := time.Now().Format(dateFormat)

	dataSources := readDataSources()
	metadata := readMetadata()

	var hadError bool

	for dir, urls := range dataSources {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("Failed to create directory %s: %v", dir, err)
		}

		for _, url := range urls {
			log.Printf("Checking %s", url)

			meta := metadata[url]
			req, err := http.NewRequest("GET", url, nil)
			if err != nil {
				log.Printf("Error creating request: %v", err)
				hadError = true
				continue
			}

			if meta.LastModified != "" {
				req.Header.Set("If-Modified-Since", meta.LastModified)
			}
			if meta.ETag != "" {
				req.Header.Set("If-None-Match", meta.ETag)
			}

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("Request failed for %s: %v", url, err)
				hadError = true
				continue
			}
			resp.Body.Close() // will re-open later if 200

			if resp.StatusCode == http.StatusNotModified {
				log.Printf("Not modified: %s", url)
				continue
			}

			if resp.StatusCode != http.StatusOK {
				log.Printf("Unexpected response for %s: %s", url, resp.Status)
				hadError = true
				continue
			}

			// Stream response to a temporary file
			resp, err = http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("Retry request failed for %s: %v", url, err)
				hadError = true
				continue
			}
			defer resp.Body.Close()

			base := filepath.Base(url)
			outPath := filepath.Join(dir, strings.TrimSuffix(base, ".csv")+"-"+today+".csv")

			tmpFile, err := ioutil.TempFile(dir, "tmp")
			if err != nil {
				log.Printf("Error creating temp file: %v", err)
				hadError = true
				continue
			}

			_, err = io.Copy(tmpFile, resp.Body)
			tmpFile.Close()
			if err != nil {
				log.Printf("Error writing to file: %v", err)
				os.Remove(tmpFile.Name())
				hadError = true
				continue
			}

			if err := os.Rename(tmpFile.Name(), outPath); err != nil {
				log.Printf("Error renaming file: %v", err)
				hadError = true
				continue
			}
			log.Printf("Downloaded: %s → %s", url, outPath)

			// Preserve last-modified time
			if lm := resp.Header.Get("Last-Modified"); lm != "" {
				if t, err := http.ParseTime(lm); err == nil {
					os.Chtimes(outPath, time.Now(), t)
					meta.LastModified = lm
				}
			}
			if etag := resp.Header.Get("ETag"); etag != "" {
				meta.ETag = etag
			}
			metadata[url] = meta
		}
	}

	writeMetadata(metadata)

	if hadError {
		os.Exit(1)
	}
}

func readDataSources() DataSources {
	f, err := os.Open(dataSourcesFile)
	if err != nil {
		log.Fatalf("Failed to open %s: %v", dataSourcesFile, err)
	}
	defer f.Close()

	var ds DataSources
	if err := yaml.NewDecoder(f).Decode(&ds); err != nil {
		log.Fatalf("Failed to parse %s: %v", dataSourcesFile, err)
	}
	return ds
}

func readMetadata() Metadata {
	file, err := os.Open(metadataFile)
	if os.IsNotExist(err) {
		return Metadata{}
	} else if err != nil {
		log.Fatalf("Failed to read metadata: %v", err)
	}
	defer file.Close()

	var m Metadata
	if err := yaml.NewDecoder(file).Decode(&m); err != nil {
		log.Fatalf("Failed to parse metadata: %v", err)
	}
	return m
}

func writeMetadata(m Metadata) {
	tmp := metadataFile + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		log.Fatalf("Failed to write metadata: %v", err)
	}
	defer f.Close()

	enc := yaml.NewEncoder(f)
	enc.SetIndent(2)
	if err := enc.Encode(m); err != nil {
		log.Fatalf("Failed to encode metadata: %v", err)
	}
	enc.Close()

	if err := os.Rename(tmp, metadataFile); err != nil {
		log.Fatalf("Failed to move metadata file: %v", err)
	}
}
