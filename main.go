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
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	dataSourcesFile = "data-sources.yaml"
	metadataFile    = ".metadata.yaml"
	dateFormat      = "2006-01-02"
	maxParallel     = 16
)

type MetadataEntry struct {
	LastModified string `yaml:"last_modified,omitempty"`
	ETag         string `yaml:"etag,omitempty"`
}

type Metadata map[string]MetadataEntry
type DataSources map[string][]string

type fetchTask struct {
	dir string
	url string
}

func main() {
	log.SetFlags(0)
	today := time.Now().Format(dateFormat)

	dataSources := readDataSources()
	metadata := readMetadata()
	tasks := make(chan fetchTask, maxParallel)

	var wg sync.WaitGroup
	var mu sync.Mutex
	hadError := false

	// Start worker pool
	for i := 0; i < maxParallel; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				if err := fetchOne(task.dir, task.url, today, metadata, &mu); err != nil {
					log.Printf("Error fetching %s: %v", task.url, err)
					mu.Lock()
					hadError = true
					mu.Unlock()
				}
			}
		}()
	}

	// Dispatch tasks
	for dir, urls := range dataSources {
		if err := os.MkdirAll(dir, 0755); err != nil {
			log.Fatalf("Failed to create directory %s: %v", dir, err)
		}
		for _, url := range urls {
			tasks <- fetchTask{dir: dir, url: url}
		}
	}
	close(tasks)
	wg.Wait()

	writeMetadata(metadata)

	if hadError {
		os.Exit(1)
	}
}

func fetchOne(dir, url, today string, metadata Metadata, mu *sync.Mutex) error {
	log.Printf("Checking %s", url)

	mu.Lock()
	meta := metadata[url]
	mu.Unlock()

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	if meta.LastModified != "" {
		req.Header.Set("If-Modified-Since", meta.LastModified)
	}
	if meta.ETag != "" {
		req.Header.Set("If-None-Match", meta.ETag)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified {
		log.Printf("Not modified: %s", url)
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected response: %s", resp.Status)
	}

	base := filepath.Base(url)
	outPath := filepath.Join(dir, strings.TrimSuffix(base, ".csv")+"-"+today+".csv")

	tmpFile, err := os.CreateTemp(dir, "tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())

	_, err = io.Copy(tmpFile, resp.Body)
	tmpFile.Close()
	if err != nil {
		return err
	}

	if err := os.Rename(tmpFile.Name(), outPath); err != nil {
		return err
	}
	log.Printf("Downloaded: %s → %s", url, outPath)

	// Update metadata
	newMeta := MetadataEntry{}
	if lm := resp.Header.Get("Last-Modified"); lm != "" {
		if t, err := http.ParseTime(lm); err == nil {
			os.Chtimes(outPath, time.Now(), t)
			newMeta.LastModified = lm
		}
	}
	if etag := resp.Header.Get("ETag"); etag != "" {
		newMeta.ETag = etag
	}

	mu.Lock()
	metadata[url] = newMeta
	mu.Unlock()
	return nil
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
