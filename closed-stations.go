package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

const dataSourcesFile = "data-sources.yaml"

type DataSources map[string][]string

func main() {
	dataSources := readDataSources()
	knownDirs := make(map[string]bool)
	for dir := range dataSources {
		knownDirs[dir] = true
	}

	entries, err := os.ReadDir(".")
	if err != nil {
		log.Fatalf("Failed to read current directory: %v", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			name := entry.Name()
			if strings.HasPrefix(name, ".") || name == "closed-stations" {
				continue // skip hidden and previously logged as closed
			}
			if !knownDirs[name] {
				fmt.Println(name)
			}
		}
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
