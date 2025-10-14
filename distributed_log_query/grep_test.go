package main

import (
	"fmt"
	"testing"
	"time"
)

var vmList = []VMInfo{
	{ID: "vm1", Address: "fa25-cs425-3701.cs.illinois.edu:8080"},
	{ID: "vm2", Address: "fa25-cs425-3702.cs.illinois.edu:8080"},
	{ID: "vm3", Address: "fa25-cs425-3703.cs.illinois.edu:8080"},
	{ID: "vm4", Address: "fa25-cs425-3704.cs.illinois.edu:8080"},
}

func runPatternTest(t *testing.T, pattern string, options []string, label string) {
	fmt.Printf("\n======Test%s=====\n", label)

	for i := 0; i < 5; i++ {
		timestamp := time.Now().Format("20060102_150405")
		start := time.Now()

		executeParallelGrep(pattern, options, vmList[0].ID, vmList[1:], timestamp) // vmList[0] is main, others are remote
		latency := time.Since(start)

		fmt.Printf("%s Pattern: Iteration %d, Pattern=%s, Options=%v, Latency=%v\n",
			label, i+1, pattern, options, latency)
	}
}

func TestInFrequentPatterns(t *testing.T) {
	runPatternTest(t, "delete", []string{"-i"}, "InFrequent")
}

func TestFrequentPatterns(t *testing.T) {
	runPatternTest(t, "get", []string{"-i"}, "Frequent")
}

func TestSomewhatFrequentPatterns(t *testing.T) {
	runPatternTest(t, "put", []string{"-i"}, "SomewhatFrequent")
}

func TestCountForFrequentPatterns(t *testing.T) {
	runPatternTest(t, "get", []string{"-i", "-c"}, "CountForFrequentPatterns")
}

func TestLogGeneration(t *testing.T) {
	fmt.Print("\n======TestLogGeneration=====\n")

	// Test generating logs for a few VMs
	testVMID := "vm1"
	testLines := 300000
	GenerateLogFile(testVMID, testLines)
	executeFileGenerationInOtherVMs(testVMID, testLines)
}
