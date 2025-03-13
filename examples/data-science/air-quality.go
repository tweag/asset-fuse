package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
)

const (
	fineParticlesID   = 365
	columnIndicatorID = 1
	columnDataValue   = 10
)

func main() {
	if len(os.Args) != 2 {
		fmt.Println("Usage: air-quality <file>")
		os.Exit(1)
	}
	f, err := os.Open(os.Args[1])
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer f.Close()
	min, max, err := calculateMinMax(f)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("Min fine particles: %f\n", min)
	fmt.Printf("Max fine particles: %f\n", max)
}

func calculateMinMax(r io.Reader) (float64, float64, error) {
	min := math.MaxFloat64
	max := -math.MaxFloat64

	csvReader := csv.NewReader(r)
	// skip over header
	csvReader.Read()
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, 0, err
		}
		indicator, err := strconv.Atoi(record[columnIndicatorID])
		if err != nil {
			return 0, 0, err
		}
		value, err := strconv.ParseFloat(record[columnDataValue], 64)
		if err != nil {
			return 0, 0, err
		}
		if indicator != fineParticlesID {
			continue
		}
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	return min, max, nil
}
