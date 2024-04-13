package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sync"
)

func main() {
	ch1, err := read("static/file1.csv")
	if err != nil {
		panic(fmt.Errorf("opening file %v", err))
	}

	ch2, err := read("static/file2.csv")
	if err != nil {
		panic(fmt.Errorf("opening file %v", err))
	}

	chM := merge(ch1, ch2)

	for val := range chM {
		fmt.Println(val)
	}

	fmt.Println("Completed.")

}

func merge(cs ...<-chan []string) <-chan []string {
	out := make(chan []string)
	wg := &sync.WaitGroup{}

	writeChannel := func(ch <-chan []string) {
		for val := range ch {
			out <- val
		}
		wg.Done()
	}

	for _, ch := range cs {
		wg.Add(1)
		go writeChannel(ch)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out

}

func read(file string) (<-chan []string, error) {
	f, err := os.Open(file)
	if err != nil {
		// panic(fmt.Errorf("opening file %v", err))
		return nil, err
	}

	out := make(chan []string)

	reader := csv.NewReader(f)
	firstRecordRead := false

	go func() {
		for {
			row, err := reader.Read()

			if err == io.EOF {
				close(out)
				return
			}

			if err != nil {
				close(out)
				return
			}

			if !firstRecordRead {
				firstRecordRead = true
				continue
			}

			out <- row
		}
	}()

	return out, nil
}
