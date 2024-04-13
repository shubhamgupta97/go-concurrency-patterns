package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
)

func main() {
	ch, err := read("static/file.csv")
	if err != nil {
		panic(fmt.Errorf("opening file %v", err))
	}

	ch1 := fanout(ch)
	ch2 := fanout(ch)
	ch3 := fanout(ch)

	for {
		if ch1 == nil && ch2 == nil && ch3 == nil {
			break
		}

		select {
		case val, ok := <-ch1:
			if ok {
				fmt.Println("From ch1:", val)
			} else {
				ch1 = nil
			}
		case val, ok := <-ch2:
			if ok {
				fmt.Println("From ch2:", val)
			} else {
				ch2 = nil
			}
		case val, ok := <-ch3:
			if ok {
				fmt.Println("From ch3:", val)
			} else {
				ch3 = nil
			}
		}
	}

	fmt.Println("Completed.")
}

func fanout(ch <-chan []string) <-chan []string {

	out := make(chan []string)

	go func() {
		for val := range ch {
			out <- val
		}

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
