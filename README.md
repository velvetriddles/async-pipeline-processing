# Async Pipeline Project

This project implements an asynchronous pipeline for processing user messages and checking for spam. It demonstrates the use of Go's concurrency features, including goroutines and channels.

## Project Structure

The project consists of two main files:

1. `spammer.go`: Contains the main pipeline logic and processing functions.
2. `common.go`: Defines common types, interfaces, and simulated database operations.

## Features

- Asynchronous processing of user messages
- Concurrent user lookup and message retrieval
- Spam checking with rate limiting
- Result combination and sorting

## Main Components

### Pipeline Functions

1. `RunPipeline`: Orchestrates the execution of the entire pipeline.
2. `SelectUsers`: Retrieves user information based on email addresses.
3. `SelectMessages`: Fetches messages for selected users.
4. `CheckSpam`: Checks messages for spam content.
5. `CombineResults`: Combines and sorts the final results.

### Simulated Database Operations

- `GetUser`: Simulates user lookup from a database.
- `GetMessages`: Simulates retrieval of messages for given users.
- `HasSpam`: Simulates spam checking for a message.

## Configuration

The project includes some configurable parameters:

- `GetMessagesMaxUsersBatch`: Maximum number of users that can be processed in a single batch (default: 2).
- `HasSpamMaxAsyncRequests`: Maximum number of concurrent spam-checking requests (default: 5).

## Usage

To use this pipeline, you need to implement the `repository.Cmd` functions and pass them to the `RunPipeline` function. For example:

```go

package main

import (
	"fmt"
	"project/app"
	"project/repository"
)

func main() {
	inputData := []string{
		"example1@mail.com",
		"example2@mail.com",
		"example3@mail.com",
	}

	outputData := []string{}
	app.RunPipeline(
		repository.Cmd(newCatStrings(inputData, 0)),
		repository.Cmd(app.SelectUsers),
		repository.Cmd(app.SelectMessages),
		repository.Cmd(app.CheckSpam),
		repository.Cmd(app.CombineResults),
		repository.Cmd(newCollectStrings(&outputData)),
	)

	for _, data := range outputData {
		fmt.Println(data)
	}
}

```

## Statistics

The project includes a `Statistic` struct that tracks various metrics during the pipeline execution, such as the number of user lookups, message retrievals, and spam checks performed.

## Error Handling

The pipeline includes basic error handling for scenarios such as:

- Exceeding the maximum batch size for message retrieval
- Rate limiting in spam checking

## Notes

- This project uses simulated database operations with artificial delays to mimic real-world scenarios.
- The spam-checking function includes a simple anti-brute-force mechanism to limit concurrent requests.


## Contact

@velvetriddles