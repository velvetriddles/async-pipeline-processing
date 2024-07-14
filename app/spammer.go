package app

import (
	"async-pipeline/repository"
	"fmt"
	"sort"
	"sync"
)

func RunPipeline(cmds ...repository.Cmd) {
	in := make(chan interface{})
	wg := new(sync.WaitGroup)

	for _, c := range cmds {
		wg.Add(1)
		out := make(chan interface{})

		go func(wg *sync.WaitGroup, c repository.Cmd, in, out chan interface{}) {
			defer wg.Done()

			c(in, out)
			close(out)
		}(wg, c, in, out)
		in = out
	}

	wg.Wait()
}

func SelectUsers(in, out chan interface{}) {
	// 	in - string
	// 	out - User

	selected := make(map[uint64]bool)
	wg := new(sync.WaitGroup)
	mu := new(sync.Mutex)

	for email := range in {
		wg.Add(1)
		go func(wg *sync.WaitGroup, email string) {
			defer wg.Done()

			user := repository.GetUser(email)
			mu.Lock()
			defer mu.Unlock()
			if _, ok := selected[user.ID]; !ok {
				out <- user
				selected[user.ID] = true
			}
		}(wg, email.(string))
	}

	wg.Wait()
}

func SelectMessages(in, out chan interface{}) {
	// 	in - User
	// 	out - MsgID

	wg := new(sync.WaitGroup)
	users := make([]repository.User, 0, repository.GetMessagesMaxUsersBatch)

	for user := range in {
		users = append(users, user.(repository.User))
		if len(users) == repository.GetMessagesMaxUsersBatch {
			wg.Add(1)
			go GetMessagesForUsers(wg, out, users...)
			users = make([]repository.User, 0, repository.GetMessagesMaxUsersBatch)
		}
	}
	if len(users) != 0 {
		wg.Add(1)
		go GetMessagesForUsers(wg, out, users...)
	}

	wg.Wait()
}

func GetMessagesForUsers(wg *sync.WaitGroup, out chan interface{}, users ...repository.User) {
	defer wg.Done()

	msgs, err := repository.GetMessages(users...)
	if err != nil {
		return
	}
	for _, msg := range msgs {
		out <- msg
	}
}

func CheckSpam(in, out chan interface{}) {
	// in - MsgID
	// out - MsgData

	limit := make(chan struct{}, repository.HasSpamMaxAsyncRequests)
	wg := new(sync.WaitGroup)

	for id := range in {
		wg.Add(1)
		go func(wg *sync.WaitGroup, out chan interface{}, limit chan struct{}, id repository.MsgID) {
			defer wg.Done()

			limit <- struct{}{}
			defer func() { <-limit }()

			spam, err := repository.HasSpam(id)
			if err != nil {
				return
			}
			out <- repository.MsgData{ID: id, HasSpam: spam}
		}(wg, out, limit, id.(repository.MsgID))
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	// in - MsgData
	// out - string

	results := make([]repository.MsgData, 0)
	for data := range in {
		results = append(results, data.(repository.MsgData))
	}

	sort.Slice(results, func(i, j int) bool {
		if results[i].HasSpam == results[j].HasSpam {
			return results[i].ID < results[j].ID
		}
		return results[i].HasSpam
	})

	for _, data := range results {
		out <- fmt.Sprintf("%t %d", data.HasSpam, data.ID)
	}
}
