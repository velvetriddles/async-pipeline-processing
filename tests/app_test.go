package test

import (
	"async-pipeline/app"
	"async-pipeline/repository"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPipeline(t *testing.T) {
	var ok = true
	var recieved uint32
	freeFlowCmds := []repository.Cmd{
		repository.Cmd(func(in, out chan interface{}) {
			out <- 1
			time.Sleep(10 * time.Millisecond)
			currRecieved := atomic.LoadUint32(&recieved)
			if currRecieved == 0 {
				ok = false
			}
		}),
		repository.Cmd(func(in, out chan interface{}) {
			for range in {
				atomic.AddUint32(&recieved, 1)
			}
		}),
	}
	repository.Statistic = repository.Stat{}
	app.RunPipeline(freeFlowCmds...)

	assert.True(t, ok,
		"во второй джобе не увеличился счетчик, а в первой уже дошли до следующего действия")
	assert.NotEqual(t, 0, recieved,
		"счетчик recieved в итоге не увеличился, а должен был")
}

func TestPipeline2(t *testing.T) {
	var recieved uint32
	freeFlowCmds := []repository.Cmd{
		repository.Cmd(func(in, out chan interface{}) {
			out <- uint32(1)
			out <- uint32(3)
			out <- uint32(4)
		}),
		repository.Cmd(func(in, out chan interface{}) {
			for val := range in {
				out <- val.(uint32) * 3
				time.Sleep(time.Millisecond * 100)
			}
		}),
		repository.Cmd(func(in, out chan interface{}) {
			for val := range in {
				fmt.Println("collected", val)
				atomic.AddUint32(&recieved, val.(uint32))
			}
		}),
	}

	timeStart := time.Now()

	repository.Statistic = repository.Stat{}
	app.RunPipeline(freeFlowCmds...)

	expectedTime := time.Millisecond * 350
	timeEnd := time.Since(timeStart)
	assert.Less(t, timeEnd, expectedTime,
		"execution too long. Got: %s. Expected: <%s", timeEnd.String(), expectedTime.String())
	assert.Equal(t, uint32((1+3+4)*3), recieved,
		"f3 have not collected inputs, recieved = %d", recieved)
}

func newCatStrings(strs []string, pauses time.Duration) func(in, out chan interface{}) {
	return func(in, out chan interface{}) {
		for _, email := range strs {
			out <- email
			if pauses != 0 {
				time.Sleep(pauses)
			}
		}
	}
}

func newCollectStrings(strs *[]string) func(in, out chan interface{}) {
	return func(in, out chan interface{}) {
		for dataRaw := range in {
			data := fmt.Sprintf("%v", dataRaw)
			*strs = append(*strs, data)
		}
	}
}

func TestUsersRace(t *testing.T) {
	inputData := make([]string, 10000)
	for i := range inputData {
		inputData[i] = fmt.Sprintf("bruce.wayne%d@mail.ru", i/10)
	}

	testResult := []string{}
	repository.Statistic = repository.Stat{}
	app.RunPipeline(
		repository.Cmd(newCatStrings(inputData, 0)),
		repository.Cmd(app.SelectUsers),
		repository.Cmd(newCollectStrings(&testResult)),
	)

	assert.Equal(t, 1000, len(testResult),
		"итоговый результат отличается от ожидаемого")
}

func TestAlias(t *testing.T) {
	inputData := []string{
		"batman@mail.ru",
		"bruce.wayne@mail.ru",
	}
	expectedOutput := []string{
		"{12499983457589032104 bruce.wayne@mail.ru}",
	}

	testResult := []string{}
	repository.Statistic = repository.Stat{}
	app.RunPipeline(
		repository.Cmd(newCatStrings(inputData, 0)),
		repository.Cmd(app.SelectUsers),
		repository.Cmd(newCollectStrings(&testResult)),
	)

	assert.Equal(t, expectedOutput, testResult,
		"итоговый результат отличается от ожидаемого")
}

func TestParallelPiplines(t *testing.T) {
	inputData := []string{
		"1000@mail.ru",
		"1001@mail.ru",
		"1002@mail.ru",
	}

	repository.Statistic = repository.Stat{}
	cntFirst, cntSecond := 0, 0

	timeStart := time.Now()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		app.RunPipeline(
			repository.Cmd(newCatStrings(inputData, 150*time.Millisecond)),
			repository.Cmd(app.SelectUsers),
			repository.Cmd(app.SelectMessages),
			repository.Cmd(func(in, out chan interface{}) {
				for range in {
					cntFirst++
				}
			}),
		)
	}()

	app.RunPipeline(
		repository.Cmd(newCatStrings(inputData, 100*time.Millisecond)),
		repository.Cmd(app.SelectUsers),
		repository.Cmd(app.SelectMessages),
		repository.Cmd(func(in, out chan interface{}) {
			for range in {
				cntSecond++
			}
		}),
	)

	wg.Wait()

	expectedTime := 2700 * time.Millisecond
	if runtime.GOOS == "windows" {
		expectedTime += 50 * time.Millisecond
	}

	timeEnd := time.Since(timeStart)
	assert.Less(t, timeEnd, expectedTime,
		"параллельные пайплайны не должны влиять друг на друга. скорость их выполнения зависит от самого медленного")
	assert.Equal(t, 22, cntFirst)
	assert.Equal(t, 22, cntSecond)
}

func TestTotal(t *testing.T) {
	inputData := []string{
		"harry.dubois@mail.ru",
		"k.kitsuragi@mail.ru",
		"d.vader@mail.ru",
		"noname@mail.ru",
		"e.musk@mail.ru",
		"spiderman@mail.ru",
		"red_prince@mail.ru",
		"tomasangelo@mail.ru",
		"batman@mail.ru",
		"bruce.wayne@mail.ru",
	}
	expectedOutput := []string{
		"true 221945221381252775",
		"true 357347175551886490",
		"true 1595319133252549342",
		"true 1877225754447839300",
		"true 4652873815360231330",
		"true 5108368734614700369",
		"true 7829088386935944034",
		"true 8065084208075053255",
		"true 9323185346293974544",
		"true 10463884548348336960",
		"true 11204847394727393252",
		"true 12026159364158506481",
		"true 12386730660396758454",
		"true 12556782602004681106",
		"true 12728377754914798838",
		"true 13245035231559086127",
		"true 14107154567229229487",
		"true 16476037061321929257",
		"true 16728486308265447483",
		"true 17087986564527251681",
		"true 17259218828069106373",
		"true 17696166526272393238",
		"false 26236336874602209",
		"false 59892029605752939",
		"false 221962074543525747",
		"false 378045830174189628",
		"false 2803967521226628027",
		"false 6652443725402098015",
		"false 7594744397141820297",
		"false 9656111811170476016",
		"false 10167774218733491071",
		"false 10462184946173556768",
		"false 10493933060383355848",
		"false 10523043777071802347",
		"false 11512743696420569029",
		"false 12792092352287413255",
		"false 12975933273041759035",
		"false 14498495926778052146",
		"false 15161554273155698590",
		"false 15262116397886015961",
		"false 15728889559763622673",
		"false 15784986543485231004",
	}

	timeStart := time.Now()
	testResult := []string{}
	repository.Statistic = repository.Stat{}
	app.RunPipeline(
		repository.Cmd(newCatStrings(inputData, 0)),
		repository.Cmd(app.SelectUsers),
		repository.Cmd(app.SelectMessages),
		repository.Cmd(app.CheckSpam),
		repository.Cmd(app.CombineResults),
		repository.Cmd(newCollectStrings(&testResult)),
	)

	expectedTime := 3000 * time.Millisecond
	if runtime.GOOS == "windows" {
		expectedTime += 50 * time.Millisecond
	}
	timeEnd := time.Since(timeStart)
	assert.Less(t, timeEnd, expectedTime,
		"слишком долгое выполнение. что-то где-то нераспараллелено. должно быть не больше, чем %s, а было %s", expectedTime, timeEnd)
	assert.Equal(t, expectedOutput, testResult,
		"итоговый результат отличается от ожидаемого")
	expectedStat := repository.Stat{
		RunGetUser:            uint32(10),
		RunGetMessages:        uint32(5),
		GetMessagesTotalUsers: uint32(9),
		RunHasSpam:            uint32(42),
	}
	assert.Equal(t, repository.Statistic, expectedStat, "количество вызовов функций не совпадает с ожидаемым")
}
