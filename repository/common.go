package repository

import (
	"errors"
	"hash/crc64"
	"log"
	"math/rand"
	"sync/atomic"
	"time"
)

type Cmd func(in, out chan interface{})

var GetMessagesMaxUsersBatch = 2
var HasSpamMaxAsyncRequests = 5

func init() {
	log.SetFlags(log.Default().Flags() | log.Lmicroseconds)
}

type User struct {
	ID    uint64
	Email string
}

type MsgID uint64

type MsgData struct {
	ID      MsgID
	HasSpam bool
}

// идем в "базу" чтоб получить user_id из email'а
// каждый запрос занимает 1 секунду
// можно без проблем выполнять параллельно
func GetUser(email string) (res User) {
	defer func(start time.Time) {
		log.Printf("[GetUser() %s] args:%v res:%v", time.Since(start), email, res)
	}(time.Now())

	atomic.AddUint32(&Statistic.RunGetUser, 1)

	time.Sleep(time.Second)

	remail := email
	alias := map[string]string{
		"batman@mail.ru":    "bruce.wayne@mail.ru",
		"spiderman@mail.ru": "peter.parker@mail.ru",
	}[remail]
	if alias != "" {
		remail = alias
	}

	// это симуляция похода в базу и получения реального id
	id := crc64.Checksum([]byte(remail), crc64.MakeTable(crc64.ISO))

	return User{
		ID:    id,
		Email: remail,
	}
}

// идем за списком писем
// каждый запрос занимает 1 секунду
// это API поддерживает батчи. то есть можно запросить за 1 вызов сразу информацию по нескольким юзерам
// GetMessagesMaxUsersBatch - максимальное кол-во юзеров, которое можно передать за 1 раз передать
func GetMessages(users ...User) (res []MsgID, err error) {
	defer func(start time.Time) {
		log.Printf("[GetMessages() %s] args:%+v res:%v err:%v", time.Since(start), users, res, err)
	}(time.Now())
	atomic.AddUint32(&Statistic.RunGetMessages, 1)
	atomic.AddUint32(&Statistic.GetMessagesTotalUsers, uint32(len(users)))

	time.Sleep(time.Second)

	if len(users) > GetMessagesMaxUsersBatch {
		atomic.AddUint32(&Statistic.ErrorGetMessage, 1)
		log.Printf("to many users in one batch request %v", users)
		return nil, errors.New("to many users")
	}

	// это симуляция похода в сервис хранения писем и получения списка писем по юзерам
	messages := make([]MsgID, 0, 10*len(users))
	for _, u := range users {
		r := rand.New(rand.NewSource(int64(u.ID))) //nolint: gosec
		n := r.Intn(10)
		for i := 0; i <= n; i++ {
			messages = append(messages, MsgID(r.Uint64()))
		}
	}
	return messages, nil
}

var antispamConcurrentRequests int32 = 0
var antispamRequestStart = func() bool {
	cr := atomic.AddInt32(&antispamConcurrentRequests, 1)
	return int(cr) <= HasSpamMaxAsyncRequests
}
var antispamRequestStop = func() {
	atomic.AddInt32(&antispamConcurrentRequests, -1)
}

// идем в антиспам
// каждый запрос занимает 100мс
// у него есть антибрут. то есть если запрашивать параллельно слишком часто, то дает "по рукам" и возвращает ошибку
// HasSpamMaxAsyncRequests - максимальное кол-во параллельных запросов
func HasSpam(id MsgID) (res bool, err error) {
	defer func(start time.Time) {
		log.Printf("[HasSpam() %s] args:%+v res:%v err:%v", time.Since(start), id, res, err)
	}(time.Now())

	atomic.AddUint32(&Statistic.RunHasSpam, 1)

	ok := antispamRequestStart()
	defer antispamRequestStop()

	time.Sleep(100 * time.Millisecond)

	if !ok {
		atomic.AddUint32(&Statistic.ErrorHasSpam, 1)
		log.Printf("got antibrute error from antispam for message %d", id)
		return true, errors.New("too many requests")
	}

	// это симуляция похода в сервис антиспама и получения факта реального наличия спама в письме
	r := rand.New(rand.NewSource(int64(id))) //nolint: gosec
	return r.Intn(2) == 1, nil               //nolint: gosec
}

type Stat struct {
	RunGetUser            uint32
	RunGetMessages        uint32
	GetMessagesTotalUsers uint32
	RunHasSpam            uint32
	ErrorGetMessage       uint32
	ErrorHasSpam          uint32
}

var Statistic = Stat{}
