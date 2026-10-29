package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type Application struct {
	Id        int
	Title     string
	Processed bool
	Canceled  bool
	CreatedOn time.Time
	UpdateOn  time.Time
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	titles := []string{"Alice", "Ben", "Den", "Jack", "Mike"}
	apps := initApplications(titles)

	results := make(chan Application, len(apps))

	var processing sync.WaitGroup
	for _, app := range apps {
		processing.Add(1)
		go func(a Application) {
			defer processing.Done()
			childCtx, childCancel := context.WithTimeout(ctx, 3*time.Second)
			defer childCancel()
			processingApplication(childCtx, a, results)
		}(app)
	}

	processing.Wait()
	close(results)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		applicationMonitor(results)
	}()
	wg.Wait()
}

func processingApplication(ctx context.Context, application Application, results chan<- Application) {
	fmt.Printf("Начало обработки заказа Id:%d \n", application.Id)
	select {
	case <-ctx.Done():
		application.Canceled = true
		application.UpdateOn = time.Now()
		fmt.Printf("Id:%d отменён по таймауту\n", application.Id)
	case <-time.After(time.Duration(1+rand.Intn(4)) * time.Second):
		application.Processed = true
		application.UpdateOn = time.Now()
		fmt.Printf("Id:%d успешно обработан\n", application.Id)
	}
	results <- application
}

func initApplications(titles []string) []Application {
	result := make([]Application, 0, len(titles))
	for i, title := range titles {
		result = append(result, Application{
			Id:        i,
			Title:     title,
			Processed: false,
			Canceled:  false,
			CreatedOn: time.Now(),
			UpdateOn:  time.Now(),
		})
	}
	return result
}

func applicationMonitor(results <-chan Application) {
	for res := range results {
		fmt.Printf("Id:%d; Title:%s; Processed:%t; Canceled:%t; CreatedOn:%s; UpdateOn:%s\n",
			res.Id,
			res.Title,
			res.Processed,
			res.Canceled,
			res.CreatedOn.Format(time.RFC3339),
			res.UpdateOn.Format(time.RFC3339),
		)
	}
}
