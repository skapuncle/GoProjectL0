package main

import (
	"GoProjectL0/common"
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/nats-io/stan.go"
	"net/http"
	"os"
	"os/signal"
	"time"
)

func main() {
	var err error
	fmt.Println(time.Now(), "Work is beginning.")

	// Создаем новый экземпляр структуры All с подключением к базе данных и кэшем
	ServStruck := common.NewAll(common.Connector{Uname: "postgres", Pass: "1234", Host: "localhost", Port: "5432", DBname: "mydb"}, 15*time.Minute, 3*time.Minute)

	// Получаем строку для подключения к базе данных
	StringOfConnectionToDataBase := ServStruck.Connctr.GetPGSQL()

	// Подключаемся к базе данных
	ServStruck.Pool, err = pgxpool.Connect(context.TODO(), StringOfConnectionToDataBase)
	if err != nil {
		fmt.Println("Unable to connect to database:", err)
		os.Exit(1)
	}
	fmt.Println(time.Now(), "Connected to Database. Success")

	// Инициализируем кэш
	err = ServStruck.UploadCache()
	if err != nil {
		fmt.Println(time.Now(), "caching data going wrong:", err)
		os.Exit(1)
	}

	// Подключаемся к серверу сообщений
	ServStruck.StreamConn, err = stan.Connect("test-cluster", "client-123", stan.NatsURL("0.0.0.0:4222"))
	if err != nil {
		fmt.Println("Can't connect to cluster", err)
		os.Exit(1)
	}
	fmt.Println(time.Now(), "Connected to cluster. Success")

	// Подписываемся на канал в сервере сообщений
	ServStruck.StreamSubs, err = ServStruck.StreamConn.Subscribe("foo", ServStruck.MessageHandler)
	if err != nil {
		fmt.Println("Can't subscribe to chanel:", err)
		os.Exit(1)
	}
	fmt.Println(time.Now(), "Subscribe is done. Succsess")

	// Запускаем HTTP-сервер, который слушает на порту 3000 и обрабатывает запросы с помощью метода OrderHandler экземпляра All
	http.HandleFunc("/", ServStruck.OrderHandler)
	err = http.ListenAndServe(":3000", nil)
	if err != nil {
		fmt.Println(time.Now(), "\"http.ListenAndServe\" have some err to you", err)
		os.Exit(1)
	}
	fmt.Println(time.Now(), "Listening on port: 3000")

	// Ожидаем прерывание сигнала от операционной системы, чтобы корректно отписаться от канала и закрыть соединения с сервером сообщений и базой данных
	signalChan := make(chan os.Signal, 1)
	cleanupDone := make(chan bool)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for range signalChan {
			fmt.Println(time.Now(), "Received an interrupt, unsubscribing and closing connection...")
			err := ServStruck.StreamSubs.Unsubscribe()
			if err != nil {
				fmt.Println(time.Now(), "trouble in unsubscribing:", err)
			}
			err = ServStruck.StreamConn.Close()
			if err != nil {
				fmt.Println(time.Now(), "Closing connection with stream server going wrong", err)
			}
			ServStruck.Pool.Close()
			cleanupDone <- true
		}
	}()
	<-cleanupDone
	fmt.Println(time.Now(), "Exiting...")
}
