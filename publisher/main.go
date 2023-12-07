package main

import (
	"GoProjectL0/common"
	"encoding/json"
	"fmt"
	"github.com/nats-io/stan.go"
	"math"
	"os"
	"os/signal"
	"time"
)

func main() {
	// подключаемся к серверу сообщений
	StreamConnection, err := stan.Connect("test-cluster", "client-publisher", stan.NatsURL("0.0.0.0:4222"))
	if err != nil {
		fmt.Println(time.Now(), "Connection err", err)
		os.Exit(1)
	}
	defer StreamConnection.Close()

	// запускаем цикл генерации и передачи сообщений в заказ
	for i := 0; i < math.MaxInt; i++ {
		GeneratedOrder := common.NewOrderGen()         // генерируемм заказ
		JsonOrder, err := json.Marshal(GeneratedOrder) // превращаем в json
		if err != nil {
			fmt.Println(time.Now(), "JSON err:", err)
			continue
		}
		err = StreamConnection.Publish("foo", JsonOrder) // отправляем в канал
		if err != nil {
			fmt.Println(time.Now(), "Publish err:", err)
			continue
		}
		// сообщение об индексе заказа и его уникальный номер, отсюда можно брать информацию, чтобы потом на сайте
		// посмотреть успешно добавилось в базу данных и/или кэш или нет
		fmt.Println(time.Now(), "Index =", i, "OrderUID =", GeneratedOrder.OrderUID)
		// частота сообщений пока регулируется этим sleep'ом можно менять значения, но у меня тормознутый комп
		// поэтому Я оставлю 30 секунд
		time.Sleep(30 * time.Second)
		if i%10 == 0 && i != 0 {
			time.Sleep(10 * time.Minute)
			// после отправления 10 записей паблишер замолкает на 10 минут, если с 0, то 11)
		}
	}
	// заглушка для завершения работы
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println(time.Now(), "Received an interrupt, closing connection...")

}
