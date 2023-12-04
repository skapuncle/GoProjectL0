package common

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

// Структура таблицы Delivery
type Delivery struct {
	Name    string `json:"name"`
	Phone   string `json:"phone"`
	Zip     string `json:"zip"`
	City    string `json:"city"`
	Address string `json:"address"`
	Region  string `json:"region"`
	Email   string `json:"email"`
}

// Метод для генерации структуры Delivery псевдослучайными значениями
func NewDeliveryGen() *Delivery {
	i := rand.Int()
	return &Delivery{"name" + strconv.Itoa(i), "phone" + strconv.Itoa(i), "zip" + strconv.Itoa(i), "city" + strconv.Itoa(i), "address" + strconv.Itoa(i), "region" + strconv.Itoa(i), "email" + strconv.Itoa(i)}
}

// Структура таблицы Payment
type Payment struct {
	Transaction  string `json:"transaction"`
	RequestID    string `json:"request_id"`
	Currency     string `json:"currency"`
	Provider     string `json:"provider"`
	Amount       int    `json:"amount"`
	PaymentDt    int    `json:"payment_dt"`
	Bank         string `json:"bank"`
	DeliveryCost int    `json:"delivery_cost"`
	GoodsTotal   int    `json:"goods_total"`
	CustomFee    int    `json:"custom_fee"`
}

// Метод для заполнения структуры Payment
func NewPaymentGen() *Payment {
	i := rand.Int()
	return &Payment{"transaction" + strconv.Itoa(i), "requestID" + strconv.Itoa(i), "currency" + strconv.Itoa(i), "provider" + strconv.Itoa(i), i, i, "bank" + strconv.Itoa(i), i, i, i}
}

// Структура таблицы Item
type Item struct {
	ChrtID      int    `json:"chrt_id"`
	TrackNumber string `json:"track_number"`
	Price       int    `json:"price"`
	Rid         string `json:"rid"`
	Name        string `json:"name"`
	Sale        int    `json:"sale"`
	Size        string `json:"size"`
	TotalPrice  int    `json:"total_price"`
	NmID        int    `json:"nm_id"`
	Brand       string `json:"brand"`
	Status      int    `json:"status"`
}

// Генератор структур Item по заданному колличеству
func NewItemGen(number int) []Item {
	It := make([]Item, number)
	number--
	for number > 0 {
		var i = rand.Int()
		It[number] = Item{i, "trackNumber" + strconv.Itoa(i), i, "rid" + strconv.Itoa(i), "name" + strconv.Itoa(i), i, "size" + strconv.Itoa(i), i, i, "brand" + strconv.Itoa(i), i}
		number--
	}
	return It
}

// Структура таблицы Order
type Order struct {
	OrderUID          string    `json:"order_uid"`
	TrackNumber       string    `json:"track_number"`
	Entry             string    `json:"entry"`
	Deliveries        Delivery  `json:"delivery"`
	Pays              Payment   `json:"payment"`
	Items             []Item    `json:"items"`
	Locale            string    `json:"locale"`
	InternalSignature string    `json:"internal_signature"`
	CustomerID        string    `json:"customer_id"`
	DeliveryService   string    `json:"delivery_service"`
	Shardkey          string    `json:"shardkey"`
	SmID              int       `json:"sm_id"`
	DateCreated       time.Time `json:"date_created"`
	OofShard          string    `json:"oof_shard"`
}

// NewOrderGen генерирует заказ
func NewOrderGen() *Order {
	var i = rand.Int()
	var k = rand.Intn(10)
	var D = NewDeliveryGen()
	var P = NewPaymentGen()
	var I = NewItemGen(k)
	return &Order{"orderUID" + strconv.Itoa(i), "trackNumber" + strconv.Itoa(i), "entry" + strconv.Itoa(i), *D, *P, I, "locale" + strconv.Itoa(i), "internalSignature" + strconv.Itoa(i), "customerID" + strconv.Itoa(i), "deliveryService" + strconv.Itoa(i), "shardkey" + strconv.Itoa(i), i, time.Now().Add(time.Duration(i) * time.Millisecond), "oofShard" + strconv.Itoa(i)}
}

// Структура кэша
type Cache struct {
	sync.RWMutex
	defaultExpiration time.Duration
	cleanupInterval   time.Duration
	items             map[string]CacheItem
}

// Структура элемента кэша
type CacheItem struct {
	Value      interface{}
	Created    time.Time
	Expiration int64
}

// Инициализация кэша
func NewCache(defaultExpiration, cleanupInterval time.Duration) *Cache {
	items := make(map[string]CacheItem)

	cache := Cache{
		items:             items,
		defaultExpiration: defaultExpiration,
		cleanupInterval:   cleanupInterval,
	}

	// Если интервал очистки больше 0, запускаем очистку
	if cleanupInterval > 0 {
		cache.StartGC()
	}

	return &cache
}

func (c *Cache) Set(key string, value interface{}, duration time.Duration) {
	var expiration int64

	// Если продолжительность жизни равно 0 - используется значение по умолчанию
	if duration == 0 {
		duration = c.defaultExpiration
	}

	if duration > 0 {
		expiration = time.Now().Add(duration).UnixNano()
	}

	c.Lock()

	defer c.Unlock()

	if _, ok := c.items[key]; ok == true {
		fmt.Println("Owerriting is not allowed")
		return
	}
	c.items[key] = CacheItem{
		Value:      value,
		Expiration: expiration,
		Created:    time.Now(),
	}
}

// Get - метод для получения данных из кэша

func (c *Cache) Get(key string) (interface{}, bool) {
	c.RLock()

	defer c.RUnlock()

	item, found := c.items[key]

	// Ключ не найден
	if !found {
		return nil, false
	}

	// Проверка на установку времени истечения
	if item.Expiration > 0 {
		// Если в момент запроса кэш устарел возвращаем nil
		if time.Now().UnixNano() > item.Expiration {
			return nil, false
		}
	}

	return item.Value, true

}

func (c *Cache) Delete(key string) error {
	c.Lock()

	defer c.Unlock()

	if _, found := c.items[key]; !found {
		return errors.New("Key not found!")
	}

	delete(c.items, key)

	return nil

}
