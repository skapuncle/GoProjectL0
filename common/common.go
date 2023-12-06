package common

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/nats-io/stan.go"
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

// Запуск сборщика мусора в горутине
func (c *Cache) StartGC() {
	go c.GC()
}

// Сборщик мусора
func (c *Cache) GC() {
	for {
		// Ожидаем время установленное в cleanupInterval
		<-time.After(c.cleanupInterval)
		if c.items == nil {
			return
		}
		// Ищем элементы с истекшим временем жизни и удаляем из хранилища
		if keys := c.expiredKeys(); len(keys) != 0 {
			c.clearItems(keys)
		}
	}
}

// Функция expiredKeys возвращает список "просроченных" ключей
func (c *Cache) expiredKeys() (keys []string) {
	c.RLock()

	defer c.RUnlock()

	for k, i := range c.items {
		if time.Now().UnixNano() > i.Expiration && i.Expiration > 0 {
			keys = append(keys, k)
		}
	}
	return
}

// clearItems удаляет ключи из переданного списка
func (c *Cache) clearItems(keys []string) {
	c.Lock()
	defer c.Unlock()

	for _, k := range keys {
		delete(c.items, k)
	}
}

// Connector - структура для подключения к БД
type Connector struct {
	Uname  string
	Pass   string
	Port   string
	Host   string
	DBname string
}

// All - структура со всем, что может понадобиться для работы
type All struct {
	Connctr    Connector
	Ordr       Order
	Pool       *pgxpool.Pool
	Cch        *Cache
	StreamConn stan.Conn
	StreamSubs stan.Subscription
}

// GetPGSQL - метод для генерации строки
func (c Connector) GetPGSQL() string {
	return fmt.Sprintf("postgresql://%s:%s@%s:%s/%s", c.Uname, c.Pass, c.Host, c.Port, c.DBname)
}

// NewAll - метод для создания новой структуры с коннектором и временными интервалами кэша
func NewAll(c Connector, defaultExpiration, cleanupInterval time.Duration) *All {
	return &All{Connctr: c, Cch: NewCache(defaultExpiration, cleanupInterval)}
}

// FromDbToCacheByKey - метод для подгрузки данных из БД в кэш, если в заказе есть номер
func (a *All) FromDbToCacheByKey() error {
	if a.Ordr.OrderUID == "" {
		return fmt.Errorf("Key is empty")
	}
	var DelId, PayId string
	query := `Select TrackNumber, Entry, Deliveries, Pays, Items, Locale, InternalSignature, CustomerID, DeliveryService, Shardkey, SmID, DateCreated, OofShard from orders where orderUID = $1`
	rows, err := a.Pool.Query(context.TODO(), query, a.Ordr.OrderUID)
	if err != nil {
		fmt.Errorf("Select from Order failed: %v", err)
	}
	it := make([]int, 0)
	for rows.Next() {
		err = rows.Scan(&a.Ordr.TrackNumber, &a.Ordr.Entry, &DelId, &PayId, &it, &a.Ordr.Locale, &a.Ordr.InternalSignature, &a.Ordr.CustomerID, &a.Ordr.DeliveryService, &a.Ordr.Shardkey, &a.Ordr.SmID, &a.Ordr.DateCreated, &a.Ordr.OofShard)
		if err != nil {
			return fmt.Errorf("Scanning rows from selected order failed: %v", err)
		}
	}

	for i := 0; i < len(it); i++ {
		query = `Select chrtid, TrackNumber, Price, Rid, Item_name, Sale, Size, TotalPrice, NmID, Brand, Status from item where chrtid = $1`
		rows, err := a.Pool.Query(context.TODO(), query, it[i])
		if err != nil {
			return fmt.Errorf("Select from Items failed: %v", err)
		}
		for rows.Next() {
			var utem Item
			err = rows.Scan(&utem.ChrtID, &utem.TrackNumber, &utem.Price, &utem.Rid, &utem.Name, &utem.Sale, &utem.Size, &utem.TotalPrice, &utem.NmID, &utem.Brand, &utem.Status)
			if err != nil {
				return fmt.Errorf("Scanning rows from selected items failed: %v", err)
			}
			a.Ordr.Items = append(a.Ordr.Items, utem)
		}
	}
	query = `Select del_name, Phone, Zip, City, Address, Region, Email from delivery where del_id = $1`
	rows, err = a.Pool.Query(context.TODO(), query, DelId)
	if err != nil {
		return fmt.Errorf("Select from Delivery failed: %v", err)
	}
	for rows.Next() {
		err = rows.Scan(&a.Ordr.Deliveries.Name, &a.Ordr.Deliveries.Phone, &a.Ordr.Deliveries.Zip, &a.Ordr.Deliveries.City, &a.Ordr.Deliveries.Address, &a.Ordr.Deliveries.Region, &a.Ordr.Deliveries.Email)
		if err != nil {
			return fmt.Errorf("Scanning rows from selected delivery failed: %v", err)
		}
	}
	query = `select Transaction, RequestID, Currency, Provider, Amount, PaymentDt, Bank, DeliveryCost, GoodsTotal, CustomFee from payment where pay_id = $1`
	rows, err = a.Pool.Query(context.TODO(), query, PayId)
	if err != nil {
		return fmt.Errorf("Select from Payment failed: %v", err)
	}
	for rows.Next() {
		err = rows.Scan(&a.Ordr.Pays.Transaction, &a.Ordr.Pays.RequestID, &a.Ordr.Pays.Currency, &a.Ordr.Pays.Provider, &a.Ordr.Pays.Amount, &a.Ordr.Pays.PaymentDt, &a.Ordr.Pays.Bank, &a.Ordr.Pays.DeliveryCost, &a.Ordr.Pays.GoodsTotal, &a.Ordr.Pays.CustomFee)
		if err != nil {
			return fmt.Errorf("Scanning rows from selected payments failed: %v", err)
		}
	}

	a.Cch.Set(a.Ordr.OrderUID, a.Ordr, 5*time.Minute)
	return nil
}
