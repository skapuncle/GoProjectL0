package common

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/nats-io/stan.go"
	"html/template"
	"math/rand"
	"net/http"
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

// UploadCache - метод для добавления данных из БД в кэш при старте программы
func (a *All) UploadCache() error {
	query := `select orderuid from orders`
	rows, err := a.Pool.Query(context.TODO(), query)
	if err != nil {
		fmt.Println(time.Now(), "Query error: ", err)
		return err
	}
	for rows.Next() {
		err = rows.Scan(&a.Ordr.OrderUID)
		if err != nil {
			fmt.Println(time.Now(), "rows scanning error:", err)
			return err
		}
	}
	err = a.FromDbToCacheByKey()
	if err != nil {
		fmt.Println(time.Now(), "caching data from db going wrong:", err)
		return err
	}
	return nil
}

// MessageHandler - обработчик сообщений из канала nats-streaming
func (a *All) MessageHandler(m *stan.Msg) {
	err := json.Unmarshal(m.Data, &a.Ordr)
	if err != nil {
		fmt.Println(err, "Json")
		return
	}
	a.Cch.Set(a.Ordr.OrderUID, a.Ordr, 5*time.Minute)
	fmt.Println(time.Now(), a.Ordr.OrderUID, "putted in cache")

	insertDelivery(a)
	insertPayment(a)
	insertOrder(a)
	insertItems(a)
}

// insertDelivery: Вставляет данные о доставке в таблицу delivery
func insertDelivery(a *All) {
	query := "INSERT INTO delivery (del_name, Phone, Zip, City, Address, Region, Email)	Values ($1, $2, $3, $4, $5, $6, $7) returning del_id"
	err := a.Pool.QueryRow(context.TODO(), query, a.Ordr.Deliveries.Name, a.Ordr.Deliveries.Phone, a.Ordr.Deliveries.Zip, a.Ordr.Deliveries.City, a.Ordr.Deliveries.Address, a.Ordr.Deliveries.Region, a.Ordr.Deliveries.Email)
	if err != nil {
		fmt.Println(time.Now(), "Insert to Delivery failed:", err)
	}
}

// insertPayment: Вставляет данные о платеже в таблицу payment
func insertPayment(a *All) {
	query := "INSERT INTO payment (Transaction, RequestID, Currency, Provider, Amount, PaymentDt, Bank, DeliveryCost, GoodsTotal, CustomFee)	Values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) returning pay_id"
	err := a.Pool.QueryRow(context.TODO(), query, a.Ordr.Pays.Transaction, a.Ordr.Pays.RequestID, a.Ordr.Pays.Currency, a.Ordr.Pays.Provider, a.Ordr.Pays.Amount, a.Ordr.Pays.PaymentDt, a.Ordr.Pays.Bank, a.Ordr.Pays.DeliveryCost, a.Ordr.Pays.GoodsTotal, a.Ordr.Pays.CustomFee)
	if err != nil {
		fmt.Println(time.Now(), "Insert to Payment failed:", err)
	}
}

// insertOrder: Вставляет данные о заказе в таблицу orders
func insertOrder(a *All) {
	it := make([]int, len(a.Ordr.Items))
	for i := 0; i < len(a.Ordr.Items); i++ {
		it[i] = a.Ordr.Items[i].ChrtID
	}
	query := "INSERT INTO orders (OrderUID, TrackNumber, Entry, Deliveries, Pays, Items, Locale, InternalSignature, CustomerID, DeliveryService, Shardkey, SmID, DateCreated, OofShard)	Values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14) returning OrderUID"
	err := a.Pool.QueryRow(context.TODO(), query, a.Ordr.OrderUID, a.Ordr.TrackNumber, a.Ordr.Entry, a.Ordr.Deliveries.Name, a.Ordr.Pays.Transaction, it, a.Ordr.Locale, a.Ordr.InternalSignature, a.Ordr.CustomerID, a.Ordr.DeliveryService, a.Ordr.Shardkey, a.Ordr.SmID, a.Ordr.DateCreated, a.Ordr.OofShard)
	if err != nil {
		fmt.Println(time.Now(), "Insert to Order failed:", err)
	}
}

// insertItems: Вставляет данные о каждом элементе заказа в таблицу item
func insertItems(a *All) {
	for j := 0; j < len(a.Ordr.Items); j++ {
		query := "INSERT INTO item (ChrtID, TrackNumber, Price, Rid, Item_name, Sale, Size, TotalPrice, NmID, Brand, Status, orderid)	Values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) returning ChrtID"
		err := a.Pool.QueryRow(context.TODO(), query, a.Ordr.Items[j].ChrtID, a.Ordr.Items[j].TrackNumber, a.Ordr.Items[j].Price, a.Ordr.Items[j].Rid, a.Ordr.Items[j].Name, a.Ordr.Items[j].Sale, a.Ordr.Items[j].Size, a.Ordr.Items[j].TotalPrice, a.Ordr.Items[j].NmID, a.Ordr.Items[j].Brand, a.Ordr.Items[j].Status, a.Ordr.OrderUID)
		if err != nil {
			fmt.Println(time.Now(), "Insert to Items failed:", err)
		}
	}
}

// OrderHandler - обработчик http-запросов
func (a *All) OrderHandler(Writer http.ResponseWriter, Request *http.Request) {
	var err error
	switch Request.Method {
	case "GET":
		err = a.getHandler(Writer)
	case "POST":
		err = a.postHandler(Writer, Request)
	default:
		http.Error(Writer, "Invalid request method", 405)
		return
	}
	if err != nil {
		http.Error(Writer, err.Error(), 500)
	}
}

// getHandler - обработчик GET-запроса
func (a *All) getHandler(Writer http.ResponseWriter) error {
	tmpl, err := template.ParseFiles("client/index.html")
	if err != nil {
		return err
	}
	return tmpl.Execute(Writer, nil)
}

// postHandler - обработчик POST-запроса
func (a *All) postHandler(Writer http.ResponseWriter, Request *http.Request) error {
	Ouid := Request.PostFormValue("order_uid")
	Value, found := a.Cch.Get(Ouid)
	if !found {
		fmt.Fprintf(Writer, "Reading from DB:\n")
		a.Ordr.OrderUID = Ouid
		err := a.FromDbToCacheByKey()
		if err != nil {
			return err
		}
		Value, _ = a.Cch.Get(a.Ordr.OrderUID)
	} else {
		fmt.Fprintf(Writer, "Reading from Cache:\n")
	}
	JsonValue, err := json.MarshalIndent(Value, "", "\t")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(Writer, string(JsonValue))
	return err
}
