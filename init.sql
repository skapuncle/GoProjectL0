CREATE TABLE delivery
(
    del_id      uuid primary key default gen_random_uuid(),
    del_name    VARCHAR(50),
    Phone   VARCHAR (50),
    Zip     VARCHAR (50),
    City    VARCHAR (50),
    Address VARCHAR (50),
    Region  VARCHAR (50),
    Email   VARCHAR (50)
);

CREATE TABLE payment
(
    pay_id uuid primary key default gen_random_uuid(),
    Transaction VARCHAR (50),
    RequestID VARCHAR (50),
    Currency VARCHAR (50),
    Provider VARCHAR (50),
    Amount bigint,
    PaymentDt bigint,
    Bank VARCHAR (50),
    DeliveryCost bigint,
    GoodsTotal bigint,
    CustomFee bigint
);

CREATE TABLE  item
(
    ChrtID BIGINT NOT NULL primary key,
    TrackNumber VARCHAR (50),
    Price BIGINT,
    Rid VARCHAR (50),
    Item_name VARCHAR (50),
    Sale bigint,
    Size VARCHAR (50),
    TotalPrice bigint,
    NmID bigint,
    Brand VARCHAR (50),
    Status bigint,
    orderid VARCHAR (50)
);


CREATE TABLE orders
(
    OrderUID VARCHAR(50) not null PRIMARY KEY ,
    TrackNumber varchar(50),
    Entry varchar(50),
    Deliveries uuid,
    FOREIGN KEY (deliveries)
        references DELIVERY (del_id),
    Pays uuid,
    FOREIGN KEY (Pays)
        references Payment (pay_id),
    Items bigint[],
    Locale varchar(50),
    InternalSignature varchar(50),
    CustomerID varchar(50),
    DeliveryService varchar(50),
    Shardkey varchar(50),
    SmID bigint,
    DateCreated timestamp,
    OofShard varchar(50)
);