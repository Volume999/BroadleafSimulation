package simulator

import "math/rand"

const (
	MaxOrderItems    = 20
	MaxAppliedOffers = 10
	MaxPayments      = 7
	DefaultKeys      = 10000
)

type Config struct {
	OrderItemsCnt    int
	SKUItemsCnt      int
	AppliedOffersCnt int
	PaymentsCnt      int
	keys             TableAccessKeys
}

type TableAccessKeys struct {
	Orders              []int
	Items               []int
	StockKeepingUnits   []int
	Customers           []int
	ItemOffers          []int
	OrderPayments       []int
	ItemOptions         []int
	CustomerOffersUsage []int
	OrderTaxes          []int
}

func NewConfig(orderItemsCnt, skuItemsCnt, appliedOffersCnt, paymentsCnt int) *Config {
	return &Config{
		OrderItemsCnt:    orderItemsCnt,
		SKUItemsCnt:      skuItemsCnt,
		AppliedOffersCnt: appliedOffersCnt,
		PaymentsCnt:      paymentsCnt,
	}
}

func randomKeys(n int, keys int) []int {
	accessKeys := make([]int, n)
	for i := range n {
		accessKeys[i] = rand.Intn(keys) + 1
	}
	return accessKeys
}

func RandomConfig() *Config {
	return &Config{
		OrderItemsCnt:    rand.Intn(MaxOrderItems) + 1,
		SKUItemsCnt:      rand.Intn(MaxOrderItems) + 1,
		AppliedOffersCnt: rand.Intn(MaxAppliedOffers) + 1,
		PaymentsCnt:      rand.Intn(MaxPayments) + 1,
	}
}

func (c *Config) SetAccessKeys(keys int) {
	if keys < 1 {
		keys = DefaultKeys
	}
	accessKeys := TableAccessKeys{
		Orders:              randomKeys(1, keys),
		Items:               randomKeys(c.OrderItemsCnt, keys),
		StockKeepingUnits:   randomKeys(c.OrderItemsCnt, keys),
		Customers:           randomKeys(1, keys),
		ItemOffers:          randomKeys(c.OrderItemsCnt, keys),
		OrderPayments:       randomKeys(c.PaymentsCnt, keys),
		ItemOptions:         randomKeys(c.OrderItemsCnt, keys),
		CustomerOffersUsage: randomKeys(c.OrderItemsCnt, keys),
		OrderTaxes:          randomKeys(1, keys),
	}
	c.keys = accessKeys
}
