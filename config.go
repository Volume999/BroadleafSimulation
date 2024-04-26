package simulation

import "math/rand"

const (
	MaxOrderItems    = 20
	MaxAppliedOffers = 10
	MaxPayments      = 7
)

type Config struct {
	OrderItemsCnt    int
	SKUItemsCnt      int
	AppliedOffersCnt int
	PaymentsCnt      int
}

func NewConfig(orderItemsCnt, skuItemsCnt, appliedOffersCnt, paymentsCnt int) *Config {
	return &Config{
		OrderItemsCnt:    orderItemsCnt,
		SKUItemsCnt:      skuItemsCnt,
		AppliedOffersCnt: appliedOffersCnt,
		PaymentsCnt:      paymentsCnt,
	}
}

func RandomConfig() *Config {
	return &Config{
		OrderItemsCnt:    rand.Intn(MaxOrderItems) + 1,
		SKUItemsCnt:      rand.Intn(MaxOrderItems) + 1,
		AppliedOffersCnt: rand.Intn(MaxAppliedOffers) + 1,
		PaymentsCnt:      rand.Intn(MaxPayments) + 1,
	}
}
