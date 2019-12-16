package worker

import (
	"encoding/json"
	"log"

	"github.com/gorilla/websocket"
)

const (
	APIKEY = "BL0CIIXASQCQGB8UU0NI"
)

// Processor for trades stream
type Processor struct {
	conn *websocket.Conn
}

func connect() *websocket.Conn {
	c, _, err := websocket.DefaultDialer.Dial("wss://stream.cryptowat.ch/connect?apikey="+APIKEY, nil)
	if err != nil {
		panic(err)
	}

	return c
	// remember to defer close this connection.
}

// NewProcessor func creates new processor var
func NewProcessor() *Processor {
	return &Processor{
		conn: connect(),
	}
}

func (p *Processor) authenticate() {
	// read first message from socker connection which will be a authentication message
	_, msg, err := p.conn.ReadMessage()
	if err != nil {
		panic(err)
	}

	//define auth result type that will hold authentication message
	var authResult struct {
		AuthenticationResult struct {
			Status string `json:"status"`
		} `json:"authenticationResult"`
	}

	err = json.Unmarshal(msg, &authResult)
	if err != nil {
		panic(err)
	}
	if authResult.AuthenticationResult.Status != "AUTHENTICATED" {
		panic(authResult.AuthenticationResult.Status)
	}
}

func (p *Processor) subscribe(resources []string) {
	// struct for subscription resource
	type StreamSubscription struct {
		Resource string `json:"resource"`
	}

	// struct for stream subscription
	type Subscription struct {
		StreamSubscription `json:"streamSubscription"`
	}

	// struct for subscription message, example here : https://docs.cryptowat.ch/websocket-api/data-subscriptions
	type SubscripeRequest struct {
		Subscriptions []Subscription `json:"subscriptions"`
	}

	// create json struct for subscription json body
	subMsg := struct {
		Subscribe SubscripeRequest `json:"subscribe"`
	}{}

	for _, resource := range resources {
		subMsg.Subscribe.Subscriptions = append(subMsg.Subscribe.Subscriptions, Subscription{StreamSubscription: StreamSubscription{Resource: resource}})
	}

	msg, err := json.Marshal(subMsg)
	err = p.conn.WriteMessage(websocket.TextMessage, msg)
	if err != nil {
		panic(err)
	}
}

func (p *Processor) handleMessages() {
	type Trade struct {
		Price     string `json:"priceStr"`
		Amount    string `json:"amountStr"`
		Timestamp int    `json:"timestamp,string"`
	}

	type Update struct {
		MarketUpdate struct {
			Market struct {
				MarketId int `json:"marketId,string"`
			} `json: "market"`
			TradesUpdate struct {
				Trades []Trade `json:"trades"`
			} `json:"tradesUpdate"`
		} `json:"marketUpdate"`
	}

	for {
		_, msg, err := p.conn.ReadMessage()
		if err != nil {
			log.Fatal("Error reading from connection", err)
			return
		}

		var update Update
		err = json.Unmarshal(msg, &update)
		if err != nil {
			panic(err)
		}

		for _, trade := range update.MarketUpdate.TradesUpdate.Trades {
			log.Printf(
				"BTC/USD trade on market %d: %s %s",
				update.MarketUpdate.Market.MarketId,
				trade.Price,
				trade.Amount,
			)
		}
	}

}

//Process read authentication result
func (p *Processor) Process() {
	defer p.conn.Close()

	p.authenticate()

	p.subscribe([]string{"markets:*:trades"})

	p.handleMessages()
}
