package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Item struct {
	ID                         string   `json:"id" bson:"_id"`
	SSSSSSSSSSSSSSSS           string   `json:"ssssssssssssssss" bson:"ssssssssssssssss"`
	IIIIIIIIIIIIIIII1111111111 bool     `json:"iiiiiiiiiiiiiiii1111111111" bson:"iiiiiiiiiiiiiiii1111111111"`
	DDDDDDDDDDDDDDDDDDDDDD     []string `json:"dddddddddddddddddddddd,omitempty" bson:"dddddddddddddddddddddd,omitempty"`
	PPPPPPPPPPPPPPPP           []string `json:"pppppppppppppppp,omitempty" bson:"pppppppppppppppp,omitempty"`
	WWWWWWWWWWWWWWWWWWWW       []string `json:"wwwwwwwwwwwwwwwwwwww,omitempty" bson:"-"`
	SSSSSSSSSSSSSSSSSSss       string   `json:"ssssssssssssssssssSS,omitempty" bson:"ssssssssssssssssssSS,omitempty"`
	XXXXXXXXXXXXXXX            string   `json:"xxxxxxxxxxxxxxx,omitempty" bson:"xxxxxxxxxxxxxxx,omitempty"`
	TTT                        string   `json:"ttt,omitempty" bson:"ttt,omitempty"`
	CCC                        []string `json:"ccc,omitempty" bson:"ccc,omitempty"`
	BBB                        []string `json:"bbb,omitempty" bson:"bbb,omitempty"`
	AAA                        []string `json:"aaa,omitempty" bson:"aaa,omitempty"`
	GGG                        []string `json:"ggg,omitempty" bson:"ggg,omitempty"`
	DDD                        []string `json:"ddd,omitempty" bson:"ddd,omitempty"`
}

func main() {
	const collectionName = "mycollection"
	dbname := uuid.New().String()
	client, db, err := Connect("mongodb://localhost:27017", dbname)

	if err != nil {
		log.Fatalln(err)
	}

	defer DropDB(db)

	if err := CreateCollection(db, collectionName); err != nil {
		log.Fatalln(err)
	}

	const maxDocs = 500000
	docs := make([]interface{}, 0, maxDocs)
	for i := 0; i < maxDocs; i++ {
		doc := &Item{
			ID: uuid.New().String(),
		}
		docs = append(docs, doc)
	}

	count := 0
	_, err = WithTransaction(client, func(ctx mongo.SessionContext) (interface{}, error) {
		count++
		fmt.Printf("Hello from transaction! count - %d\n", count)
		return db.Collection(collectionName).InsertMany(ctx, docs)
	})

	if count > 1 {
		fmt.Println("Very strange... why the transaction has been called more than once (((: What wrong with it?")
	}

	if err != nil {
		log.Fatalln(err)
	}
}

func WithTransaction(client *mongo.Client, callback func(ctx mongo.SessionContext) (interface{}, error)) (interface{}, error) {
	ctx, cancel := DefaultContext()
	defer cancel()

	s, err := client.StartSession()
	if err != nil {
		return nil, err
	}

	defer s.EndSession(ctx)

	opts := options.Transaction().SetReadConcern(readconcern.Snapshot())
	return s.WithTransaction(ctx, callback, opts)
}

func CreateCollection(db *mongo.Database, name string) error {
	cmd := bson.D{bson.E{Key: "create", Value: name}}
	ctx, cancel := DefaultContext()
	defer cancel()
	return db.RunCommand(ctx, cmd).Err()
}

func Connect(url string, dbname string) (client *mongo.Client, db *mongo.Database, err error) {
	ctx, cancel := DefaultContext()
	defer cancel()

	if client, err = mongo.Connect(ctx, nil); err != nil {
		return
	}

	db = client.Database(dbname)
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		return
	}

	return
}

func DefaultContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 10*time.Minute)
}

func DropDB(db *mongo.Database) {
	ctx, cancel := DefaultContext()
	defer cancel()
	db.Drop(ctx)
}
