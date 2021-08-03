package mongodb

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Connection struct {
	Uri        string
	Database   string
	TimeOut    time.Duration
	Client     *mongo.Client
	Context    context.Context
	CancelFunc context.CancelFunc
}

func Close(connection *Connection) error {
	var err error
	defer connection.CancelFunc()
	defer func() {
		err = connection.Client.Disconnect(connection.Context)
	}()
	return err
}

func Connect(connection *Connection) (*Connection, error) {
	var err error
	if connection.TimeOut.String() == "0s" {
		connection.TimeOut = 30
	}
	connection.Context, connection.CancelFunc = context.WithTimeout(context.Background(),
		connection.TimeOut*time.Second)
	connection.Client, err = mongo.Connect(connection.Context, options.Client().ApplyURI(connection.Uri))
	return connection, err
}

func Insert(connection *Connection, col string, doc interface{}) (*mongo.InsertOneResult, error) {

	collection := connection.Client.Database(connection.Database).Collection(col)
	return collection.InsertOne(connection.Context, doc)
}

func InsertMany(connection *Connection, col string, docs []interface{}) (*mongo.InsertManyResult, error) {

	collection := connection.Client.Database(connection.Database).Collection(col)
	return collection.InsertMany(connection.Context, docs)
}

func Query(connection *Connection, col string, query, field interface{}) (result []bson.D, err error) {

	collection := connection.Client.Database(connection.Database).Collection(col)
	cursor, err := collection.Find(connection.Context, query, options.Find().SetProjection(field))
	if err != nil {
		return []bson.D{}, err
	}
	var results []bson.D
	err = cursor.All(connection.Context, &results)
	if err != nil {
		return []bson.D{}, err
	}
	return results, nil
}

func Update(connection *Connection, col string, filter, update interface{}) (result *mongo.UpdateResult, err error) {

	collection := connection.Client.Database(connection.Database).Collection(col)
	return collection.UpdateOne(connection.Context, filter, update)
}

func UpdateMany(connection *Connection, col string, filter, update interface{}) (result *mongo.UpdateResult, err error) {

	collection := connection.Client.Database(connection.Database).Collection(col)
	return collection.UpdateMany(connection.Context, filter, update)
}

func Delete(connection *Connection, col string, query interface{}) (result *mongo.DeleteResult, err error) {

	collection := connection.Client.Database(connection.Database).Collection(col)
	return collection.DeleteOne(connection.Context, query)
}

func DeleteMany(connection *Connection, col string, query interface{}) (result *mongo.DeleteResult, err error) {

	collection := connection.Client.Database(connection.Database).Collection(col)
	return collection.DeleteMany(connection.Context, query)
}

func Ping(connection *Connection) error {

	if err := connection.Client.Ping(connection.Context, readpref.Primary()); err != nil {
		return err
	}
	fmt.Println("connected successfully")
	return nil
}
