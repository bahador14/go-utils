package mongodb

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.mongodb.org/mongo-driver/bson"
	"testing"
)

type MongoDBTestSuite struct {
	suite.Suite
	Connection *Connection
}

var TestCollection string = "test_collection"

func (suite *MongoDBTestSuite) SetupSuite() {
	var err error
	suite.Connection, err = Connect(&Connection{
		Uri:      "mongodb://localhost:27017",
		Database: "test_database",
	})
	assert.Nil(suite.T(), err)
}

func (suite *MongoDBTestSuite) Test_1_Ping() {
	err := Ping(suite.Connection)
	assert.Nil(suite.T(), err)
}

func (suite *MongoDBTestSuite) Test_2_Insert() {
	var document interface{}
	document = bson.D{
		{"rollNo", 175},
		{"maths", 80},
		{"science", 90},
		{"computer", 95},
	}
	insertOneResult, err := Insert(suite.Connection, TestCollection, document)
	assert.Nil(suite.T(), err)
	assert.NotEmpty(suite.T(), insertOneResult.InsertedID)
}

func (suite *MongoDBTestSuite) Test_3_InsertMany() {
	var documents []interface{}
	documents = []interface{}{
		bson.D{
			{"rollNo", 153},
			{"maths", 65},
			{"science", 59},
			{"computer", 55},
		},
		bson.D{
			{"rollNo", 162},
			{"maths", 86},
			{"science", 80},
			{"computer", 69},
		},
	}
	insertManyResult, err := InsertMany(suite.Connection, TestCollection, documents)
	assert.Nil(suite.T(), err)
	assert.NotEmpty(suite.T(), insertManyResult.InsertedIDs)
}

func (suite *MongoDBTestSuite) Test_4_Query() {
	var filter, option interface{}
	filter = bson.D{
		{"maths", bson.D{{"$gt", 70}}},
	}
	option = bson.D{{"_id", 0}}
	results, err := Query(suite.Connection,
		TestCollection, filter, option,
	)
	assert.Nil(suite.T(), err)
	assert.NotEmpty(suite.T(), results)

}

func (suite *MongoDBTestSuite) Test_5_Update() {
	filter := bson.D{
		{"maths", bson.D{{"$lt", 100}}},
	}
	update := bson.D{
		{"$set", bson.D{
			{"maths", 100},
		}},
	}
	result, err := Update(suite.Connection, TestCollection, filter, update)
	assert.Nil(suite.T(), err)
	assert.NotEmpty(suite.T(), result.MatchedCount)
	assert.NotEmpty(suite.T(), result.ModifiedCount)

}

func (suite *MongoDBTestSuite) Test_6_UpdateMany() {
	filter := bson.D{
		{"computer", bson.D{{"$lt", 100}}},
	}
	update := bson.D{
		{"$set", bson.D{
			{"computer", 100},
		}},
	}
	result, err := UpdateMany(suite.Connection, TestCollection, filter, update)
	assert.Nil(suite.T(), err)
	assert.NotEmpty(suite.T(), result.MatchedCount)
	assert.NotEmpty(suite.T(), result.ModifiedCount)
}

func (suite *MongoDBTestSuite) Test_7_Delete() {
	query := bson.D{
		{"maths", bson.D{{"$gt", 60}}},
	}
	result, err := Delete(suite.Connection, TestCollection, query)
	assert.Nil(suite.T(), err)
	assert.NotEmpty(suite.T(), result.DeletedCount)
}

func (suite *MongoDBTestSuite) Test_8_DeleteMany() {
	query := bson.D{
		{"science", bson.D{{"$gt", 0}}},
	}
	result, err := DeleteMany(suite.Connection, TestCollection, query)
	assert.Nil(suite.T(), err)
	assert.NotEmpty(suite.T(), result.DeletedCount)
}

func (suite *MongoDBTestSuite) TearDownSuite() {
	err := Close(suite.Connection)
	assert.Nil(suite.T(), err)
}

func TestSuggestionServiceTestSuite(t *testing.T) {
	suite.Run(t, new(MongoDBTestSuite))
}
