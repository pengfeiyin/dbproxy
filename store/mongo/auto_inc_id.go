package mongo

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const AutoIncIDName = "auto_inc_id"

//AutoIncID struct
type AutoIncID struct {
	ID  string `bson:"_id" json:"id"`
	Num int64  `bson:"n" json:"n"`
}

// GetIncID 获取
func GetIncID(ctx context.Context, db *mongo.Database, id string) (int64, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	res := db.Collection(AutoIncIDName).
		FindOneAndUpdate(
			ctx,
			bson.M{"_id": id},
			bson.M{"$inc": bson.M{"n": int64(1)}},
			options.
				FindOneAndUpdate().
				SetUpsert(true).
				SetReturnDocument(options.After),
		)
	if res.Err() != nil {
		return 0, res.Err()
	}

	var incID = new(AutoIncID)
	if err := res.Decode(&incID); err != nil {
		return 0, err
	} else if incID.Num == 0 {
		return 0, errors.New("invalid inc id")
	}

	return incID.Num, nil
}
