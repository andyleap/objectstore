// objectstore project objectstore.go
package objectstore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"

	"github.com/boltdb/bolt"
)

type ObjectStore struct {
	db         *bolt.DB
	bucketPath [][]byte
	types      map[reflect.Type]*TypeData
	typeLock   sync.Mutex
}

type Index struct {
	Name   []byte
	Field  []int
	Unique bool
}

type TypeData struct {
	Name         []byte
	PrimaryIndex *Index
	Indexes      []*Index
	IndexNames   map[string]*Index
	Items        map[string]Object
}

type Object struct {
	indices map[string][]byte
}

func New(db *bolt.DB, bucketPath [][]byte) *ObjectStore {
	return &ObjectStore{
		db:         db,
		bucketPath: bucketPath,
		types:      make(map[reflect.Type]*TypeData),
	}
}

func (os *ObjectStore) Register(o interface{}) {
	if os.types[reflect.TypeOf(o)] != nil {
		return
	}
	val := reflect.TypeOf(o)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	td := &TypeData{
		IndexNames: make(map[string]*Index),
	}
	td.Name = []byte(val.Name())

	for l1 := 0; l1 < val.NumField(); l1++ {
		field := val.Field(l1)
		if field.Name == "Object" {
			continue
		}
		Tag := field.Tag.Get("objectstore")
		subTags := strings.Split(Tag, ",")
		TagData := make(map[string]string)
		for _, subTag := range subTags {
			kv := strings.Split(subTag, ":")
			if len(kv) == 1 {
				TagData[kv[0]] = "true"
			} else {
				TagData[kv[0]] = kv[1]
			}
		}

		name := TagData["index"]
		unique := TagData["unique"] == "true"
		if name == "" {
			name = field.Name
		}

		if name != "-" {
			index := &Index{
				Name:   []byte(field.Name),
				Field:  []int{l1},
				Unique: unique,
			}
			td.Indexes = append(td.Indexes, index)
			td.IndexNames[field.Name] = index
		}
	}
	os.types[reflect.TypeOf(o)] = td
}

func (os *ObjectStore) Save(val interface{}) error {
	value := reflect.ValueOf(val)
	if value.Kind() != reflect.Ptr {
		return fmt.Errorf("os.Save requires pointer to object to save")
	}
	value = value.Elem()
	o := value.FieldByName("Object").Interface().(Object)
	if o.indices == nil {
		o.indices = make(map[string][]byte)
		value.FieldByName("Object").Set(reflect.ValueOf(o))
	}

	typeData := os.types[value.Type()]
	err := os.db.Update(func(tx *bolt.Tx) error {
		bpath := append(os.bucketPath, typeData.Name)
		bucket, _ := tx.CreateBucketIfNotExists(bpath[0])
		for _, path := range bpath[1:] {
			bucket, _ = bucket.CreateBucketIfNotExists(path)
		}
		data, _ := json.Marshal(val)
		if typeData.PrimaryIndex == nil {
			pribucket, _ := bucket.CreateBucketIfNotExists([]byte("primary"))
			if o.indices["primary"] == nil {
				id, _ := pribucket.NextSequence()
				o.indices["primary"], _ = json.Marshal(id)
			}
			pribucket.Put(o.indices["primary"], data)
		} else {
			priID := value.FieldByIndex(typeData.PrimaryIndex.Field).Interface()
			newPriID, _ := json.Marshal(priID)
			if !bytes.Equal(newPriID, o.indices["primary"]) {
				//os.Delete()
			}
			o.indices["primary"] = newPriID
			pribucket, _ := bucket.CreateBucketIfNotExists(typeData.PrimaryIndex.Name)
			if pribucket.Get(o.indices["primary"]) != nil {
				return fmt.Errorf("Primary Key was not unique")
			}
			pribucket.Put(o.indices["primary"], data)
		}
		for _, index := range typeData.Indexes {
			indexVal := value.FieldByIndex(index.Field).Interface()
			indexRaw, _ := json.Marshal(indexVal)

			indexBucket, _ := bucket.CreateBucketIfNotExists(index.Name)

			if !bytes.Equal(indexRaw, o.indices[string(index.Name)]) {
				if index.Unique {
					preVal := indexBucket.Get(o.indices[string(index.Name)])
					if preVal != nil && bytes.Equal(preVal, o.indices["primary"]) {
						indexBucket.Delete(o.indices[string(index.Name)])
					}
				} else {
					curIndex := indexBucket.Get(o.indices[string(index.Name)])
					curIndexes := [][]byte{}
					json.Unmarshal(curIndex, &curIndexes)
					found := false
					for k, curIndexesItem := range curIndexes {
						if bytes.Equal(curIndexesItem, o.indices["primary"]) {
							curIndexes = append(curIndexes[:k], curIndexes[k+1:]...)
							found = true
						}
					}
					if found {
						curIndex, _ = json.Marshal(curIndexes)
						indexBucket.Put(o.indices[string(index.Name)], curIndex)
					}
				}
			}

			if index.Unique {
				preVal := indexBucket.Get(indexRaw)
				if preVal != nil && !bytes.Equal(preVal, o.indices["primary"]) {
					return fmt.Errorf("Primary Key was not unique")
				}
				indexBucket.Put(indexRaw, o.indices["primary"])
			} else {
				curIndex := indexBucket.Get(indexRaw)
				curIndexes := [][]byte{}
				json.Unmarshal(curIndex, &curIndexes)
				found := false
				for _, curIndexesItem := range curIndexes {
					if bytes.Equal(curIndexesItem, o.indices["primary"]) {
						found = true
					}
				}
				if !found {
					curIndexes = append(curIndexes, o.indices["primary"])
				}
				curIndex, _ = json.Marshal(curIndexes)
				indexBucket.Put(indexRaw, curIndex)
			}
			o.indices[string(index.Name)] = indexRaw
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (os *ObjectStore) Delete(val interface{}) error {
	value := reflect.ValueOf(val)
	if value.Kind() == reflect.Ptr {
		value = value.Elem()
	}
	o := value.FieldByName("Object").Interface().(Object)
	typeData := os.types[value.Type()]
	err := os.db.Update(func(tx *bolt.Tx) error {
		bpath := append(os.bucketPath, typeData.Name)
		bucket, _ := tx.CreateBucketIfNotExists(bpath[0])
		for _, path := range bpath[1:] {
			bucket, _ = bucket.CreateBucketIfNotExists(path)
		}
		if typeData.PrimaryIndex == nil {
			pribucket, _ := bucket.CreateBucketIfNotExists([]byte("primary"))
			if o.indices["primary"] == nil {
				id, _ := pribucket.NextSequence()
				o.indices["primary"], _ = json.Marshal(id)
			}
			pribucket.Delete(o.indices["primary"])
		} else {
			priID := value.FieldByIndex(typeData.PrimaryIndex.Field).Interface()
			newPriID, _ := json.Marshal(priID)
			if !bytes.Equal(newPriID, o.indices["primary"]) {
				//os.Delete()
			}
			o.indices["primary"] = newPriID
			pribucket, _ := bucket.CreateBucketIfNotExists(typeData.PrimaryIndex.Name)
			pribucket.Delete(o.indices["primary"])
		}
		for _, index := range typeData.Indexes {
			indexBucket, _ := bucket.CreateBucketIfNotExists(index.Name)

			if index.Unique {
				preVal := indexBucket.Get(o.indices[string(index.Name)])
				if preVal != nil && bytes.Equal(preVal, o.indices["primary"]) {
					indexBucket.Delete(o.indices[string(index.Name)])
				}
			} else {
				curIndex := indexBucket.Get(o.indices[string(index.Name)])
				curIndexes := [][]byte{}
				json.Unmarshal(curIndex, &curIndexes)
				found := false
				for k, curIndexesItem := range curIndexes {
					if bytes.Equal(curIndexesItem, o.indices["primary"]) {
						curIndexes = append(curIndexes[:k], curIndexes[k+1:]...)
						found = true
					}
				}
				if found {
					curIndex, _ = json.Marshal(curIndexes)
					indexBucket.Put(o.indices[string(index.Name)], curIndex)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (os *ObjectStore) GetAll(ret interface{}) error {
	err := os.db.View(func(tx *bolt.Tx) error {
		retVal := reflect.ValueOf(ret).Elem()

		if retVal.Kind() == reflect.Slice {
			retValType := retVal.Type().Elem()
			if retValType.Kind() == reflect.Ptr {
				retValType = retValType.Elem()
			}
			typeData := os.types[retValType]
			bpath := append(os.bucketPath, typeData.Name)
			bucket := tx.Bucket(bpath[0])
			for _, path := range bpath[1:] {
				bucket = bucket.Bucket(path)
			}
			retVal.SetLen(0)
			var pribucket *bolt.Bucket
			if typeData.PrimaryIndex == nil {
				pribucket = bucket.Bucket([]byte("primary"))
			} else {
				pribucket = bucket.Bucket(typeData.PrimaryIndex.Name)
			}
			c := pribucket.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				valType := retVal.Type().Elem()
				pointer := false
				if valType.Kind() == reflect.Ptr {
					valType = valType.Elem()
					pointer = true
				}
				val := reflect.New(valType)
				json.Unmarshal(v, val.Interface())
				value := val
				if value.Kind() == reflect.Ptr {
					value = value.Elem()
				}
				o := Object{}
				o.indices = make(map[string][]byte)
				value.FieldByName("Object").Set(reflect.ValueOf(o))
				o.indices["primary"] = k
				for _, index := range typeData.Indexes {
					indexVal := value.FieldByIndex(index.Field).Interface()
					indexRaw, _ := json.Marshal(indexVal)
					o.indices[string(index.Name)] = indexRaw
				}
				if pointer {
					retVal = reflect.Append(retVal, val)
				} else {
					retVal = reflect.Append(retVal, val.Elem())
				}
			}
			reflect.ValueOf(ret).Elem().Set(reflect.Indirect(retVal))
		}

		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (os *ObjectStore) GetByField(field string, val interface{}, ret interface{}) error {
	err := os.db.View(func(tx *bolt.Tx) error {
		retVal := reflect.ValueOf(ret).Elem()

		if retVal.Kind() == reflect.Slice {
			retValType := retVal.Type().Elem()
			if retValType.Kind() == reflect.Ptr {
				retValType = retValType.Elem()
			}
			typeData := os.types[retValType]
			index, ok := typeData.IndexNames[field]
			if !ok {
				return fmt.Errorf("No such index: %s", field)
			}
			bpath := append(os.bucketPath, typeData.Name)
			bucket := tx.Bucket(bpath[0])
			for _, path := range bpath[1:] {
				bucket = bucket.Bucket(path)
			}
			indexBucket := bucket.Bucket(index.Name)
			indexRaw, _ := json.Marshal(val)
			priIDs := [][]byte{}
			if index.Unique {
				priID := indexBucket.Get(indexRaw)
				priIDs = append(priIDs, priID)
			} else {
				json.Unmarshal(indexBucket.Get(indexRaw), &priIDs)
			}
			retVal.SetLen(0)
			for _, priID := range priIDs {
				valType := retVal.Type().Elem()
				pointer := false
				if valType.Kind() == reflect.Ptr {
					valType = valType.Elem()
					pointer = true
				}
				val := reflect.New(valType)
				var rawData []byte
				if typeData.PrimaryIndex == nil {
					pribucket := bucket.Bucket([]byte("primary"))
					rawData = pribucket.Get(priID)
				} else {
					pribucket := bucket.Bucket(typeData.PrimaryIndex.Name)
					rawData = pribucket.Get(priID)
				}
				json.Unmarshal(rawData, val.Interface())
				value := val
				if value.Kind() == reflect.Ptr {
					value = value.Elem()
				}
				o := Object{}
				o.indices = make(map[string][]byte)
				value.FieldByName("Object").Set(reflect.ValueOf(o))
				o.indices["primary"] = priID
				for _, index := range typeData.Indexes {
					indexVal := value.FieldByIndex(index.Field).Interface()
					indexRaw, _ := json.Marshal(indexVal)
					o.indices[string(index.Name)] = indexRaw
				}
				if pointer {
					retVal = reflect.Append(retVal, val)
				} else {
					retVal = reflect.Append(retVal, val.Elem())
				}
			}
			reflect.ValueOf(ret).Elem().Set(reflect.Indirect(retVal))
		}

		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

func (os *ObjectStore) GetFirstByField(field string, val interface{}, ret interface{}) error {
	err := os.db.View(func(tx *bolt.Tx) error {
		retVal := reflect.ValueOf(ret)
		if retVal.Kind() == reflect.Ptr {
			retValType := retVal.Type().Elem()
			if retValType.Kind() == reflect.Ptr {
				retValType = retValType.Elem()
			}
			typeData := os.types[retValType]
			index, ok := typeData.IndexNames[field]
			if !ok {
				return fmt.Errorf("No such index: %s", field)
			}
			bpath := append(os.bucketPath, typeData.Name)
			bucket := tx.Bucket(bpath[0])
			for _, path := range bpath[1:] {
				bucket = bucket.Bucket(path)
			}
			indexBucket := bucket.Bucket(index.Name)
			indexRaw, _ := json.Marshal(val)
			priIDs := [][]byte{}
			if index.Unique {
				priID := indexBucket.Get(indexRaw)
				priIDs = append(priIDs, priID)
			} else {
				json.Unmarshal(indexBucket.Get(indexRaw), &priIDs)
			}
			priID := priIDs[0]
			var rawData []byte
			if typeData.PrimaryIndex == nil {
				pribucket := bucket.Bucket([]byte("primary"))
				rawData = pribucket.Get(priID)
			} else {
				pribucket := bucket.Bucket(typeData.PrimaryIndex.Name)
				rawData = pribucket.Get(priID)
			}
			json.Unmarshal(rawData, retVal.Interface())
			value := retVal
			if value.Kind() == reflect.Ptr {
				value = value.Elem()
			}
			o := Object{}
			o.indices = make(map[string][]byte)
			o.indices["primary"] = priID
			value.FieldByName("Object").Set(reflect.ValueOf(o))
			for _, index := range typeData.Indexes {
				indexVal := value.FieldByIndex(index.Field).Interface()
				indexRaw, _ := json.Marshal(indexVal)
				o.indices[string(index.Name)] = indexRaw
			}
			reflect.ValueOf(ret).Elem().Set(reflect.Indirect(retVal))
		}

		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
