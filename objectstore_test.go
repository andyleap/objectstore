package objectstore

import (
	"fmt"
	"os"
	"testing"

	"github.com/boltdb/bolt"
)

type Person struct {
	Object

	Name string `objectstore:"unique"`
	Age  int    ``
}

func (p Person) String() string {
	return fmt.Sprintf("%s, age %d", p.Name, p.Age)
}

func init() {
	os.Remove("test.db")
}

func TestBasic(t *testing.T) {
	db, _ := bolt.Open("test.db", 0777, nil)
	os := New(db, nil)
	os.Register(Person{})

	Bob := Person{
		Name: "Bob",
		Age:  25,
	}
	os.Save(&Bob)

	Joe := Person{
		Name: "Joe",
		Age:  26,
	}
	os.Save(&Joe)

	Sally := Person{
		Name: "Sally",
		Age:  25,
	}
	os.Save(&Sally)

	People := []Person{}
	os.GetByField("Age", 25, &People)

	fmt.Println(People) // prints "[Bob, age 25 Sally, age 25]"

	var person Person
	os.GetFirstByField("Name", "Bob", &person)

	fmt.Println(person) // prints "[Bob, age 25]"
	os.Delete(&person)

	People = []Person{}
	os.GetAll(&People)

	fmt.Println(People)

	os.GetFirstByField("Age", 26, &person)
	person.Name = "Bill"
	os.Save(&person)

	People = []Person{}
	os.GetAll(&People)

	fmt.Println(People)

}
