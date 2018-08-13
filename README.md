# Shardb v 0.1

Disclaimer: I created this code during my time in college for the purpose of extending my knowledge in databases. It is not intended to be used in a production environment.

![alt text](https://github.com/Kisulken/shardb/blob/master/description.jpg?raw=true)

Search can be performed based on the primary keys for every type of structure.
There are two types of primary keys: unique and not.

<b>Unique</b> key must be unique across the collection.

<b>NOT unique</b> key may have duplicates.

Example of use
```Go
import "shardb"
...
database = db.NewDatabase("test")
err := database.ScanAndLoadData("")
if err != nil {
    fmt.Println(err)
}
if database.GetCollectionsCount() <= 0 {
    c, _ := database.AddCollection("some_collection")
    p := Person("Login", "Name", 20)
    err = c.Write(&p)
    if err != nil {
        panic(err)
    }
    _, err = c.Delete(&p)
    if err != nil {
        panic(err)
    }
    database.Optimize()
    database.Sync()
}
```

Every structure that you are to write in the database must obey the interface "CustomStructure"
and be registered in the system.
```Go
type Person struct {
    Login string // primary unique key
    Name  string
    Age   int    // primary key
}

func (c *Person) GetDataIndex() []*db.FullDataIndex {
    return []*db.FullDataIndex{
        {"Login", c.Login, true},
        {"Age", strconv.Itoa(c.Age), false},
    }
}

func InitCustomTypes(db *db.Database) {
    db.RegisterType(&Person{})
}
```
More detailed example can be found in <i>examples/general_example.go</i>
