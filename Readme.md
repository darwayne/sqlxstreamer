# sqlxstreamer

sqlxstreamer is library that allows efficient streaming of queries that can return large results.
Results are streamed via a cursor which is more efficient than paging results via limit offset.

sqlsxstreamer is built on top of [sqlx](https://github.com/jmoiron/sqlx)


### Example Usage
```go
type User struct {
	ID string      `db:"id"`
	Name string    `db:"name"`
}

db, _ := sqlx.Connect("postgres", "user=example dbname=example password=example")

err := sqxlstreamer.New().
	BatchSize(100).
	Query("SELECT * FROM users WHERE is_using_postgres = ? AND is_using_sqlx = ?").
	Args(true, true).
	EachBatch(func(retrieve sqlxstreamer.Callback){
		var users []User
		retrieve(&users, func(){
			fmt.Printf("Batch returned %d users \n", len(users))
		})
	})
```
  - If you had 1,000 users in your database `EachBatch` would get called 10 times, since the batch size is 100