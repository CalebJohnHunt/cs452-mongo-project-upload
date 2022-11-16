package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Connection URI

func main() {
	uri := os.Args[1]
	// Create a new client and connect to the server
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
	if err != nil {
		panic(err)
	}
	defer func() {
		if err = client.Disconnect(context.TODO()); err != nil {
			panic(err)
		}
	}()

	db := client.Database("mongo_project")

	// state_pops(db)
	db.Collection("pop").InsertMany(context.TODO(), foo("state_pops.csv", []string{"SUMLEV", "REGION", "DIVISION", "STATE_NUM", "state", "population", "POPEST18PLUS2019", "PCNT_POPEST18PLUS"}))
	fmt.Println("uploaded pop")
	// us_state_vaccinations(db)
	db.Collection("vax").InsertMany(context.TODO(), foo("us_state_vaccinations.csv", []string{"date", "location", "total_vaccinations", "total_distributed", "people_vaccinated", "people_fully_vaccinated_per_hundred", "total_vaccinations_per_hundred", "people_fully_vaccinated", "people_vaccinated_per_hundred", "distributed_per_hundred", "daily_vaccinations_raw", "daily_vaccinations", "daily_vaccinations_per_million", "share_doses_used", "total_boosters", "total_boosters_per_hundred"}))
	fmt.Println("uploaded vax")
	// us_counties(db)
	db.Collection("cases").InsertMany(context.TODO(), foo("us-counties.csv", []string{"date", "county", "state", "fips", "cases", "deaths"}))
	fmt.Println("uploaded cases")
}

func foo(filename string, headers []string) []interface{} {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	csvr := csv.NewReader(bufio.NewReader(f))

	_, err = csvr.Read() // remove headers
	if err != nil {
		panic(err)
	}

	stuff := []interface{}{}

	for {
		record, err := csvr.Read()
		if err == io.EOF {
			return stuff
		}

		if len(headers) != len(record) {
			panic(fmt.Sprintf("Weirdness: %d %d\n", len(headers), len(record)))
		}

		thing := map[string]interface{}{}

		for i, header := range headers {
			n, err := strconv.Atoi(record[i])
			if err == nil {
				thing[header] = n
				continue
			}
			nf, err := strconv.ParseFloat(record[i], 64)
			if err == nil {
				thing[header] = nf
				continue
			}
			thing[header] = record[i]
		}
		stuff = append(stuff, thing)
	}
}
