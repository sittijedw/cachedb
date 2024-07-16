package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

var dbConnection *sql.DB
var redisClient *redis.Client

type Product struct {
	ProductID   int     `json:"productid"`
	ProductName string  `json:"productName"`
	RetailPrice float64 `json:"retailPrice"`
}

type Response struct {
	Products []Product `json:"products"`
	Source   string    `json:"source"`
}

func main() {
	redisClient = redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
	})

	dbConnection = connectDB()
	defer dbConnection.Close()

	r := gin.Default()
	r.GET("/products", getProductsHandler)

	srv := &http.Server{
		Addr:    ":" + os.Getenv("PORT"),
		Handler: r,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go gracefully(ctx, srv)

	if err := srv.ListenAndServe(); err != nil {
		if !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	}

	fmt.Println("\nbye")
}

func gracefully(ctx context.Context, srv *http.Server) {
	<-ctx.Done()
	{
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			log.Fatal(err)
		}
	}
}

func connectDB() *sql.DB {
	url := os.Getenv("DATABASE_URL")
	db, err := sql.Open("postgres", url)

	if err != nil {
		log.Println("Connect to database error", err)
	}

	return db
}

func getProductsFromDB() ([]Product, error) {
	rows, err := dbConnection.Query("SELECT product_id, product_name, retail_price FROM products")

	if err != nil {
		return nil, err
	}

	var products = make([]Product, 0)
	for rows.Next() {
		var product Product

		err := rows.Scan(&product.ProductID, &product.ProductName, &product.RetailPrice)
		if err != nil {
			log.Println("Can't scan row into product struct", err)
		}

		products = append(products, product)
	}

	return products, nil
}

func getProductsFromRedis() (string, error) {
	return redisClient.Get(context.Background(), "Products").Result()
}

func parseProductsToString(products []Product) (string, error) {
	productsJson, err := json.Marshal(products)

	if err != nil {
		return "", err
	}

	return string(productsJson), nil
}

func parseStringToProducts(productsString string) ([]Product, error) {
	var products []Product
	err := json.Unmarshal([]byte(productsString), &products)

	if err != nil {
		return nil, err
	}

	return products, nil
}

func setProductsToRedis(products []Product) error {
	productsString, err := parseProductsToString(products)

	if err != nil {
		return err
	}

	err = redisClient.Set(context.Background(), "Products", productsString, 10*time.Second).Err()
	return err
}

func getProductsHandler(ctx *gin.Context) {
	result, err := getProductsFromRedis()

	if err != nil {
		products, err := getProductsFromDB()

		if err != nil {
			ctx.JSON(http.StatusInternalServerError, err)
			return
		}

		err = setProductsToRedis(products)
		if err != nil {
			ctx.JSON(http.StatusInternalServerError, err)
			return
		}

		resp := Response{
			Products: products,
			Source:   "Database",
		}

		ctx.JSON(http.StatusOK, resp)
	} else {
		products, err := parseStringToProducts(result)

		if err != nil {
			ctx.JSON(http.StatusInternalServerError, err)
			return
		}

		resp := Response{
			Products: products,
			Source:   "Redis",
		}

		ctx.JSON(http.StatusOK, resp)
	}
}
