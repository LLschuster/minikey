package main

import (
	"fmt"

	"github.com/gin-gonic/gin"
	db "github.com/llschuster/minikey/src/db"
)

//RequestJSON structur for inserting new values
type RequestJSON struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func health(c *gin.Context) {
	c.JSON(200, gin.H{
		"message": "pong",
	})
}

func addKey(c *gin.Context) {
	var data RequestJSON
	err := c.BindJSON(&data)
	if err != nil {
		c.JSON(400, gin.H{
			"message": "Data is bad formatted it should be a json with fields key and value",
		})
	}
	result, err := db.InsertKey(data.Key, data.Value, db.Primitive)
	if !result {
		c.JSON(500, gin.H{
			"message": "Server internal error",
			"error":   fmt.Sprintf("%v", err),
		})
	}
	c.JSON(200, gin.H{
		"message": fmt.Sprintf("Added key %v with value %v", data.Key, data.Value),
	})
}

func getKey(c *gin.Context) {
	key := c.Param("key")
	c.JSON(200, gin.H{
		"message": fmt.Sprintf("got key %s ", key),
	})
}

func deleteKey(c *gin.Context) {
	key := c.Param("key")
	c.JSON(200, gin.H{
		"message": fmt.Sprintf("delete key %s ", key),
	})
}

func main() {

	db.DBinit()

	r := gin.Default()
	apiV1 := r.Group("/v1")
	{
		apiV1.GET("/ping", health)
		apiV1.POST("/insert", addKey)
		apiV1.GET("/db/:key", getKey)
		apiV1.DELETE("/db/:key", deleteKey)
	}
	r.Run()
}
