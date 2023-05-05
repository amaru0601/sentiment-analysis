package cache

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"
)

func GetREVIEWS(ctx context.Context, client *redis.Client, idMovie string) []string {
	context := context.Background()

	val, err := client.Get(context, idMovie).Result()
	if err == redis.Nil {
		fmt.Println("Key no existe en Redis")
		return nil
	} else if err != nil {
		fmt.Println("Error al leer el valor de Redis:", err)
		return nil
	} else {
		//fmt.Println("Valor de Redis para key 'mykey':", val)
		str := strings.Trim(val, "[]")
		slice := strings.Split(str, "\",")
		for _, element := range slice {
			fmt.Println(element)
		}
		fmt.Println(len(slice))
		return slice
	}
}
