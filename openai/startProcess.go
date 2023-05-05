package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	cache "sentiment/redis"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
)

func main() {
	lambda.Start(handleRequest)
}

type Body struct {
	MovieID string `json:"movie_id"`
}

func handleRequest(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
	response := events.APIGatewayProxyResponse{
		Headers: map[string]string{
			"Access-Control-Allow-Origin":      "*",
			"Access-Control-Allow-Credentials": "true",
		},
		Body:       `{"error": "No se pudo procesar el analisis sentimental de la película"}`,
		StatusCode: http.StatusInternalServerError,
	}

	body := new(Body)
	err := json.Unmarshal([]byte(request.Body), body)
	if err != nil {
		fmt.Println(err.Error())
		return response, err
	}

	// CONSEGUIR REDIS
	ctx := context.Background()
	clientRedis := redis.NewClient(&redis.Options{
		Addr: "ec2-54-173-57-88.compute-1.amazonaws.com:6379",
	})
	// Intenta conectarse a Redis y realiza un Ping
	_, err = clientRedis.Ping(ctx).Result()
	if err != nil {
		fmt.Println("Error de conexión:", err)
		return response, nil
	}

	reviews := cache.GetREVIEWS(ctx, clientRedis, body.MovieID)

	//______________________________________________________________________
	// START concurrency
	//______________________________________________________________________

	var wg sync.WaitGroup

	for i, review := range reviews {
		wg.Add(1)
		go func(review string, i int) {

			fmt.Println("REVIEW ", i)

			// ANALYZE WITH OPENAI
			analysis, err := AnalyzeText(review)
			if err != nil {
				fmt.Println("ERROR ANALYZE TEXT")
				fmt.Println(err)
				//return response, nil
			}

			jsonData, err := json.Marshal(analysis)
			if err != nil {
				fmt.Println("Error al convertir a JSON Analysis:", err)
				//return response, nil
			}

			// SEND MESSAGE TO KAFKA
			writer := getKafkaWriter()
			defer writer.Close()
			defer wg.Done()
			fmt.Println("start to send message")
			err = writer.WriteMessages(context.Background(),
				kafka.Message{
					Key:   nil,
					Value: []byte(string(jsonData)),
				},
			)
			if err != nil {
				fmt.Println("Error KAFKA")
				fmt.Println(err.Error())
				//return response, nil
			}
		}(review, i)
	}
	wg.Wait()
	//______________________________________________________________________
	// END concurrency
	//______________________________________________________________________

	response.StatusCode = http.StatusAccepted
	response.Body = `{"info": "the process has started"}`

	return response, nil
}

func getKafkaWriter() *kafka.Writer {
	brokerAddresses := []string{"34.207.66.208:9093"}

	return &kafka.Writer{
		Addr:  kafka.TCP(brokerAddresses...),
		Topic: "reviews",
	}
}

func AnalyzeText(text string) (*Review, error) {
	// Parámetros de solicitud
	url := "https://api.openai.com/v1/engines/text-davinci-002/completions"
	data := map[string]interface{}{
		"prompt":            "Clasifica los sentimientos de este texto colocando solamente si es neutral, positivo o negativo: " + text,
		"temperature":       1,
		"max_tokens":        60,
		"top_p":             1.0,
		"frequency_penalty": 0.0,
		"presence_penalty":  0.0,
	}

	// Convertir data a JSON
	jsonData, err := json.Marshal(data)
	if err != nil {
		fmt.Println("Error al convertir a JSON:", err)
		return nil, err
	}

	// Crear request
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		fmt.Println("Error al crear la solicitud HTTP para OPENAI:", err)
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer <YOUR OPENAI-TOKEN GOES HERE>")

	// Realizar la solicitud HTTP
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error al hacer la solicitud HTTP para OPENAI:", err)
		return nil, err
	}
	defer resp.Body.Close()

	// Leer la respuesta
	var result map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		fmt.Println("Error al leer la respuesta:", err)
		return nil, err
	}

	return &Review{
		Analysis: result,
		Review:   text,
	}, nil

}

type Review struct {
	Analysis map[string]interface{} `json:"analysis"`
	Review   string                 `json:"review"`
}
