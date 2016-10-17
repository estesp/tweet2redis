package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	log "github.com/Sirupsen/logrus"
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"gopkg.in/redis.v4"
)

const (
	TWITTER_CONSUMER_KEY    = "TWITTER_CONSUMER_KEY"
	TWITTER_CONSUMER_SECRET = "TWITTER_CONSUMER_SECRET"
	TWITTER_ACCESS_TOKEN    = "TWITTER_ACCESS_TOKEN"
	TWITTER_ACCESS_SECRET   = "TWITTER_ACCESS_SECRET"
	REDIS_HOST              = "REDIS_HOST"
	REDIS_PORT              = "REDIS_PORT"
)

var (
	defaultHashtag = "OpenStackSummit"
	defaultHost    = "localhost"
	defaultPort    = 6379

	twitterConsumerKey, twitterConsumerSecret, twitterAccessToken, twitterAccessSecret string
	redisHost, redisPort, hashtag                                                      string
)

func init() {
	// read environment variables
	twitterConsumerKey = os.Getenv(TWITTER_CONSUMER_KEY)
	twitterConsumerSecret = os.Getenv(TWITTER_CONSUMER_SECRET)
	twitterAccessToken = os.Getenv(TWITTER_ACCESS_TOKEN)
	twitterAccessSecret = os.Getenv(TWITTER_ACCESS_SECRET)

	redisHost = os.Getenv(REDIS_HOST)
	redisPort = os.Getenv(REDIS_PORT)
}

func main() {
	// info level logging by default
	log.SetLevel(log.InfoLevel)

	host := defaultHost
	port := defaultPort
	if redisHost != "" {
		host = redisHost
	}
	if redisPort != "" {
		rport, err := strconv.Atoi(redisPort)
		if err != nil {
			log.Warnf("Can't convert provided redis port (%s) to int: %v", redisPort, err)
		} else {
			port = rport
		}
	}
	if len(os.Args) > 1 {
		hashtag = os.Args[1]
	} else {
		hashtag = defaultHashtag
	}
	log.Infof("Collector: will be retrieving twitter stream search on %q", hashtag)

	if twitterConsumerKey == "" || twitterConsumerSecret == "" ||
		twitterAccessToken == "" || twitterAccessSecret == "" {
		errorMsg := fmt.Sprintf("Must set all Twitter access keys, secrets, and tokens (%s, %s, %s, and %s)",
			TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET)
		log.Error(errorMsg)
		os.Exit(1)
	}
	config := oauth1.NewConfig(twitterConsumerKey, twitterConsumerSecret)
	token := oauth1.NewToken(twitterAccessToken, twitterAccessSecret)
	// http.Client will automatically authorize Requests
	httpClient := config.Client(oauth1.NoContext, token)

	// Twitter client
	log.Info("Setup Twitter OAUTH/HTTP client..")
	client := twitter.NewClient(httpClient)
	log.Infof("Connecting to redis (host %s:%d)", host, port)
	rdClient, err := connectToRedis(host, port)
	if err != nil {
		log.Fatalf("can't connect to redis on %s:%d; Error: %v", host, port, err)
	}
	log.Info("...connected. Start twitter streaming API.")
	stream, err := readTweets(client, rdClient, hashtag)
	if err != nil {
		log.Errorf("can't start reading Twitter stream for %q: Error: %v", hashtag, err)
	}
	// Wait for SIGINT and SIGTERM (HIT CTRL-C)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)

	stream.Stop()
}

func readTweets(client *twitter.Client, rdClient *redis.Client, hashtag string) (*twitter.Stream, error) {
	demux := twitter.NewSwitchDemux()
	demux.Tweet = func(tweet *twitter.Tweet) {
		b, err := json.Marshal(tweet)
		if err != nil {
			log.Errorf("encoding tweet to JSON failed: %v", err)
			return
		}
		err = rdClient.Set(tweet.IDStr, string(b), 0).Err()
		if err != nil {
			log.Errorf("can't write JSON encoded tweet to redis: %v", err)
			return
		}
		err = rdClient.SAdd("hashtags", hashtag).Err()
		if err != nil {
			log.Warnf("can't add hashtag (%s) to list of hashtags in redis: %v", hashtag, err)
		}
		err = rdClient.LPush("pubq", fmt.Sprintf("%s:%s", hashtag, tweet.IDStr)).Err()
		if err != nil {
			log.Errorf("can't add tweet to publish queue: %v", err)
			return
		}
		err = rdClient.Incr(hashtag).Err()
		if err != nil {
			log.Errorf("couldn't increment hashtag total count for %q: %v", hashtag, err)
		}
		log.Infof("Added tweet %s to publish queue", tweet.IDStr)
	}
	demux.StreamLimit = func(limit *twitter.StreamLimit) {
		log.Warnf("Stream Limit warning: %v", limit)
	}
	demux.StreamDisconnect = func(disconnect *twitter.StreamDisconnect) {
		log.Warnf("Stream disconnect from Twitter: %v", disconnect)
	}
	demux.Warning = func(stall *twitter.StallWarning) {
		log.Warnf("Stall warning: %v", stall)
	}
	params := &twitter.StreamFilterParams{
		Track:         []string{hashtag},
		StallWarnings: twitter.Bool(true),
	}
	stream, err := client.Streams.Filter(params)
	if err != nil {
		return nil, err
	}
	go func() {
		for message := range stream.Messages {
			demux.Handle(message)
		}
	}()
	return stream, nil
}

func connectToRedis(host string, port int) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Password: "", // no password
		DB:       0,  // use default DB
	})

	_, err := client.Ping().Result()
	return client, err
}
